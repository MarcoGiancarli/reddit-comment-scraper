__author__ = 'Marco Giancarli -- m.a.giancarli@gmail.com'


import logging
import requests
import random
import unicodecsv as csv
import sys
import time
import nltk
import threading
import Queue
from datetime import datetime
from lxml import html


class CommentScraper():
    def __init__(self,
                 verbose=True,
                 log_level=logging.ERROR,
                 data_dir='data/',
                 posts_dir='data/posts/',
                 comments_dir='data/comments/',
                 proxy_queue=None,
                 delay=4,
                 scraper_id=None):
        self.verbose = verbose
        self.log_level = log_level
        self.data_dir = data_dir
        self.posts_dir = posts_dir
        self.comments_dir = comments_dir
        self.session = requests.session()
        self.delay = delay

        logging.basicConfig(
            filename='comment_scraper.log',
            filemode='w',
            level=log_level
        )

        if scraper_id is None:
            self.id = CommentScraper.make_id().next()

        popular_user_agents = []
        with open('res/popular_user_agents.txt', 'r') as user_agent_file:
            for user_agent in user_agent_file.readlines():
                popular_user_agents.append(user_agent.strip())

        # use some random subset of the user agents for any given
        self.user_agents = []
        while len(self.user_agents) == 0:  # loop if, by chance, we pick nothing
            self.user_agents = [
                user_agent
                for user_agent
                in popular_user_agents
                if random.random() < 0.25
            ]

        # use user-supplied proxy urls
        if proxy_queue is None:
            self.log('Warning: No proxies are being used.', level='warning')
            self.proxy = None
        else:
            self.proxy = proxy_queue.get()

        self.proxy_queue = proxy_queue

        # get ip for current network
        # self.my_ip = requests.get('http://icanhazip.com').text.strip()
        # self.log('Your network IP is ' + self.my_ip)
        # self.proxy_ip_test = self.request('http://icanhazip.com').strip()
        # self.log('Proxy IP is currently ' + self.proxy_ip_test)

    @staticmethod
    def make_scrapers(http_proxy_urls,
                      subreddits_filename='data/subreddits.csv',
                      num_threads=100,
                      verbose=True,
                      log_level=logging.ERROR):
        # load subreddits and put them in the queue
        subreddits_file = open(subreddits_filename, 'r')
        subreddits = [sub.strip() for sub in subreddits_file.readlines()]
        
        # set up queue for subreddits
        subreddit_queue = Queue.Queue(maxsize=len(subreddits))
        for sub in subreddits:
            subreddit_queue.put(sub)

        # make thread-safe queue for the proxies
        proxy_queue = Queue.PriorityQueue(len(http_proxy_urls))
        for url in http_proxy_urls:
            proxy_queue.put(Proxy(url))

        # start threads for scrapers for each proxy available.
        scraper_threads = [
            ScraperThread(
                queue=subreddit_queue,
                proxy_queue=proxy_queue,
                delay=3,
                verbose=verbose,
                log_level=log_level
            )
            for dummy
            in range(min(num_threads, proxy_queue.qsize()))
        ]

        # start daemon for each scraper
        for scraper in scraper_threads:
            scraper.setDaemon(True)
            scraper.start()

        subreddit_queue.join()

    @staticmethod
    def make_id():
        while True:
            CommentScraper.id_num += 1
            yield 'scraper_' + str(CommentScraper.id_num)

    def get_subreddits(self):
        subreddit_list_url = 'http://www.reddit.com/subreddits'  # seed url
        subreddit_names = []

        for dummy in range(400):  # 400 pages == top 10,000 subreddit cap
            self.log('Scraping subreddit urls from %s...' % subreddit_list_url)

            response_text = self.request(subreddit_list_url)
            root = html.fromstring(response_text)

            titles = root.xpath('//div/p[@class="titlerow"]/a/@href')
            new_subreddit_names = [
                (title  # put in tuple for easy file writing
                    .replace('http://www.reddit.com/r/', '')  # remove prefix
                    .replace('/', '')  # remove trailing '/'
                ,)
                for title
                in titles
            ]

            # add to total names list
            subreddit_names.extend(new_subreddit_names)

            num_new_subreddits = str(len(new_subreddit_names))
            self.log('Found ' + num_new_subreddits + ' new subreddits.')

            # if we can't find the next button three times in a row, give up.
            next_links = root.xpath('//a[@rel="nofollow next"]/@href')
            if len(next_links) < 1:
                break

            next_link = next_links[0]  # should be only one
            self.log('Following link to next page ' + next_link + '...')
            subreddit_list_url = next_link
            # sleep to avoid reddit getting mad
            time.sleep(self.delay)

        self.log('Next button not found. Finished getting subreddits.')
        self.log('Found %d total subreddits.' % len(subreddit_names))

        subreddits_filename = self.data_dir + 'subreddits.csv'
        self.write_list_to_file(subreddit_names, subreddits_filename)

        return subreddit_names

    def scrape_subreddit(self, subreddit_name):
        subreddit_url = 'http://www.reddit.com/r/{subreddit_name}'  # seed url
        subreddit_url = subreddit_url.format(subreddit_name=subreddit_name)
        got_first_page = False

        for dummy in range(4000):  # 4000 pages == 100,000 post cap
            self.log('Scraping post links from ' + subreddit_url + '...')

            response_text = self.request(subreddit_url)
            root = html.fromstring(response_text)

            comments_urls = root.xpath(
                '//div[@id="siteTable"]//ul[@class="flat-list buttons"]/li[@cla'
                'ss="first"]/a/@href'
            )
            # there shouldn't be more than 25 of these.
            # if there are, some are likely in the side bar, which comes first.
            if len(comments_urls) > 25:
                comments_urls = comments_urls[-25:]

            new_post_data = [
                (
                    subreddit_name,
                    CommentScraper.get_post_id_from_url(url)
                )  # the url can be reconstructed from these two strings
                for url
                in comments_urls
            ]

            next_links = root.xpath('//a[@rel="nofollow next"]/@href')

            if not got_first_page:
                got_first_page = len(new_post_data) > 0

            if len(next_links) < 1:
                break

            self.log('Found %d new posts.' % len(new_post_data))

            next_link = next_links[0]  # should be only one
            self.log('Following link to next page ' + next_link + '...')
            subreddit_url = next_link
            # sleep between requests to avoid pissing off reddit
            time.sleep(self.delay)

            # write the comments to file
            comments_filename = self.comments_dir + subreddit_name + '.csv'
            for sub_name, post_id in new_post_data:
                comments = self.scrape_post(sub_name, post_id)
                self.write_list_to_file(comments, comments_filename, mode='a')
            # write the post data to file
            posts_filename = self.posts_dir + subreddit_name + '.csv'
            self.write_list_to_file(new_post_data, posts_filename, 'a')

        self.log('Next button not found. Finished getting posts.')

    def scrape_post(self, subreddit_name, post_id):
        post_url = 'http://www.reddit.com/r/{sub_name}/comments/{post_id}/'
        post_url = post_url.format(
            sub_name=subreddit_name,
            post_id=post_id
        )
        
        self.log('Scraping comments from ' + post_url + '...')

        response_text = self.request(post_url)
        root = html.fromstring(response_text)

        comment_elements = root.xpath(
            '//div[@class="entry unvoted"]/form/div[@class="usertext-body may-b'
            'lank-within md-container "]/div'
        )
        author_elements = root.xpath(
            '//div[@class="entry unvoted"]/p[@class="tagline"]/a[contains(@clas'
            's, "author may-blank")]'
        )
        score_elements = root.xpath(
            '//div[@class="entry unvoted"]/p[@class="tagline"]/span[@class="sco'
            're unvoted"]'
        )
        time_elements = root.xpath(
            '//div[@class="entry unvoted"]/p[@class="tagline"]/time[@class="liv'
            'e-timestamp"]/@title'
        )

        comment_texts = [
            CommentScraper.format_comment_text(unicode(element.text_content()))
            for element
            in comment_elements
        ]
        author_texts = [
            unicode(element.text_content())
            for element
            in author_elements
        ]
        score_texts = [
            unicode(element.text_content().replace(u' point', u'').replace(u's', u''))
            for element
            in score_elements
        ]
        time_texts = [
            unicode(element)
            for element
            in time_elements
        ]

        comments = [
            (subreddit_name, post_id, author, score, post_time, comment)
            for author, score, post_time, comment
            in zip(author_texts, score_texts, time_texts, comment_texts)
        ]

        self.log('Collected %d comments.' % len(comments))

        return comments

    def log(self, text, level='info'):
        current_time = datetime.utcnow()
        sec = current_time.second
        min_ = current_time.minute
        hour = current_time.hour
        mday = current_time.day
        mon = current_time.month
        year = current_time.year

        # add leading '0' when necessary
        sec = '0' + str(sec) if sec < 10 else sec
        min_ = '0' + str(min_) if min_ < 10 else min_
        hour = '0' + str(hour) if hour < 10 else hour

        current_time = '[{year}/{mon}/{mday} {hour}:{min}:{sec}]'.format(
            sec=sec, min=min_, hour=hour, mday=mday, mon=mon, year=year
        )

        text = '%s %s: %s\n' % (current_time, self.id, str(text))
        if self.verbose:
            # Use this (NOT print) because print separates the text and \n when
            # it prints in a multithreaded environment. This prints everything
            # to stdout at once, making threads play nicely with each other.
            sys.stdout.write(text)

        if level == 'info':
            logging.info(text)
        if level == 'error':
            logging.error(text)
        if level == 'warning':
            logging.warning(text)
        if level == 'debug':
            logging.debug(text)
        if level == 'critical':
            logging.critical(text)

    def get_current_ip(self):
        if self.session:
            return self.session.get('http://icanhazip.com').text
        else:
            return None

    # accepts a list of tuples, an output name, and a file open mode
    @staticmethod
    def write_list_to_file(list_, filename, mode='w'):
        list_ = list(list_)  # just in case
        # store the subreddits in a text file in data/
        with open(filename, mode) as output_file:
            writer = csv.writer(
                output_file,
                delimiter=',',
                quotechar='"',
                encoding='utf-8'
            )
            for vals in list_:
                vals = tuple(unicode(val) for val in vals)
                writer.writerow(vals)

    def request(self, url):
        user_agent = {'User-agent': random.choice(self.user_agents)}
        if self.proxy:
            proxies = {'http': self.proxy.url}
        else:
            proxies = None

        # this is in all reddit pages, so if we don't see this, we failed
        verify_text = 'reddit, reddit.com'

        failures = 0
        response_text = None
        while not response_text:
            try:
                response_text = self.session.get(
                    url,
                    headers=user_agent,
                    proxies=proxies,
                    timeout=20,
                ).text
                if verify_text in response_text:
                    self.proxy.on_success()  # increase confidence
                    break
                else:
                    raise Exception(message='failed to validate')
            except Exception as e:
                self.log(e.message, level='error')
                self.proxy.on_failure(e.message)  # decrease confidence
                failures += 1
                if failures > 4:
                    self.swap_proxies()
                time.sleep(self.delay)

        return response_text

    def swap_proxies(self, replace=True):
        self.log(
            'Swapping proxies... (Proxy queue size: %d)' %
            self.proxy_queue.qsize()
        )
        old_proxy = self.proxy
        # queue.get() waits until a proxy is available in the queue
        self.proxy = self.proxy_queue.get()
        if replace:
            try:
                self.proxy_queue.put(old_proxy, timeout=10)
            except Exception as e:
                self.log(
                    'Could not insert proxy into proxy queue: ' + e.message,
                    'error'
                )

    @staticmethod
    def html_to_text(html_string):
        # TODO: make this more robust
        retval = nltk.clean_html(html_string.text_content())
        return retval

    @staticmethod
    def get_post_id_from_url(url):
        # trying to be as robust as possible here
        comments_part = '/comments/'
        start = url.find(comments_part) + len(comments_part)
        url_end = url[start:]
        post_id = url_end.split('/')[0]
        return post_id

    @staticmethod
    def format_comment_text(comment_text):
        comment_text = comment_text.strip()
        # We're only really interested in the words, so make whitespace simple.
        comment_text = comment_text.replace(u'\n', u' ')
        comment_text = comment_text.replace(u'\r', u' ')
        comment_text = comment_text.replace(u'\t', u' ')
        return comment_text


# initialize the id number for the static method make_id()
CommentScraper.id_num = 0


class ScraperThread(threading.Thread):
    def __init__(self, queue, proxy_queue, delay=4,
                 verbose=True, log_level=logging.ERROR):
        threading.Thread.__init__(self)
        self.queue = queue
        self.proxy_queue = proxy_queue
        self.delay = delay
        self.cs = CommentScraper(
            proxy_queue=proxy_queue,
            delay=self.delay,
            verbose=verbose,
            log_level=log_level,
        )

    def run(self):
        while True:
            qsize = self.queue.qsize()
            self.cs.log('Estimated subreddits remaining: %d' % qsize)
            subreddit_name = self.queue.get()
            self.cs.scrape_subreddit(subreddit_name)
            self.proxy_queue.put(self.cs.proxy)
            self.queue.task_done()


class Proxy:
    def __init__(self, url):
        self.url = url
        self.failure = 0
        # make this 1 to avoid divide by zero errors and prioritize new proxies
        self.success = 1

    failure_message_substrings = [
        'timed out', 'Connection refused', 'reset by peer', 'Max retries',
        'failed to validate'
    ]

    ''' Compare proxies by "uncertainty". Lower is better. '''
    def __lt__(self, other):
        return self.failure / self.success < other.failure / other.success

    def __gt__(self, other):
        return self.failure / self.success > other.failure / other.success

    def __le__(self, other):
        return self.failure / self.success <= other.failure / other.success

    def __ge__(self, other):
        return self.failure / self.success >= other.failure / other.success

    def __eq__(self, other):
        return self.failure / self.success == other.failure / other.success

    def on_success(self):
        self.success += 1

    def on_failure(self, message=None):
        if message:
            if any(sub in message for sub in Proxy.failure_message_substrings):
                self.failure += 1
