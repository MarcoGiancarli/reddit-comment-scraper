__author__ = 'Marco Giancarli -- m.a.giancarli@gmail.com'


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
                 verbose=False,
                 data_dir='data/',
                 posts_dir='data/posts/',
                 comments_dir='data/comments/',
                 http_proxy_urls=None,
                 delay=5,
                 id=None):
        self.verbose = verbose
        self.data_dir = data_dir
        self.posts_dir = posts_dir
        self.comments_dir = comments_dir
        self.session = requests.session()
        self.delay = delay

        if id is None:
            self.id = CommentScraper.make_id().next()

        popular_user_agents = [
            'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; FSL 7.0.6.01001)',
            'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; FSL 7.0.7.01001)',
            'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; FSL 7.0.5.01003)',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0',
            'Mozilla/5.0 (X11; U; Linux x86_64; de; rv:1.9.2.8) Gecko/20100723 Ubuntu/10.04 (lucid) Firefox/3.6.8',
            'Mozilla/5.0 (Windows NT 5.1; rv:13.0) Gecko/20100101 Firefox/13.0.1',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:11.0) Gecko/20100101 Firefox/11.0',
            'Mozilla/5.0 (X11; U; Linux x86_64; de; rv:1.9.2.8) Gecko/20100723 Ubuntu/10.04 (lucid) Firefox/3.6.8',
            'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; .NET CLR 1.0.3705)',
            'Mozilla/5.0 (Windows NT 5.1; rv:13.0) Gecko/20100101 Firefox/13.0.1',
            'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:13.0) Gecko/20100101 Firefox/13.0.1',
            'Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)',
            'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
            'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)',
            'Opera/9.80 (Windows NT 5.1; U; en) Presto/2.10.289 Version/12.01',
            'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)',
            'Mozilla/5.0 (Windows NT 5.1; rv:5.0.1) Gecko/20100101 Firefox/5.0.1',
            'Mozilla/5.0 (Windows NT 6.1; rv:5.0) Gecko/20100101 Firefox/5.02',
            'Mozilla/5.0 (Windows NT 6.0) AppleWebKit/535.1 (KHTML, like Gecko) Chrome/13.0.782.112 Safari/535.1',
            'Mozilla/4.0 (compatible; MSIE 6.0; MSIE 5.5; Windows NT 5.0) Opera 7.02 Bork-edition [en]',
        ]

        # use some random subset of the user agents for any given
        self.user_agents = []
        while len(self.user_agents) == 0:
            self.user_agents = [
                user_agent
                for user_agent
                in popular_user_agents
                if random.random() < 0.25
            ]

        # use user-supplied proxy urls
        if http_proxy_urls is None:
            self.log('Warning: No proxies are being used.')
        self.http_proxy_urls = http_proxy_urls

        # get ip for current network
        self.my_ip = requests.get('http://icanhazip.com').text.strip()
        self.log('Your network IP is ' + self.my_ip)
        self.proxy_ip_test = self.request('http://icanhazip.com').strip()
        self.log('Proxy IP is currently ' + self.proxy_ip_test)


    @staticmethod
    def make_scrapers(http_proxy_urls,
                      subreddits_filename='data/subreddits.csv'):
        # load subreddits and put them in the queue
        subreddits_file = open(subreddits_filename, 'r')
        subreddits = [sub.strip() for sub in subreddits_file.readlines()]
        scraper_queue = Queue.Queue(maxsize=len(subreddits))
        for sub in subreddits:
            scraper_queue.put(sub)

        # start threads for scrapers for each proxy available.
        scraper_threads = [
            ScraperThread(scraper_queue, [proxy], 3)
            for proxy
            in http_proxy_urls
        ]

        # start daemon for each scraper
        for scraper in scraper_threads:
            scraper.setDaemon(True)
            scraper.start()

        scraper_queue.join()


    @staticmethod
    def make_id():
        while True:
            CommentScraper.id_num += 1
            yield 'scraper_' + str(CommentScraper.id_num)


    def get_subreddits(self):
        subreddit_list_url = 'http://www.reddit.com/subreddits'  # seed url
        subreddit_names = []

        for dummy in range(200):  # 200 pages == top 5000 subreddit cap
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
        subreddit_url = 'http://www.reddit.com/r/{subreddit_name}/top'  # seed url
        subreddit_url = subreddit_url.format(subreddit_name=subreddit_name)
        # post_data = []

        for dummy in range(400):  # 400 pages == 10000 post cap
            self.log('Scraping post links from ' + subreddit_url + '...')

            response_text = self.request(subreddit_url)
            root = html.fromstring(response_text)

            comments_urls = root.xpath(
                '//ul[@class="flat-list buttons"]/li[@class="first"]/a/@href'
            )
            new_post_data = [
                (
                    subreddit_name,
                    CommentScraper.get_post_id_from_url(url)
                )  # the url can be reconstructed from these two strings
                for url
                in comments_urls
            ]

            self.log('Found %d new posts.' % len(new_post_data))

            next_links = root.xpath('//a[@rel="nofollow next"]/@href')
            if len(next_links) < 1:
                break

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
        # self.log('Found %d total posts.' % len(post_data))
        # return post_data


    def scrape_post(self, subreddit_name, post_id):
        post_url = 'http://www.reddit.com/r/{subreddit_name}/comments/{post_id}/'
        post_url = post_url.format(
            subreddit_name=subreddit_name,
            post_id=post_id
        )
        
        self.log('Scraping comments from ' + post_url + '...')

        response_text = self.request(post_url)
        root = html.fromstring(response_text)

        comment_elements = root.xpath(
            '//div[@class="entry unvoted"]/form/div[@class="usertext-body m' + \
            'ay-blank-within md-container "]/div'
        )
        author_elements = root.xpath(
            '//div[@class="entry unvoted"]/p[@class="tagline"]/a[contains(@class, "author may-blank")]'
        )
        comment_texts = [
            unicode(comment_element.text_content())
            for comment_element
            in comment_elements
        ]
        author_texts = [
            unicode(author_element.text_content())
            for author_element
            in author_elements
        ]
        comments = [
            (
                subreddit_name,
                post_id,
                author,
                CommentScraper.format_comment_text(comment)
            )
            for author, comment
            in zip(author_texts, comment_texts)
        ]

        self.log('Collected %d comments.' % len(comments))

        return comments


    def log(self, text):
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
            # to stdout at once, making threads play nice with each other.
            sys.stdout.write(text)

        # TODO: add actual logging if a location is specified in constructor


    def get_current_ip(self):
        if self.session:
            return self.session.get('http://icanhazip.com').text
        else:
            return None


    # accepts a list of tuples, an output name, and a file open mode
    def write_list_to_file(self, list_, filename, mode='w'):
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
        if self.http_proxy_urls:
            proxies = {'http': random.choice(self.http_proxy_urls)}
        else:
            proxies = None

        failures = 0
        while True:
            try:
                response_text = self.session.get(
                    url,
                    headers=user_agent,
                    proxies=proxies
                ).text
                break
            except Exception as e:
                self.log(e.message)
                if 'Errno 104' not in e.message:
                    failures += 1
                    if failures > 3:
                        # return dummy data
                        response_text = '<!doctype html><head></head><body></body>'
                        break
                    time.sleep(self.delay)

        return response_text


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
        comment_text = comment_text.replace('\n', '\\n')
        comment_text = comment_text.replace('\r', '\\r')
        comment_text = comment_text.replace('\t', '\\t')
        return comment_text


CommentScraper.id_num = 0


class ScraperThread(threading.Thread):
    def __init__(self, queue, http_proxy_urls, delay=4):
        threading.Thread.__init__(self)
        self.queue = queue
        self.http_proxy_urls = http_proxy_urls
        self.delay = delay
        self.cs = CommentScraper(
            http_proxy_urls=http_proxy_urls,
            delay=self.delay,
            verbose=True,
        )

    def run(self):
        while True:
            subreddit_name = self.queue.get()
            self.cs.scrape_subreddit(subreddit_name)
            self.queue.task_done()
