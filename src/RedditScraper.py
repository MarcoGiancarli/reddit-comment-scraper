__author__ = 'Marco Giancarli -- m.a.giancarli@gmail.com'


import requests
import random
import csv
import time
import nltk
from datetime import datetime
from lxml import html


class CommentScraper():
    def __init__(self,
                 verbose=False,
                 data_dir='data/',
                 posts_dir='data/posts/',
                 comments_dir='data/comments/',
                 http_proxy_urls=None):
        self.verbose = verbose
        self.data_dir = data_dir
        self.posts_dir = posts_dir
        self.comments_dir = comments_dir
        self.session = None

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
        self.user_agents = [
            user_agent
            for user_agent
            in popular_user_agents
            if random.random() < 0.25
        ]

        # user-supplied proxy urls
        if http_proxy_urls:
            self.log('Warning: No proxies are being used.')
        self.http_proxy_urls = http_proxy_urls

        # get ip for current network
        self.my_ip = requests.get('http://icanhazip.com').text
        self.log('Your network IP is ' + self.my_ip)


    @staticmethod
    def make_scrapers(http_proxy_urls):
        pass
        # TODO: make a bunch of scraper instances and run them concurrently
        # TODO: make one for each proxy(s)


    def get_subreddits(self, delay=3):
        subreddit_list_url = 'http://www.reddit.com/subreddits'  # seed url
        subreddit_names = []
        consecutive_fails = 0

        for dummy in range(200):  # 200 pages == top 5000 subreddit cap
            self.log(
                'Scraping subreddit urls from ' +
                subreddit_list_url +
                '...'
            )

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
                if consecutive_fails < 2:
                    consecutive_fails += 1
                    self.log('Trying again...')
                    time.sleep(1)
                    continue
                else:
                    break
            else:
                consecutive_fails = 0

            next_link = next_links[0]  # should be only one
            self.log('Following link to next page ' + next_link + '...')
            subreddit_list_url = next_link
            # sleep to avoid reddit getting mad
            time.sleep(delay)

        num_total_subreddits = str(len(subreddit_names))
        self.log('Next button not found. Finished getting subreddits.')
        self.log('Found ' + num_total_subreddits + ' total subreddits.')

        subreddits_filename = self.data_dir + 'subreddits.csv'
        self.write_list_to_file(subreddit_names, subreddits_filename)


    def scrape_subreddit(self, subreddit_name, delay=0.2):
        subreddit_url = 'http://www.reddit.com/r/{subreddit_name}/top'  # seed url
        subreddit_url = subreddit_url.format(subreddit_name=subreddit_name)
        post_data = []
        consecutive_fails = 0

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

            # add to total names list
            post_data.extend(new_post_data)

            num_new_posts = str(len(new_post_data))
            self.log('Found ' + num_new_posts + ' new posts.')

            next_links = root.xpath('//a[@rel="nofollow next"]/@href')
            if len(next_links) < 1:
                if consecutive_fails < 2:
                    consecutive_fails += 1
                    self.log('Trying again...')
                    time.sleep(1)
                    continue
                else:
                    break
            else:
                consecutive_fails = 0

            next_link = next_links[0]  # should be only one
            self.log('Following link to next page ' + next_link + '...')
            subreddit_url = next_link
            # sleep between requests to avoid pissing off reddit
            time.sleep(delay)
            for sub_name, post_id in new_post_data:
                self.scrape_post(sub_name, post_id)

        num_total_posts = str(len(post_data))
        self.log('Next button not found. Finished getting posts.')
        self.log('Found ' + num_total_posts + ' total posts.')

        posts_filename = self.posts_dir + subreddit_name + '.csv'
        self.write_list_to_file(post_data, posts_filename)


    def scrape_post(self, subreddit_name, post_id):
        post_url = 'http://www.reddit.com/r/{subreddit_name}/comments/{post_id}/'
        post_url = post_url.format(
            subreddit_name=subreddit_name,
            post_id=post_id
        )
        
        self.log('Scraping comments from ' + post_url + '...')

        response_text = self.request(post_url)
        root = html.fromstring(response_text)

        comment_texts = root.xpath(
            '//div[@class="usertext-body may-blank-within md-container "]/div'
        )
        comments = [
            (
                subreddit_name,
                post_id,
                CommentScraper.html_to_text(comment)
            )
            for comment
            in comment_texts
        ]

        # TODO: somehow store data -- does it need to be thread-safe?


    def log(self, text):
        current_time = datetime.utcnow()
        sec = current_time.second
        min = current_time.minute
        hour = current_time.hour
        mday = current_time.day
        mon = current_time.month

        current_time = '[{mon}/{mday} {hour}:{min}:{sec}] '.format(
            sec=sec, min=min, hour=hour, mday=mday, mon=mon
        )

        text = current_time + str(text)
        if self.verbose:
            print text

        # TODO: add actual logging if a location is specified in constructor


    def get_current_ip(self):
        if self.session:
            return self.session.get('http://icanhazip.com').text
        else:
            return None


    def write_list_to_file(self, list_, filename):
        # store the subreddits in a text file in data/
        with open(filename, 'w') as output_file:
            writer = csv.writer(output_file, delimiter=',', quotechar='"')
            for sub in list_:
                writer.writerow(sub)


    def request(self, url):
        user_agent = {'User-agent': random.choice(self.user_agents)}
        if self.http_proxy_urls:
            proxies = {'http': random.choice(self.http_proxy_urls)}
        else:
            proxies = None

        try:
            response_text = requests.get(
                url,
                headers=user_agent,
                proxies=proxies
            ).text
        except Exception as e:
            self.log(e.message)
            response_text = ''

        return response_text


    @staticmethod
    def html_to_text(html_string):
        # TODO: make this more robust
        retval = nltk.clean_html(html_string.text_content())
        retval = retval.replace('\\n', ' ')
        return retval


    @staticmethod
    def get_post_id_from_url(url):
        # trying to be as robust as possible here
        comments_part = '/comments/'
        start = url.find(comments_part) + len(comments_part)
        url_end = url[start:]
        post_id = url_end.split('/')[0]
        return post_id
