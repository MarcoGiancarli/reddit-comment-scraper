from RedditScraper import CommentScraper as CS


def main():
    http_proxy_urls = [
        'http://107.167.21.243:80',
        'http://96.126.110.193:4444',
        # 'http://64.6.105.183:4444',
        'http://69.38.16.164:8080',
        'http://174.46.79.26:80',
        # 'http://52.26.143.237:80',

    ]
    # NOTE: using a sample of around 5000 comments from r/funny, it seems that
    # comments there are on average about 90 characters each. I'll round to 100.
    # NOTE: each thread gets maybe 10000 comments per hour. With 100 proxies
    # the scraper should get a million comments per hour == 100 million chars
    # per hour. Fun stuff!
    # TODO: figure out a good way to detect and swap out bad proxies
    CS.make_scrapers(http_proxy_urls=http_proxy_urls)


if __name__ == '__main__':
    main()