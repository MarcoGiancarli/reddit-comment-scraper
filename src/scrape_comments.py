from RedditScraper import CommentScraper as CS


def main():
    # cs = CS(verbose=True)
    # cs.get_subreddits()
    # exit()
    # cs.scrape_subreddit('funny', delay=2)
    http_proxy_urls = [
        # 'http://174.46.79.26:80',
        # 'http://107.170.58.132:3128',
        'http://54.246.113.163:9090',
        'http://107.167.21.243:80',
        # 'http://69.38.16.164:8080',
    ]
    CS.make_scrapers(http_proxy_urls=http_proxy_urls)


if __name__ == '__main__':
    main()