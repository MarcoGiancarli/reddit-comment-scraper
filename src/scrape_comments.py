from RedditScraper import CommentScraper as CS


def main():
    # cs = CS(verbose=True)
    # cs.scrape_subreddit('funny', delay=2)
    http_proxy_urls = [
        ''
    ]
    CS.make_scrapers(http_proxy_urls=http_proxy_urls)


if __name__ == '__main__':
    main()