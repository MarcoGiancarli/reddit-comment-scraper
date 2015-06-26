from RedditScraper import CommentScraper as CS


def main():
    cs = CS(verbose=True)
    cs.scrape_subreddit('funny', delay=2)


if __name__ == '__main__':
    main()