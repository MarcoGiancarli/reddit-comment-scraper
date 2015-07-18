from RedditScraper import CommentScraper as CS
from selenium import webdriver

def main():
    http_proxy_urls = scrape_hma()
    print http_proxy_urls
    # NOTE: using a sample of around 5000 comments from r/funny, it seems that
    # comments there are on average about 90 characters each. I'll round to 100.
    # NOTE: each thread gets maybe 10000 comments per hour. With 100 proxies
    # the scraper should get a million comments per hour == 100 million chars
    # per hour. Fun stuff!
    # TODO: figure out a good way to detect and swap out bad proxies
    CS.make_scrapers(http_proxy_urls=http_proxy_urls)


def scrape_hma():
    proxies = []

    driver = webdriver.PhantomJS()
    page = 'http://proxylist.hidemyass.com/search-1314945'
    # This should contain ~150-200 proxies. Thank you China.

    try:
        driver.get(page)
        rows = driver.find_elements_by_tag_name('tr')
        row = 1  # ignore title row
        while row < len(rows):
            fields = rows[row].find_elements_by_tag_name('td')
            # get ip, port, and protocol for proxy
            ip = fields[1].text
            port = fields[2].text

            proxy_url = 'http://%s:%s' % (ip, port)
            proxies.append(proxy_url)
            row += 1
    except:
        print 'Could not reach the HideMyAss site. You\'re shit out of luck'
        exit(1)

    return proxies


if __name__ == '__main__':
    main()