from scrapy.spider import *


class Sample(Spider):
    name = "sample"
    start_urls = ["http://etvnet.com/tv/serialyi-online/detektivyi-koltso-mertvetsa/268780/"]

    def parse(self, response):
        print response.url
