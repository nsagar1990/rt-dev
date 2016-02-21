from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import *
'''
import MYSQLdb
conn =MYSQLdb.connect("localhost","itunes","itunes123","Itunes")
cursor=conn.cursor()
cursor.execute(sql)
'''

class Itunes(Spider):
    name="Itunes"
    start_urls=["http://www.apple.com/in/itunes/charts/songs/"]

    def parse(self,response):
        hdoc=Selector(response)
        links= response.xpath('//div[@class="section-content"]//h3/a/@href').extract()

        for  li in links:
            print li
            yield Request(li,callback = self.parse_next)

    def parse_next(self,response):
        title = "".join(response.xpath('//span[@itemprop="name"]/text()').extract())
        genre = "<>".join(response.xpath('//span[@itemprop="genre"]/text()').extract())
        release_date = "".join(response.xpath('//span[@itemprop="dateCreated"]/text()').extract())
        copyright = "".join(response.xpath('//li[@class="copyright"]/text()').extract())
        duration ="".join( response.xpath('//meta[@itemprop="duration"]/following-sibling::span/text()').extract())
        price ="".join( response.xpath('//span[@itemprop="price"]/text()').extract())
        movie ="".join( response.xpath('//div[@class="left"]/h1/text()').extract())
        #sql= insert into  songs(title,genre,release_date,copyright,duration,price,movie)values('title','genre','release_date','copyright',duration,price,'movie');
        print movie
        print price
        print title
        print genre
        print release_date
        print copyright
        print duration
