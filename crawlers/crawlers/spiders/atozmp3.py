import re
import MySQLdb
from pprint import pprint
from scrapy.spider import *
from scrapy.selector import *
from scrapy.http import *

SONGS_SINGERS = re.compile("\d+-.*-.*")
RELEASE_YEAR = re.compile('(.*) \((\d{4})\)')

class Atozmp3(Spider):
    name = "atozmp3"
    start_urls = ["http://www.atozmp3.in/tag/A"]

    def create_cursor(self):
        self.cursor = MySQLdb.connect(host="localhost", user="root", passwd="hdrn59!", db="ATOZMP3").cursor()


    def parse(self, response):
        self.create_cursor()
        hdoc = Selector(response)

        alpha_lists = hdoc.xpath('//div[@class="clear alphabet-list"]//a//@href').extract()
        for alpha in alpha_lists:
            yield Request(alpha, callback = self.parse_movies_list)

    def parse_movies_list(self, response):
        hdoc = Selector(response)

        movies_list = hdoc.xpath('//div[@class="list-content"]//h2[@class="entry-title"]//a/@href').extract()
        for movie in movies_list:
            yield Request(movie, callback=self.parse_movies)

        pagination_nodes = hdoc.xpath('//div[@class="pagenavi clear"]//a/@href').extract()
        for page in pagination_nodes:
            yield Request(page, callback=self.parse_movies_list)

    def parse_movies(self, response):
        hdoc = Selector(response)

        title = ''.join(hdoc.xpath('//h1[@class="entry-title"]//text()').extract())
        title = title.replace("Songs Download", "").replace("Telugu Movie Mp3", "")
        meta_content = hdoc.xpath('//div[@class="entry entry-content"]//p//span')

        song_dict = []
        cast_info, music_director, director, producer, music_lable, lyrics = [ '' ] * 6
        for meta in meta_content:
            meta_text = ''.join(meta.xpath(".//text()").extract())
            if "Cast & Crew" in meta_text:
                cast_info = meta_text.split("::")[-1].strip()

            elif "Music ::" in meta_text:
                music_director = meta_text.split("::")[-1].strip()

            elif "Lyrics ::" in meta_text:
                lyrics = meta_text.split("::")[-1].strip()

            elif "Director ::" in meta_text:
                director = meta_text.split("::")[-1].strip()

            elif "Producer ::" in meta_text:
                producer = meta_text.split("::")[-1].strip()

            elif "Cassettes & CD" in meta_text:
                music_lable = meta_text.split("::")[-1].strip()
            else:
                x = meta_text.encode('utf8')
                if  "\xe2\x80\x93" in x:
                    temp = {}
                    x = x.split("\xe2\x80\x93")
                    if len(x) == 3:
                        no, song_name, singers = x
                        temp = {'song' : song_name.strip(), 'singers' : singers.strip()}
                    elif len(x) == 2:
                        no, song_name = x
                    song_dict.append(temp)


        sk  = response.url.split("/")[-1].replace(".html", "")
        if RELEASE_YEAR.findall(title):
            title, release_year = RELEASE_YEAR.findall(title)[0]
        else:
            return

        query = 'insert into Movie(sk, title, languages, release_year, original_languages, production_country, reference_url, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()'
        values = (sk, title, "Telugu", release_year, "", "India", response.url)
        self.cursor.execute(query, values)
