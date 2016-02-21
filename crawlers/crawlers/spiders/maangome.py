import md5
import re
import MySQLdb
from pprint import pprint
from scrapy.spider import *
from scrapy.selector import *
from scrapy.http import *

lang_pat = re.compile("www.maango.me/(\w+)")
title    = re.compile("(.*) \(\d{4}\)")

class MaangoMe(Spider):
    name = "mangoome"
    start_urls = ["http://www.maango.me/hindi/", "http://www.maango.me/tamil/"]

    def __init__(self, *args, **kwargs):
        super(MaangoMe, self).__init__(*args, **kwargs)
        self.cursor = MySQLdb.connect(db="MAANGOME", user="root",
                                      passwd="hdrn59!", host="localhost").cursor()

    def parse(self, response):
        hdoc = Selector(response)
        movies = hdoc.xpath('//div[@id="dle-content"]//div[@class="short-film"]')

        lang = lang_pat.findall(response.url)
        for movie in movies:
            link = ''.join(movie.xpath('.//h5//a/@href').extract())
            yield Request(link, callback = self.parse_movies, meta ={'langauge' : lang[0]})

        next_page = "".join(hdoc.xpath('//div[@class="pages"]//div[@class="navi"][2]//a/@href').extract())
        yield Request(next_page, callback = self.parse)

    def parse_movies(self, response):
        hdoc = Selector(response)

        movie_dict, singers_dict_, cast_dict_  = {}, list(), list()
        meta_details = hdoc.xpath('//ul[@class="ul-ffilm"]//li')
        for meta in meta_details:
            heading = ''.join(meta.xpath('.//span[@class="type"]//text()').extract())
            text = ''.join(meta.xpath('.//p//text()').extract())

            if "Movie Title" in heading:
                text = title.findall(text)[0]
                movie_dict['title']       = text
            if "Language" in heading:
                movie_dict['language']    = text
            if "Year" in heading:
                movie_dict['rel_year']    = text

            if "Director" in heading:
                director        = "".join(meta.xpath('.//p//text()').extract())
                movie_dict['director_sk'] = md5.md5(director).hexdigest()
                movie_dict['director_name'] = director
            if "Music" in heading:
                music_director  = text
    
            if "Singers" in heading:
                singers = meta.xpath('.//a')
                for singer in singers:
                    singers_dict = dict()
                    singer_sk = "".join(singer.xpath('.//@href').extract())
                    if singer_sk == "#": continue
                    singers_dict['singer_sk']   = "".join(singer.xpath('.//@href').extract()).split("/")[-2]
                    singers_dict['singer_name'] = "".join(singer.xpath('.//text()').extract())
                    singers_dict_.append(singers_dict)

            if "Starring" in heading: 
                casts = meta.xpath('.//a')
                for cast in casts:
                    cast_dict  = dict()
                    cast_sk = "".join(cast.xpath('.//@href').extract())
                    if cast_sk == "#":continue
                    cast_dict['cast_sk']     = "".join(cast.xpath('.//@href').extract()).split("/")[-2]
                    cast_dict['cast_name']   = "".join(cast.xpath('.//text()').extract())
                    cast_dict_.append(cast_dict)


        movie_dict['rating']      = "".join(hdoc.xpath('//div[@class="rating"]//li[@class="current-rating"]//text()').extract())
        movie_dict['image']       = "http://www.maango.me"+"".join(hdoc.xpath('//div[@class="img-poster"]//img/@src').extract())
        movie_dict['views']       = "".join(hdoc.xpath('//div[@class="bottom-news"]//div[@class="views-news"]//span[@class="figures"]//text()').extract())
        movie_dict['langauge']    = response.meta['langauge']
        movie_dict['description'] = ''
        movie_dict['reference_url'] = response.url

        songs_dict_ = []
        songs      = hdoc.xpath('//div[@class="story"]//div[@id="song_box"]')
        for song in songs:
            songs_dict = dict()
            songs_dict['song_title']   = "".join(song.xpath('.//span[@id="song_title"]//text()').extract())
            songs_dict['song_details'] = "".join(song.xpath('.//span[@id="song_right"]//text()').extract()).split("|")[1].split("Length :")[1].strip()
            songs_dict['song_artists'] = "".join(song.xpath('.//div[@id="song_link"][1]//text()').extract()).split(":")[1]
            songs_dict_.append(songs_dict)

        mov_dict = dict()
        movie_dict['sk']      = md5.md5(response.url).hexdigest()
        movie_dict['genres']  = ''
        mov_dict['meta']      = movie_dict
        mov_dict['cast']      = cast_dict_
        mov_dict['songs']     = songs_dict_
        mov_dict['singers']   = singers_dict_
        self.load_data(mov_dict)


    def load_data(self, mov_dict):

        mov_dict = mov_dict['meta']
        #moviedata loading
        query = 'insert into Movie(sk, title, description, genres, languages, release_year, reference_url, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now();'
        values = (mov_dict['sk'], mov_dict['title'], mov_dict['description'],
                  mov_dict['genres'], mov_dict['language'], mov_dict['rel_year'],
                  mov_dict['reference_url'])
        self.cursor.execute(query, values)
