import re
import md5
import json
from scrapy.spider import *
from scrapy.http import *
from scrapy.selector import *
from CHARTS.items import *

wiki_url = "https://en.wikipedia.org%s"
api_url = "https://en.wikipedia.org/w/api.php?action=query&titles=%s&prop=revisions&rvprop=content&format=json"

class WikiCrawler(Spider):
    name = "wiki_crawler"
    start_urls = ['https://en.wikipedia.org/wiki/List_of_Bollywood_films_of_2016',
                  'https://en.wikipedia.org/wiki/List_of_Bollywood_films_of_2015'
                  'https://en.wikipedia.org/wiki/List_of_Bollywood_films_of_2017']


    def parse(self, response):
        hdoc = Selector(response)

        movie_rows = hdoc.xpath('//table[@class="wikitable sortable"]//tr')
        for movie in movie_rows:
            opening     = ''.join(movie.xpath(".//td[1]//span[@style='white-space:nowrap']//text()").extract())
            title       = ''.join(movie.xpath(".//td[2]//text()").extract())
            movie_url   = ''.join(movie.xpath(".//td[2]//a/@href").extract())
            genre       = ''.join(movie.xpath(".//td[3]//text()").extract())
            director    = ''.join(movie.xpath(".//td[4]//a/@href").extract())
            if not director:
                director = ''.join(movie.xpath(".//td[4]//text()").extract())

            casts = movie.xpath(".//td[4]//a/@href").extract()
            for cast in casts:
                url = wiki_url % casts

            if title:
                #url = api_url %title
                url = wiki_url % movie_url
                yield Request(url, callback = self.parse_movie)

    def parse_movie(self, response):
        hdoc = Selector(response)

        infobox = {}
        infobox_nodes = hdoc.xpath('//table[@class="infobox vevent"]//tr')
        title = ''.join(hdoc.xpath('//h1[@id="firstHeading"]//text()').extract())
        infobox['title'] = title
        for info in infobox_nodes:
            th = ''.join(info.xpath('./th//text()').extract())
            tr = ''.join(info.xpath('.//td//text()').extract())

            if 'Directed by' in th:
                infobox['director'] = tr

            if 'Produced by' in th:
                infobox['producer'] = tr

            if 'Written by' in th:
                infobox['writer']   = tr

            if 'Screenplay by' in th:
                infobox['screenplay'] = tr

            if 'Music by' in th:
                infobox['music']    = tr

            if 'Cinematography' in th:
                infobox['Cinematography'] = tr

            if "Language" in th:
                infobox['Language'] = tr

            if "Starring" in th:
                infobox['casts'] = tr.split("\n")

            if "Edited by" in th:
                infobox['editor'] = tr

            if "Country" in th:
                infobox['country'] = tr

            if "Production" in th and "companies" in th:
                infobox['production_company'] = tr

            if 'Distributed by'in th:
                infobox['distributor'] = tr

            if "Release dates" in th:
                infobox['release_date'] = tr

            if "Running time" in th:
                infobox['runtime'] = tr

        movieitem  = MovieItem()
        wiki_gid = "".join(re.findall('"wgArticleId":(\d+),', response.body))
        movieitem['sk']                     = wiki_gid
        movieitem['title']                  = title
        movieitem['description']            = ''
        movieitem['genres']                 = ''
        movieitem['duration']               = ''
        movieitem['languages']              = infobox.get('Language', '')
        movieitem['original_languages']     = ''
        movieitem['aka']                    = ''
        movieitem['production_country']     = 'India'
        movieitem['reference_url']          = response.url
        movieitem['release_year']           = "2016"
        yield movieitem


        for key, values in infobox.iteritems():
            if not isinstance(values, str):
                continue
            values = ",".join(values.split("\n"))

            program_crew                         = ProgramCrewItem()
            program_crew['program_sk']           = wiki_gid
            program_crew['program_type']         = "movie"
            try:
                program_crew['crew_sk']              = md5.md5(values).hexdigest()
            except:
                continue
            program_crew['role']                 = key
            program_crew['description']          = ''
            program_crew['role_title']           = ''
            program_crew['rank']                 = 1
            yield program_crew

            crewitem = CrewItem()
            crewitem['sk']                   = md5.md5(values).hexdigest()
            crewitem['name']                 = values
            crewitem['original_name']        = ''
            crewitem['description']          = ''
            crewitem['aka']                  = ''
            crewitem['gender']               = ''
            crewitem['birth_date']           = ''
            crewitem['death_date']           = ''
            crewitem['country']              = ''
            crewitem['biography']            = ''
            crewitem['height']               = ''
            crewitem['weight']               = ''
            crewitem['image']                = ''
            crewitem['reference_url']        = ''
            yield crewitem
