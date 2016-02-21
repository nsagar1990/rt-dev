import re
import MySQLdb
import datetime
import traceback
import json
from pprint import pprint
from datetime import *
from datetime import timedelta
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request

keys1_to_look_for = [ 'teaser', 'trailer', 'First Look', 'promo'] #mostly for trailers
keys2_to_look_for = [ 'Video Song', "SONG TRAILER"]
db_name = "RT"
languages = ["english", "tamil", "hindi"]

class YoutubeBrowse(BaseSpider):
    name                = "youtube_browse"
    allowed_domains, start_urls     = [], []
    SKIP_LIST_SHOWS         = ['Satyamev Jayate']
    SKIP_LIST_CHANNEL_IDS   = ['UC-LPIU24bQXVljUXivKEeRQ']
    NEXT_PAGE_URL       = 'https://www.googleapis.com/youtube/v3/search?pageToken=%s&key=%s&channelId=%s&part=snippet,id&order=date&maxResults=50'

    def __init__(self, *args, **kwargs):
        super(YoutubeBrowse, self).__init__(*args, **kwargs)
        self.crawl_type = kwargs.get('crawl_type', 'keepup')
        self.pub_date   = kwargs.get('pub_date', 7)
        self.create_cursor()

    def create_cursor(self):
        self.conn   = MySQLdb.connect(db=db_name, host="localhost", user="root", passwd="hdrn59!")
        self.cursor = self.conn.cursor()

    def start_requests(self):
        auth_key = 'AIzaSyDRcG35_ORO3-DGJGLZ238ZKzajsK_xZ28'
        top_url  = 'https://www.googleapis.com/youtube/v3/search?pageToken=&key=%s&channelId=%s&part=snippet,id&order=date&maxResults=50'

        if self.crawl_type == 'catchup':
            self.pub_date = 120

        self.published_date = datetime.now() - timedelta(days=self.pub_date)

        channels = ['UCs56ffejFhWlCvlvRIdpb9Q',    #viacom
		            'UCgajhM4W-_0YnUk680B10Sg',    #Baahubhali
		            'UCF1JIbMUs6uqoZEY1Haw0GQ',    #Shemaroo
                    'UCqZR7KVVsZRSfezZKJllxVQ',    #Bigpictures
                    'UCgo7B-9h9MsXXV8B8oQKzgw',    #Saregama
                    'UCq-Fj5jknLsUf-MWSy4_brA',    #Tseries
                    'UCGqvJPRcv7aVFun-eTsatcA',    #FoxStarHindhi
                    'UC_IRYSp4auq7hKLvziWVH6w',    #Pixar
                    'UCvC4D8onUfXzvjTOM-dBfEA',    #Marvel
                    'UCq0OueAsdxH6b8nyAspwViw',    #Universal
                    'UCl6Sc_rNW7INU9kovV5SuAA',    #Dreamworks
                    'UC2-BeLxzUBSs0uSrmzWhJuQ',    #20th Century Fox
                    'UCF9imwPMSGz4Vq1NiTWCC7g',    #Paramount Pictures
                    'UCVUOdukwCsi68lkv0XztC9Q',    #Nicarts
                    'UCz97F7dMxBNOfGYu3rx8aCw',    #Sony Pictures
                    'UCjmJDM5pRKbUlVIzDYYWb6g',    #Warner Bros
                    'UCopY0NAzASqmD6eVaRj2TFQ',    #White hill productions
                    'UC8mbSexPity9eEIHr547pUQ',    #14 Reels
                    'UCiJfiEg1FImWsVuEu0L8X6Q',    #Geetha arts
                    'UCU0PnZqMDQ0uvwacGzCX3NA',    #Suresh productions
                    'UC8UDQn1V3-dxaRDu_XtRC4g',    #Puri jagannadh
                    'UCMNvQderPzsd_-sYKQyxHCA',    #Sri venkateswara creations
                    'UCSbEeTlKPcfwAlqwJ08rZaw',    #NTR Arts
                    'UC56gTxNs4f9xZ7Pa2i5xNzg',    #Sony Music India
                    'UC6DRQW2vxV-IfdsVxeAwm_g',    #VFF
                    'UC23xZJ5WrAHpIw2aLV8yUug',    #thirupathi brothers
                    'UCyvOJDBxhi1yqW97hXw3BDw',    #UTV Motion Pictures
                    'UCIx3RWYwikMlDiJeCEUbfEA',    #Wunderbar Studios
                    'UCU8v1Rn64Q1n7Cg2jelHpog',    #Studio Green
                    'UCjmJDM5pRKbUlVIzDYYWb6g',    #WarnerBrosPictures
                    'UCor9rW6PgxSQ9vUPWQdnaYQ',    #Fox Searchlight Pictures
                    'UCf5CjDJvsFvtVIhkfmKAwAA',    #MGM
                    'UCzee67JnEcuvjErRyWP3GpQ',    #Saregama Tamil
                    'UCcXqIv2HjTo_c2IPYmUqiQg',    #Madhura Audio
                    'UCbTLwN10NoCU4WDzLf1JMOA',    #YRF
                    'UCKZSn5C-RzrLjuWJF8wWiDw',    #Mythri MOvies
                    'UCq-Fj5jknLsUf-MWSy4_brA',    #tseries
                    'UCjcqzy7MSaN2KPnzOKIcpEQ',    #Dulquer Salmaan
                    'UCEKWXRsfUHkan-D_ljU8Asw',    #Rajsri
                    'UCSHLoG-bXj1aVA2T5y8t84A',    #Balajimotion
                    'UCLbdVvreihwZRL6kwuEUYsA',    #T series
                    'UClqoU3DHuKFsYCCLXUNUE1g',    #kalaippuli S Thanuii
                ]

        for channel in channels:
            channel   =  channel.replace('\n', '')
            url       =  top_url % (auth_key, channel)
            yield Request(url, callback = self.parse, meta = {'channel' : channel, 'auth_key': auth_key})

    def parse(self, response):
        data               = eval(response.body)
        nextPageToken      = data.get('nextPageToken')
        prevPageToken      = data.get('prevPageToken')
        videos_list        = data.get('items', [])
        channel            = response.meta['channel']
        auth_key           = response.meta['auth_key']
        published_dates    = []

        for video in videos_list:
            g_item    =  {}
            video_id = video['id'].get('videoId')
            if not video_id:
                continue

            org_title   = video['snippet']['title']
            title       = self.clean_title(org_title)
            ch_title    = video['snippet']['channelTitle']
            desc        = video['snippet']['description']

            video_type = ''
            for ti in keys2_to_look_for:
                if ti.lower() in title.lower():
                    video_type = "song_promo"

            for ti in keys1_to_look_for:
                if ti.lower() in title.lower():
                    video_type = "trailer"

            if not video_type: 
                continue 

            image  = self.get_image(video)
            publish_date = self.process_date(video)
            ref_url = 'https://www.youtube.com/watch?v=%s'%video_id

            data_dict = {'title' : title, 'org_title' : org_title,
                        'channel_title' : channel, 'description' : desc,
                        'image' : image, 'pub_date' : publish_date,
                        'reference_url' : ref_url, 'video_id' : video_id,
                        'prg_type' : video_type}

            self.load_data(data_dict)

    def get_image(self, video):
        image      = video['snippet']['thumbnails'].get('high', {}).get('url', '')
        if not image:
            image        = video['snippet']['thumbnails']['default']['url']
        return image

    def process_date(self, video):
        published        = video['snippet']['publishedAt']
        pub_date         = published.split('T')[0]
        publish_date = datetime.strptime(pub_date, '%Y-%m-%d').date()
        return publish_date

    def clean_title(self, title):
        cleaned_title = title
        if '|' in title:
            cleaned_title = title.split('|')[0].strip()
        if '-' in title:
            cleaned_title = cleaned_title.split('-')[0].strip()

        if '-' in title:
            cleaned_title = title.split('-')[0].strip()

        return cleaned_title

    def load_data(self, data):
        if data['prg_type'] == "trailer":
            query = "insert into youtube_trailers(video_id, title, org_title, description, published_date, channel_title, image_url, video_url, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
            values = (data['video_id'], data['title'], data['org_title'],
                      data['description'], data['pub_date'], data['channel_title'],
                      data['image'], data['reference_url'])
    
        if data['prg_type'] == "song_promo":
            query = "insert into youtube_songs(video_id, title, org_title, description, published_date, channel_title, image_url, video_url, created_at, modified_at) values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
            values = (data['video_id'], data['title'], data['org_title'],
                      data['description'], data['pub_date'], data['channel_title'],
                      data['image'], data['reference_url'])


        self.cursor.execute(query, values)
        self.conn.commit()
