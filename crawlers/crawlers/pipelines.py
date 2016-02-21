# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import MySQLdb
from items import *

class RtPipeline(object):
    def __init__(self):
        self.config_file_path = os.getcwd()
        self.config_file_name = "crawlers.cfg"
        self.create_cursor()

    def create_cursor(self):
        self.cursor = MySQLdb.connect(db="WIKI", user="root",
                                      passwd="hdrn59!", host="localhost").cursor()

    def process_item(self, item, spider):

        if isinstance(item, MovieItem):
            query  = "insert into Movie(sk, title, description, genres, duration, languages, release_year, original_languages, \
                      aka, production_country, reference_url, modified_at, created_at) values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) \
                      on duplicate key update modified_at = now()"
            values = (item['sk'], item['title'], item['description'], item['genres'],                 
                      item['duration'], item['languages'], item['release_year'], item['original_languages'], item['aka'],                    
                      item['production_country'], item['reference_url'])     
            self.cursor.execute(query, values)

        if isinstance(item, ProgramCrewItem):
            query = "insert into ProgramCrew(program_sk, program_type, crew_sk, role, description, role_title, rank, modified_at, created_at) \
                     values ( %s, %s, %s,  %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
            values = ( item['program_sk'],
                       item['program_type'],  item['crew_sk'],              
                       item['role'], item['description'],          
                       item['role_title'], item['rank'])

            self.cursor.execute(query, values)

        if isinstance(item, PopularityItem):
            query = "insert into Popularity(program_sk, program_type, no_of_views, no_of_ratings, no_of_reviews,\
                     no_of_comments, no_of_likes, no_of_dislikes, modified_at, created_at) values \
                     ( %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key \
                     update no_of_views=%s, no_of_ratings=%s, no_of_reviews=%s, no_of_comments=%s, \
                     no_of_likes=%s, no_of_dislikes=%s, modified_at = now()"
            values = (item['program_sk'], item['program_type'],        
                      item['no_of_views'], item['no_of_ratings'],       
                      item['no_of_reviews'], item['no_of_comments'],      
                      item['no_of_likes'], item['no_of_dislikes'], 
                      item['no_of_views'], item['no_of_ratings'],             
                      item['no_of_reviews'], item['no_of_comments'],          
                      item['no_of_likes'], item['no_of_dislikes'])

            print query % values

        if isinstance(item, CrewItem):
            query = "insert into Crew(sk, name, original_name, description, aka, gender, birth_date, death_date, country, biography, image,  reference_url, modified_at, created_at) values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) on duplicate key update modified_at = now()"
            values = (  item['sk'], item['name'],                 
                        item['original_name'], item['description'],          
                        item['aka'], item['gender'],               
                        item['birth_date'], item['death_date'],           
                        item['country'], item['biography'],            
                        item['image'], item['reference_url'])
            self.cursor.execute(query, values)

        if isinstance(item, SongItem):
            query = "insert into Songs(track_title, movie_id, singers, duration, modified_at, created_at) \
                     values ( %s, %s, %s, %s, now(), now()) on duplicate key update modified_at=now()"
            values = (  item['track_title'],         
                        item['movie_id'],            
                        item['singers'],             
                        item['duration'])
            print query % values 

        return item
