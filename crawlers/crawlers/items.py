# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field
class MovieItem(Item):
    sk                     = Field()
    title                  = Field()
    description            = Field()
    genres                 = Field()
    duration               = Field()
    release_year           = Field()
    languages              = Field()
    original_languages     = Field()
    aka                    = Field()
    production_country     = Field()
    reference_url          = Field()

class ProgramCrewItem(Item):
    program_sk           = Field()
    program_type         = Field()
    crew_sk              = Field()
    role                 = Field()
    description          = Field()
    role_title           = Field()
    rank                 = Field()

class PopularityItem(Item):
    program_sk          = Field()
    program_type        = Field()
    no_of_views         = Field()
    no_of_ratings       = Field()
    no_of_reviews       = Field()
    no_of_comments      = Field()
    no_of_likes         = Field()
    no_of_dislikes      = Field()

class CrewItem(Item):
    sk                   = Field()
    name                 = Field()
    original_name        = Field()
    description          = Field()
    aka                  = Field()
    gender               = Field()
    birth_date           = Field()
    death_date           = Field()
    country              = Field()
    biography            = Field()
    height               = Field()
    weight               = Field()
    image                = Field()
    reference_url        = Field()

class SongItem(Item):
    track_title         = Field()
    movie_id            = Field()
    singers             = Field()
    duration            = Field()
