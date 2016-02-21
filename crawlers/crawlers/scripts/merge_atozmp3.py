import MySQLdb
import difflib

words_to_removed = ['Trailers', 'Trailer', 'Theatrical', 'Latest', 'Official:', 'First Look', "official", 'audio', 'teaser']

class MergeMaango:
    def __init__(self):
        self.maango_db  = "ATOZMP3"
        self.youtube_db = "RT"
        self.host       = "localhost"
        self.whole_dict = {}

    def create_cursor(self, db_name):
        cursor = MySQLdb.connect(db=db_name, host=self.host,
                                 passwd="hdrn59!", user="root").cursor()
        return cursor


    def fetch_utube_records(self):
        cursor = self.create_cursor(self.maango_db)
        query  = 'select sk, title, release_year from Movie'
        cursor.execute(query)

        for record in cursor.fetchall():
            sk, title, release_year = record

            start_char = title[0]
            self.whole_dict.setdefault(start_char, list())
            self.whole_dict[start_char].append((sk, title, release_year))

    def clean_title(self, title):
        for i in words_to_removed:
            title = title.lower().replace(i.lower(), "")
        return title.strip()

    def word_by_word_merge(self, title, title_to_compare):
        title_ = self.clean_title(title).split(" ")

        count = 0
        toc_list = title_to_compare.split(" ")
        for ti in toc_list:
            if ti.lower() in title_:
                count += 1

        if count == len(toc_list):
            return True

    def fetch_maango_records(self):
        cursor = self.create_cursor(self.youtube_db)
        query  = "select video_id, title, org_title from youtube_trailers"
        cursor.execute(query)

        for record in cursor.fetchall():
            video_id, title, org_title = record

            start_char = title[0]
            list_to_compare = self.whole_dict.get(start_char)
            if not list_to_compare: continue

            for li in list_to_compare:
                sk, ti, release_year = li
                result = self.word_by_word_merge(title, ti)

                if result:
                    data_dict = {"title": ti, "maango_sk": sk, "youtube_sk": video_id}
                    print data_dict

    def main(self):
        self.fetch_utube_records()
        self.fetch_maango_records()

if __name__ == "__main__":
    MergeMaango().main()
