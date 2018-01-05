import configparser
import pandas as pd
from sqlalchemy import create_engine
import argparse


class DataToDB(object):
    def __init__(self, cfg_file_path=None):
        config = configparser.RawConfigParser()
        config.read(cfg_file_path)
        self.ratings_csv = config.get('data', 'ratings_csv')
        self.movies_csv = config.get('data', 'movies_csv')
        self.links_csv = config.get('data', 'links_csv')
        db_type = 'postgresql'
        db_driver = 'psycopg2'
        db_user = config.get('db', 'username')
        db_pass = config.get('db', 'password')
        db_host = config.get('db', 'host')
        db_port = config.get('db', 'port')
        db_name = config.get('db', 'name')
        pool_size = 50
        sqlalchemy_database_uri = \
            '%s+%s://%s:%s@%s:%s/%s' % (db_type, db_driver, db_user, db_pass, db_host, db_port, db_name)
        self.engine = create_engine(sqlalchemy_database_uri, pool_size=pool_size, max_overflow=0)
        self.setup_db()

    def setup_db(self):
        self.engine.execute("""DROP TABLE IF EXISTS ratings;""")
        self.engine.execute(
            """
            CREATE TABLE ratings (
                "userId" INTEGER,
                "movieId" INTEGER,
                "rating" FLOAT,
                "timestamp" INTEGER
            );
            """
        )
        self.engine.execute(
            """
            ALTER TABLE ratings
            ADD CONSTRAINT PK_rating PRIMARY KEY ("userId", "movieId");
            """
        )
        self.engine.execute("""DROP TABLE IF EXISTS movies;""")
        self.engine.execute(
            """
            CREATE TABLE movies (
                "movieId" INTEGER,
                "title" TEXT,
                "genres" TEXT
            );
            """
        )
        self.engine.execute("""DROP TABLE IF EXISTS links;""")
        self.engine.execute(
            """
            CREATE TABLE links (
                "movieId" INTEGER,
                "imdbId" INTEGER,
                "tmdbId" INTEGER
            );
            """
        )

    def run(self):
        table = 'ratings'
        df = pd.read_csv(self.ratings_csv)
        df = df.dropna()
        df.to_sql(table, self.engine, if_exists='append', index=False)
        table = 'movies'
        df = pd.read_csv(self.movies_csv)
        df = df.dropna()
        df.to_sql(table, self.engine, if_exists='append', index=False)
        table = 'links'
        df = pd.read_csv(self.links_csv)
        df = df.dropna()
        df.to_sql(table, self.engine, if_exists='append', index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c',
        '--cfg_file_path',
        action='store',
        default='default.cfg'
    )
    args = parser.parse_args()
    cfg_file_path = args.cfg_file_path
    data_to_db = DataToDB(cfg_file_path=cfg_file_path)
    data_to_db.run()
