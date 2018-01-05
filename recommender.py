import os
import shutil
import logging
from pyspark.mllib.recommendation import ALS
from sklearn.metrics import roc_auc_score
from time import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import configparser
import pandas as pd
from pyspark import SQLContext


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Recommender(object):
    """
    Recommendation system.
    """
    def __init__(self, spark_context, cfg_file_path=None):
        """
        Initialize the recommendation system.
        """
        logger.info('Initializing the Recommendation System...')
        config = configparser.RawConfigParser()
        config.read(cfg_file_path)
        self.db_type = 'postgresql'
        self.db_driver = 'psycopg2'
        self.db_user = config.get('db', 'username')
        self.db_pass = config.get('db', 'password')
        self.db_host = config.get('db', 'host')
        self.db_port = config.get('db', 'port')
        self.db_name = config.get('db', 'name')
        self.rank = config.getint('recommender', 'rank')
        self.iterations = config.getint('recommender', 'iterations')
        self.lambda_ = config.getfloat('recommender', 'lambda_')
        self.update_batch_size = config.getint('recommender', 'update_batch_size')
        self.model_path = config.get('recommender', 'path')
        self.sc = spark_context
        self.sqlc = SQLContext(sparkContext=self.sc)
        pool_size = 100
        sqlalchemy_database_uri = \
            '%s+%s://%s:%s@%s:%s/%s' % \
            (self.db_type, self.db_driver, self.db_user, self.db_pass, self.db_host, self.db_port, self.db_name)
        self.sql_engine = create_engine(sqlalchemy_database_uri, pool_size=pool_size, max_overflow=0)
        Internal = sessionmaker(bind=self.sql_engine)
        self.sql_internal = Internal()
        # Load the data.
        self.load_ratings()
        self.batch_count = 0
        # # Load or train the model.
        # if os.path.isdir(self.model_path):
        #     logger.info('Found model, loading...')
        #     self.model = MatrixFactorizationModel.load(self.sc, self.model_path)
        #     logger.info('Done.')
        # else:
        #     self.model = None
        #     self.train()
        self.model = None
        self.train()

    def load_ratings(self):
        """
        Load ratings.
        """
        logger.info('Loading Ratings...')
        ratings_df = pd.read_sql_table('ratings', self.sql_engine)
        ratings_df.drop(labels=['timestamp'], axis=1, inplace=True)
        self.ratings_RDD = self.sqlc.createDataFrame(ratings_df)
        logger.info('Done.')

    def predict_interests(self, user_id):
        """
        Get the top ratings for the user ID.
        """
        try:
            top_ratings = \
                [(recommendation[1], recommendation[2]) for recommendation in
                 self.model.recommendProducts(int(user_id), 10)]
        except:
            return []
        sql_query_imdbId = 'SELECT "imdbId" FROM links WHERE "movieId" = '
        sql_query_title = 'SELECT "title" FROM movies WHERE "movieId" = '
        response = []
        for movie_id_rating in top_ratings:
            movie_id = movie_id_rating[0]
            rating = movie_id_rating[1]
            imdb_id_df = pd.read_sql_query(sql_query_imdbId + str(movie_id), self.sql_engine)
            title_df = pd.read_sql_query(sql_query_title + str(movie_id), self.sql_engine)
            imdb_id = imdb_id_df.iloc[0]['imdbId']
            title = title_df.iloc[0]['title']
            response.append((movie_id, imdb_id, rating, title))
        return response

    def train(self):
        """
        Create the model on all data.
        """
        logger.info('Training the ALS model...')
        model = ALS.train(
            self.ratings_RDD,
            rank=self.rank,
            iterations=self.iterations,
            lambda_=self.lambda_
        )
        self.model = model
        logger.info('Done.')
        logger.info('Saving the ALS model in {}...'.format(self.model_path))
        if not os.path.isdir(self.model_path):
            os.mkdir(self.model_path)
        else:
            shutil.rmtree(self.model_path)
            os.mkdir(self.model_path)
        self.model.save(self.sc, self.model_path)
        logger.info('Done.')

    def add_rating(self, user_id, movie_id, rating):
        """
        Add additional movie interest in the format (user_id, movie_id, rating).
        """
        timestamp = 0
        try:
            self.sql_internal.execute(
                """INSERT INTO ratings (\"userId\", \"movieId\", \"rating\", \"timestamp\") 
                VALUES ({}, {}, {}, {})""".format(user_id, movie_id, rating, timestamp)
            )
            self.sql_internal.commit()
            self.batch_count += 1
            if self.batch_count >= self.update_batch_size:
                self.train()
                self.load_ratings()
                self.batch_count = 0
        except:
            pass
        return None

    def evaluate(self):
        """
        Measure the Area Under the Curve AUC and time elapsed for testing.
        """
        logger.info('ALS evaluation.')
        max_user_id = self.ratings_RDD.max()[0]
        max_item_id = self.ratings_RDD.max(lambda x: x[1])[1]
        # Training and test set split.
        train, test = self.ratings_RDD.randomSplit([0.8, 0.2])
        train.cache()
        test.cache()
        # Create the model on the training data.
        logger.info('Training the ALS model for rank: {}, iterations: {}, lambda: {}...'.format(
            self.rank, self.iterations, self.lambda_))
        model = ALS.train(
            train,
            rank=self.rank,
            iterations=self.iterations,
            lambda_=self.lambda_)
        logger.info('Done.')

        # Test set evaluation.
        true_coords = test.map(lambda x: (x[0], x[1]))
        start = time()
        pred_ratings = model.predictAll(true_coords)
        print('Time elapsed for testing: {0:.3f} sec'.format(time() - start))

        true_coords = true_coords.collect()
        true_labels = [0] * max_user_id * max_item_id
        for user_item in true_coords:
            ind = (user_item[0] - 1) * (user_item[1] - 1) + max_item_id
            true_labels[ind] = 1.0
        pred_ratings = pred_ratings.collect()
        pred_scores = [0] * max_user_id * max_item_id
        for rating in pred_ratings:
            ind = (rating[0] - 1) * (rating[1] - 1) + max_item_id
            if rating[2] < 0:
                pred_scores[ind] = rating[2]
            else:
                pred_scores[ind] = rating[2]
        auc = roc_auc_score(true_labels, pred_scores)
        logger.info('AUC: {} for rank: {}, iterations: {}, lambda: {}...'.format(
            auc, self.rank, self.iterations, self.lambda_))
        results = {
            'AUC score': auc,
        }
        return results

