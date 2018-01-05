import cherrypy
from paste.translogger import TransLogger
import configparser
from app import create_app
from pyspark import SparkContext, SparkConf
import argparse


class RecommnderServer(object):
    def __init__(self, cfg_file_path=None):
        self.cfg_file_path = cfg_file_path
        config = configparser.RawConfigParser()
        config.read(cfg_file_path)
        self.socket_port = config.getint('server', 'socket_port')
        self.socket_host = config.get('server', 'socket_host')
        rank = config.getint('recommender', 'rank')
        iterations = config.getint('recommender', 'iterations')
        lambda_ = config.getfloat('recommender', 'lambda_')
        update_batch_size = config.getint('recommender', 'update_batch_size')
        self.recommender_params = {
            'rank': rank,
            'iterations': iterations,
            'lambda_': lambda_,
            'update_batch_size': update_batch_size,
        }
        # Load spark context
        conf = SparkConf().setAppName('recommendation-system-server')
        # IMPORTANT: pass additional Python modules to each worker.
        self.sc = SparkContext(conf=conf, pyFiles=['recommender.py', 'app.py'])

    def run_server(self):
        # Initialize spark context and main app.
        app = create_app(spark_context=self.sc, cfg_file_path=self.cfg_file_path)
        # Enable WSGI access logging via Paste.
        app_logged = TransLogger(app)
        # Mount the WSGI callable object (app) on the root directory.
        cherrypy.tree.graft(app_logged, '/')
        # Set the configuration of the web server.
        cherrypy.config.update({
            'engine.autoreload.on': False,
            'log.screen': True,
            'server.socket_port': self.socket_port,
            'server.socket_host': self.socket_host
        })
        # Start the CherryPy WSGI web server.
        cherrypy.engine.start()
        cherrypy.engine.block()

    # def evaluate(self):
    #     # Initialize spark context.
    #     sc = recommender_server.init_spark_context()
    #     results = evaluate_model(spark_context=sc, params=self.recommender_params)
    #     print('Evaluation results:')
    #     print(results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c',
        '--cfg_file_path',
        action='store',
        default='default.cfg'
    )
    args = parser.parse_args()

    # Initialize and start web server.
    cfg_file_path = args.cfg_file_path
    recommender_server = RecommnderServer(cfg_file_path=cfg_file_path)
    recommender_server.run_server()
