import json
from flask import Flask, Blueprint, request, render_template, flash
from wtforms import Form, validators, StringField
import logging
from recommender import Recommender
from utilities.misc import is_int, is_float
import codecs

main = Blueprint('main', __name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ReusableForm(Form):
    name = StringField('Name:', validators=[validators.required()])
    user_id = StringField('User ID:', validators=[validators.required()])
    movie_id = StringField('Movie ID:', validators=[validators.required()])
    rating = StringField('Rating:', validators=[validators.required()])


@main.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')


@main.route('/predict', methods=['GET', 'POST'])
def predict_interests():
    form = ReusableForm(request.form)
    if request.method == 'POST':
        user_id = str(request.form['user_id'])
        if user_id is not None and user_id != '':
            logger.debug('Recommendations requested for user {}.'.format(user_id))
            top_ratings = recommender.predict_interests(user_id)
            response_first = \
                "<!DOCTYPE html> <head> <title>Recommendations</title> </head> <body> " \
                "<h1>Predictions for user ID {}:</h1> ".format(user_id)
            response_middle = ""
            for top_rating in top_ratings:
                movie_id = top_rating[0]
                imdb_id = top_rating[1]
                rating = top_rating[2]
                title = top_rating[3]
                response_middle += "<p>{}</p>".format(title)
            response_last = \
                '</a> </body> <p> <a href="/">Back</a> </p>'
            response = response_first + response_middle + response_last
            with codecs.open('./templates/prediction.html', 'w', 'utf-8') as prediction_file:
                prediction_file.write(response)
            return render_template('prediction.html')
    return render_template('predict.html', form=form)


@main.route('/add_rating', methods=['GET', 'POST'])
def add_rating():
    form = ReusableForm(request.form)
    if request.method == 'POST':
        user_id = str(request.form['user_id'])
        movie_id = str(request.form['movie_id'])
        rating = str(request.form['rating'])
        logger.debug('Addition of (userID, itemID, rating): ({}, {}, {}) requested.'.format(user_id, movie_id, rating))
        if user_id is not None and user_id != '' and is_int(user_id) or \
            movie_id is not None and movie_id != '' and is_int(movie_id) or \
            rating is not None and rating != '' and is_float(rating):
            recommender.add_rating(user_id, movie_id, rating)
            flash('({}, {}, {}) added'.format(user_id, movie_id, rating))
        else:
            flash('Invalid triplet (userID, itemID, rating) entered, try again.')
    return render_template('add_rating.html', form=form)


@main.route('/evaluate', methods=['GET'])
def evaluate_model(spark_context, params):
    logger.debug('Model evaluation.')
    recommender = Recommender(spark_context, params)
    eval_results = recommender.evaluate()
    return json.dumps(eval_results)


def create_app(spark_context, cfg_file_path):
    global recommender
    recommender = Recommender(spark_context, cfg_file_path)
    app = Flask(__name__)
    app.register_blueprint(main)
    app.config['SECRET_KEY'] = '7d441f27d441f27567d441f2b6176a'
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    return app
