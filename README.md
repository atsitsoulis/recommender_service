# recommender_service
Recommendation System for MovieLens, based on ALS. It is a skeleton for a Flask based webservice that allows users to add and predict ratings. The initial data comes from the "ml-latest-small" dataset from MovieLens and its format is adopted. The main engine of the recommendation system is PySpark's ALS (Alternating Least Squares) implementation.

## Important components
- Postgre Database with a table called "movielens" to store the data and metadata.
- Pyspark (ALS)
- Python 3.6

## Usage
1. python ./utilities/movielens.py
2. python ./utilities/db.py -c <cfg_file_path>
3. python ./server.py -c <cfg_file_path>

The argument <cfg_file_path> is the path of the configuration file that contains the parameters for every component.

## ToDo
I am working on a detailed version of the README file.
