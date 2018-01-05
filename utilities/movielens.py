import os
import zipfile
import urllib.request


def download_dataset(dataset_name):
    """
    Download a dataset from MovieLens project.
    """
    dataset_archive_path = os.path.join('../', dataset_name + '.zip')
    dataset_path = os.path.join('../', dataset_name)
    urllib.request.urlretrieve(
        'http://files.grouplens.org/datasets/movielens/' + dataset_name + '.zip', dataset_archive_path)
    with zipfile.ZipFile(dataset_archive_path, 'r') as archive_file:
        archive_file.extractall('..')
    os.rename(dataset_path, '../data/')


if __name__ == '__main__':
    dataset_name = 'ml-latest-small'
    download_dataset(dataset_name)
