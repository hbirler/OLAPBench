import urllib.request
import os


def download_if_not_exists(url, filename):
    if not os.path.isfile(filename):
        print('Downloading ' + url)
        urllib.request.urlretrieve(url, filename)