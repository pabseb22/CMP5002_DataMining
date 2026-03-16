from pathlib import Path
import tarfile
import urllib.request

def load_housing_data():
    path = Path('../data/housing.tgz')

    if not path.is_file():
        Path('../data').mkdir(exist_ok=True)
        url = 'https://github.com/ageron/data/raw/main/housing.tgz'

        urllib.request.urlretrieve(url, path)

        with tarfile.open(path) as tar:
            tar.extractall(path='./data/raw')

if __name__ == '__main__':    
    load_housing_data()