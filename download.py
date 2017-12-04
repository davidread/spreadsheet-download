#import multiprocessing
import os
import re
import posixpath
from urllib.parse import urlsplit, unquote

import requests

def download(url, id_='abc', extension='xls'):
    filename = url2filename(url)
    filepath = apply_filepath_conventions('dgu_data', filename, id_, extension)
    download_file(url, filepath)
    print('Downloaded: {}'.format(filepath))


def download_file(url, filename):
    r = requests.get(url, stream=True)  # stream it to disk
    with open(filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)


def url2filename(url):
    """Return basename corresponding to url.
    >>> print(url2filename('http://example.com/path/to/filename%C3%80?opt=1'))
    filename
    """
    urlpath = urlsplit(url).path
    basename = posixpath.basename(unquote(urlpath)) or 'no-name'
    basename = re.sub(r'[\.]', '-',  basename)  # punctuation
    basename = re.sub(r'[^a-zA-Z0-9_-]', '',  basename)  # whitelist characters
    return basename
    
    
def apply_filepath_conventions(path, basename, id, extension):
    filename = '{}-{}.{}'.format(basename, id, extension)
    return os.path.join(path, filename)
