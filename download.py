#import multiprocessing
import os
import re
import posixpath
from urllib.parse import urlsplit, unquote

import requests

def download(url, id_='abc', extension='xls'):
    filename = url2filename(url)
    filepath = apply_filepath_conventions('dgu_data', filename, id_, extension)
    response = download_file(url, filepath)
    print('Downloaded: {}'.format(filepath))
    return filepath, response


def download_file(url, filename):
    response = requests_wrapper(requests.get, url, stream=True)  # stream it to disk
    with open(filename, 'wb') as f:
        for chunk in response.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
    return response


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


class DownloadException(Exception):
    pass

def requests_wrapper(func, *args, **kwargs):
    '''
    Run a requests command, catching any exceptions and reraising them as
    DownloadException. Status errors, such as 404 or 500 do not cause
    exceptions. Instead they are exposed as not response.ok.
    e.g.
    >>> requests_wrapper(requests.get, url, timeout=url_timeout)
    runs:
        res = requests.get(url, timeout=url_timeout)
    '''
    try:
        response = func(*args, **kwargs)
    except requests.exceptions.ConnectionError as e:
        raise DownloadException('Connection error: {}'.format(e))
    except requests.exceptions.HTTPError as e:
        raise DownloadException('Invalid HTTP response: {}'.format(e))
    except requests.exceptions.Timeout as e:
        raise DownloadException('Connection timed out after {}'.format(kwargs.get('timeout', '?')))
    except requests.exceptions.TooManyRedirects as e:
        raise DownloadException('Too many redirects')
    except requests.exceptions.RequestException as e:
        raise DownloadException('Error downloading: {}'.format(e))
    except Exception as e:
        raise DownloadException('Error with the download: {}'.format(e))
    return response