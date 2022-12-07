import HTMLParser
import time
import urlparse
from datetime import datetime, timedelta
 
from tornado import httpclient, gen, ioloop
 
import motor
import toro
 
from bs4 import BeautifulSoup
from pymongo.errors import DuplicateKeyError
 
 
@gen.coroutine
def spider(base_url, concurrency, db):
    q = toro.JoinableQueue()
    sem = toro.BoundedSemaphore(concurrency)
 
    start = time.time()
    fetching, fetched = set(), set()
 
    @gen.coroutine
    def fetch_url(db):
        current_url = yield q.get()
        try:
            if current_url in fetching:
                return
 
            print 'fetching', current_url
            fetching.add(current_url)
            urls = yield get_links_from_url(current_url, db)
            fetched.add(current_url)
 
            for new_url in urls:
                if new_url.startswith(base_url):
                    yield q.put(new_url)
 
        finally:
            q.task_done()
            sem.release()
 
    @gen.coroutine
    def worker(db):
        while True:
            yield sem.acquire()
            fetch_url(db)
 
    q.put(base_url)
 
    worker(db)
    yield q.join(deadline=timedelta(seconds=300))
    assert fetching == fetched
    print 'Done in %d seconds, fetched %s URLs.' % (
        time.time() - start, len(fetched))
 
 
def get_data_from_response(url, response):
    soup = BeautifulSoup(response.body)
    try:
        date = soup.find('meta', {'itemprop': 'datePublished'})['content']  # YYYY-MM-DD
        time = soup.find('time', {'class': 'fecha'}).text[-5:] # HH:MM
        str_date = '{} {}'.format(date, time)
        data = {
            'category': 'tecnologia',
            'titulo': soup.find('h1', {'itemprop': 'headline'}).text,
            'tags': [link.text for link in soup.find('div', {'class': 'nota-tags'}).findAll('a')],
            'fecha': datetime.strptime(str_date, '%Y-%m-%d %H:%M'),
            'noticia': soup.find('div', {'class': 'nota-texto'}).text,
            'url': url,
            'fetched': datetime.now()
        }
    except Exception as e:
        return None
    else:
        return data
 
 
@gen.coroutine
def get_links_from_url(url, db):
    try:
        response = yield httpclient.AsyncHTTPClient().fetch(url)
        print 'fetched', url
        data = get_data_from_response(url, response)
        if data:
            try:
                yield db.news.insert(data)
            except DuplicateKeyError:
                pass
        urls = [urlparse.urljoin(url, remove_fragment(new_url))
                for new_url in get_links(response.body)]
    except Exception as e:
        print e, url
        raise gen.Return([])
 
    raise gen.Return(urls)
 
 
def remove_fragment(url):
    scheme, netloc, url, params, query, fragment = urlparse.urlparse(url)
    return urlparse.urlunparse((scheme, netloc, url, params, '', ''))
 
 
def get_links(html):
    class URLSeeker(HTMLParser.HTMLParser):
        def __init__(self):
            HTMLParser.HTMLParser.__init__(self)
            self.urls = []
 
        def handle_starttag(self, tag, attrs):
            href = dict(attrs).get('href')
            if href and tag == 'a':
                self.urls.append(href)
 
    url_seeker = URLSeeker()
    url_seeker.feed(html)
    return url_seeker.urls
 
 
if __name__ == '__main__':
    import logging
    logging.basicConfig()
    loop = ioloop.IOLoop.current()
 
    def stop(future):
        loop.stop()
        future.result()  
 
    client = motor.MotorClient()
    loop.run_sync(client.open)
    db = client.noticias
    future = spider('http://elcomercio.pe/tecnologia', 10, db)
    future.add_done_callback(stop)
    loop.start()
