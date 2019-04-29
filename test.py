import asyncio
from urllib.parse import urljoin, urldefrag, urlparse
import time
import requests
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch

start_url = 'http://site-of-thrones.ru/'
domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(start_url))
index_url = start_url.translate({ord(i): None for i in """"[*\>:\<'"|/?]"""})


def beautify_text(soup):
    for script in soup(["script", "style"]):
        script.extract()  # rip it out
    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)
    res = {'text': text}
    return res


def get_links(url):
    page = requests.get(url).text
    soup = BeautifulSoup(page, features="html.parser")
    absolute_links = list(map(lambda x: x if x.startswith(('http://', 'https://')) else urljoin(start_url, x),
                              [i.get('href', '') for i in soup.find_all('a')]))
    links = [urldefrag(x)[0] for x in absolute_links if x.startswith(domain)]

    data = beautify_text(soup)
    # print(links)
    print(list(set(links)))
    return list(set(links)), data


def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Есть контакт!')
    else:
        print('Awww it could not connect!')
    return _es


def create_index(es_object, index_name):
    created = False
    # index settings
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "text": {
                "dynamic": "strict",
                "properties": {
                    "link": {
                        "type": "text"
                    },
                    "text": {
                        "type": "text"
                    }
                }
            }
        }
    }

    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def store_record(elastic_object, index_name, record):
    is_stored = True
    try:
        outcome = elastic_object.index(index=index_name, doc_type='text', body=record)
        print(outcome)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


# async def spider(start_link, es):
#     # logging.basicConfig(level=logging.ERROR)
#     links_to_visit = [start_link]
#     for link in links_to_visit:
#         print(link)
#         new_links, data = await get_links(link)
#         res = {"link": link, "text": data}
#         print(res)
#         if create_index(es, index_url):
#             await store_record(es, index_url, res)
#             print('Data indexed successfully')
#         for new_link in new_links:
#             if new_link not in links_to_visit:
#                 links_to_visit.append(new_link)


# async def main():
#     es = await connect_elasticsearch()
#     links_to_visit, data = await get_links(start_url)
#
#     tasks = []
#     for link in links_to_visit:
#         task = asyncio.create_task(spider(link, es))
#         tasks.append(task)
#     await asyncio.gather(*tasks)


if __name__ == '__main__':
    es = connect_elasticsearch()
    # logging.basicConfig(level=logging.ERROR)
    links_to_visit = [start_url]
    # print(get_links(start_url))
    begin = time.time()

    for link in links_to_visit:
        print(link)
        new_links, data = get_links(link)
        res = {"link": link, "text": data}
        if create_index(es, link):
            out = store_record(es, index_url, res)
            print('Data indexed successfully')
        for new_link in new_links:
            if new_link not in links_to_visit:
                links_to_visit.append(new_link)
    print(time.time()-begin)
    print(len(links_to_visit))
