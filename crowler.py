from urllib.parse import urljoin, urldefrag, urlparse

import requests
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch
import asyncio
start_url = 'http://fargo-online.net/'


class Crowler:
    def __init__(self, url, rps, max_pages):
        self.url = url
        self.rps = rps
        self.links = [url]
        self.domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(start_url))
        self.index_url = start_url.translate({ord(i): None for i in """"[*\>:\<'"|/?]"""})

    @staticmethod
    async def connect_elasticsearch():
        _es = None
        _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        if _es.ping():
            print('Yay Connect')
        else:
            print('Awww it could not connect!')
        return _es

    def create_index(self, es_object, index_name):
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

    async def get_links(self, url):
        page = requests.get(url).text
        soup = BeautifulSoup(page, features="html.parser")
        await asyncio.sleep(0)
        absolute_links = list(map(lambda x: x if x.startswith(('http://', 'https://')) else urljoin(start_url, x),
                                  [i.get('href', '') for i in soup.find_all('a')]))
        links = [urldefrag(x)[0] for x in absolute_links if x.startswith(self.domain)]

        data = await self.beautify_text(soup)

        return links, data

    async def store_record(self, elastic_object, index_name, record):
        is_stored = True
        try:
            outcome = await elastic_object.index(index=index_name, doc_type='text', body=record)
            print(outcome)
        except Exception as ex:
            print('Error in indexing data')
            print(str(ex))
            is_stored = False
        finally:
            return is_stored

    async def spider(self,start_link,es):
        # es = await self.connect_elasticsearch()
        links_to_visit = [start_link]
        for link in links_to_visit:
            print(link)
            new_links, data = await self.get_links(link)
            res = {"link": link, "text": data}
            if self.create_index(es, self.index_url):
                await self.store_record(es, self.index_url, res)
                print('Data indexed successfully')
            for new_link in new_links:
                if new_link not in self.links and new_link not in links_to_visit:
                    self.links.append(new_link)
                    links_to_visit.append(new_link)

    async def main(self):
        es = await self.connect_elasticsearch()
        links_to_visit, data = await self.get_links(start_url)
        self.links.append(links_to_visit)
        tasks = []
        for link in links_to_visit:
            task = asyncio.create_task(self.spider(link,es))
            tasks.append(task)
        await asyncio.gather(*tasks)



    @staticmethod
    async def beautify_text(soup):
        for script in soup(["script", "style"]):
            script.extract()  # rip it out
        text = soup.get_text()
        await asyncio.sleep(0)
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = '\n'.join(chunk for chunk in chunks if chunk)
        res = {'text': text}
        return res


if __name__ == '__main__':
    craw = Crowler(start_url, 10, 200)
    asyncio.run(craw.main())
