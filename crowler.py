import asyncio
from urllib.parse import urljoin, urldefrag, urlparse

import aiohttp
from aioelasticsearch import Elasticsearch
from bs4 import BeautifulSoup
import requests
start_url = 'http://fargo-online.net/'
index_url = start_url.translate({ord(i): None for i in """"[*\>:\<'"|/?]"""})

class Crowler:
    def __init__(self, url, rps, max_pages):
        self.url = url
        self.id =0
        self.rps = rps
        self.links = [url]
        self.domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(start_url))

    @staticmethod
    def connect_elasticsearch():
        _es = None
        _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        if _es.ping():
            print('Yay Connect')
        else:
            print('Awww it could not connect!')
        return _es

    def create_index(self, es_object, index_name):
        created = False
        settings = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "page": {
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

    async def get_links(self, page):
        soup = BeautifulSoup(page, features="html.parser")
        await asyncio.sleep(0)
        absolute_links = list(map(lambda x: x if x.startswith(('http://', 'https://')) else urljoin(start_url, x),
                                  [i.get('href', '') for i in soup.find_all('a')]))
        links = [urldefrag(x)[0] for x in absolute_links if x.startswith(self.domain)]
        return list(set(links)), soup

    async def store_record(self, elastic_object, index_name,_id, record):
        is_stored = True
        try:
            outcome = await elastic_object.index(index=index_name,doc_type='page',body=record,id=_id)
            # print(outcome)
        except Exception as ex:
            print('Error in indexing data')
            print(str(ex))
            is_stored = False
        finally:
            return is_stored

    async def spider(self, start_link, es, i, session):
        links_to_visit = [start_link]
        async with session.get(start_link) as response:
            for link in links_to_visit:
                new_links,soup = await self.get_links(await response.text())
                if self.create_index(es, index_url):
                    self.id+=1
                    await self.store_record(es, index_url,self.id, {"link": link, "text": await self.beautify_text(soup)})
                    print('Data indexed successfully')
                for new_link in new_links:
                    if new_link not in self.links and new_link not in links_to_visit:
                        self.links.append(new_link)
                        links_to_visit.append(new_link)
                print(i,self.links)

                # await asyncio.sleep(1/self.rps)

    async def main(self):
        es = self.connect_elasticsearch()
        tasks = []
        async with aiohttp.ClientSession() as session:
            async with session.get(start_url) as resp:
                links_to_visit, soup = await self.get_links(await resp.text())
                if self.create_index(es, index_url):
                    await self.store_record(es, index_url,self.id, {"link": start_url, "text": await self.beautify_text(soup)})
                    print('Data indexed successfully')

                self.links.append(links_to_visit)
                print(links_to_visit)

            for i, link in enumerate(links_to_visit):
                task = asyncio.create_task(self.spider(link, es, i,session))
                tasks.append(task)
            await asyncio.gather(*tasks)
        await es.close()




    @staticmethod
    async def beautify_text(soup):
        for script in soup(["script", "style"]):
            script.extract()  # rip it out
        text = soup.get_text()
        await asyncio.sleep(0)
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = '\n'.join(chunk for chunk in chunks if chunk)

        return text


import time

if __name__ == '__main__':
    craw = Crowler(start_url, 100, 200)
    begin = time.time()
    asyncio.run(craw.main())
    print(len(craw.links))
    print(time.time() - begin)
