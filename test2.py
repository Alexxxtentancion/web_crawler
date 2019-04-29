from elasticsearch import Elasticsearch
start_url = 'http://fargo-online.net/'
import json
index_url = start_url.translate({ord(i): None for i in """"[*\>:\<'"|/?]"""})
def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    return _es


def search(es_object, index_name, search):
    res = es_object.search(index=index_name, body=search)
    print(res)


if __name__ == '__main__':
    # craw = Crowler(start_url, 10, 200)
    es = connect_elasticsearch()
    search_object = {'query': {'match': {'text': 'Лестер Нигар'}}}
    search(es, index_url, json.dumps(search_object))
