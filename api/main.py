from aioelasticsearch import Elasticsearch
from aiohttp import web

from api.view import search

async def connect_elasticsearch():
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if await _es.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    return _es


if __name__ == '__main__':
    es = connect_elasticsearch()
    app = web.Application()
    app.router.add_post('/api/search', search)
    web.run_app(app)



