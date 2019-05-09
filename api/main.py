from aioelasticsearch import Elasticsearch
from aiohttp import web

from api.view import search

async def connect_elasticsearch(app):
    app['es'] = Elasticsearch([{'host': 'localhost', 'port': 9200}])




if __name__ == '__main__':
    app = web.Application()
    app.on_startup.append(connect_elasticsearch)
    app.router.add_post('/api/search', search)
    web.run_app(app)



