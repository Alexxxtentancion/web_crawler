from aiohttp import web
import json
from crowler import index_url
from aioelasticsearch import Elasticsearch


async def search(request):
    try:
        es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        query = request.query['q']
        limit = request.query['limit']
        offset = request.query['offset']
        search_object = {'query': {'match': {'text': query}},'size':limit,'from':offset}
        res = await es.search(index=index_url, body=json.dumps(search_object))
        return web.Response(text=json.dumps(res.get('hits').get('hits')), status=200)
    except Exception as e:
        response_obj = { 'status' : 'failed', 'reason': str(e) }
        return web.Response(text=json.dumps(response_obj), status=500)

