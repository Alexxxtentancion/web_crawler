from aiohttp import web
import json
from crawler import index_url


async def search(request,app):
    try:
        query = request.query['q']
        limit = request.query['limit']
        offset = request.query['offset']
        search_object = {'query': {'match': {'text': query}},'size':limit,'from':offset}
        res = await app['es'].search(index=index_url, body=json.dumps(search_object))
        return web.Response(text=json.dumps(res.get('hits').get('hits')), status=200)
    except Exception as e:
        response_obj = { 'status' : 'failed', 'reason': str(e) }
        return web.Response(text=json.dumps(response_obj), status=500)

