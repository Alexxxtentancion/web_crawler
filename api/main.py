from aiohttp import web
import json
from test2 import index_url
from aioelasticsearch import Elasticsearch
async def handle(request):
    name = request.match_info.get('name', "Anonymous")
    text = "Hello, " + name
    return web.Response(text=text)

async def new_user(request):
    try:
        # happy path where name is set
        es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        user = request.query['q']
        # Process our new user
        print("Creating new user with name: " , user)

        search_object = {'query': {'match': {'text': user}}}
        res = await es.search(index=index_url, body=json.dumps(search_object))
        # print(res.get('hits').get('hits')[0].get('_source'))

        response_obj = { 'status' : 'success' }
        # return a success json response with status code 200 i.e. 'OK'
        return web.Response(text=json.dumps(res.get('hits').get('hits')), status=200)
    except Exception as e:
        # Bad path where name is not set
        response_obj = { 'status' : 'failed', 'reason': str(e) }
        # return failed with a status code of 500 i.e. 'Server Error'
        return web.Response(text=json.dumps(response_obj), status=500)

app = web.Application()
# app.add_routes([web.get('/', handle),
#                 web.get('/{name}', handle)])
app.router.add_get('/',handle)
app.router.add_post('/api/search', new_user)
web.run_app(app)