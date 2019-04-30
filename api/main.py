from aiohttp import web

from api.view import search

if __name__ == '__main__':
    app = web.Application()
    app.router.add_post('/api/search', search)
    web.run_app(app)
