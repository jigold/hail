from aiohttp import web
from .batch import app
from .globals import init_db

init_db()
web.run_app(app, host='0.0.0.0', port=5000)
