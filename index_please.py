#!/usr/bin/env python3

import asyncio
import memcache
import string
import time

from aiohttp import web

mc = memcache.Client(['127.0.0.1:11211'], debug=False)

def wait_for_lock():
    global mc

    while mc.get('index_please_lock'):
        print('locked, waiting')
        time.sleep(0.01)

    return

@asyncio.coroutine
def handle(request):
    global mc

    print(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()) , request.path)
    ddd_id = request.match_info.get('ddd_id', "")

    if ddd_id:

        for i in ddd_id:
            if not i in string.printable:
                return web.Response(body='error'.encode('utf-8'))


        if ddd_id == 'reset':
            wait_for_lock()
            mc.set('index_please_lock', True)
            mc.set('index_please', [])
            mc.set('index_please_lock', False)
            return web.Response(body="reset".encode('utf-8'))


        wait_for_lock()
        mc.set('index_please_lock', True)
        current_que = mc.get('index_please')
        if not current_que:
            current_que = [ddd_id]
        else:
            current_que = mc.get('index_please')
            current_que.append(ddd_id)
        mc.set('index_please', current_que)
        mc.set('index_please_lock', False)
        text = ','.join(current_que)
        return web.Response(body=text.encode('utf-8'))

    return web.Response(body="ok".encode('utf-8'))

# Remove lock on startup.
mc.set('index_please_lock', False)
app = web.Application()
app.router.add_route('GET', '/', handle)
app.router.add_route('GET', '/{ddd_id}', handle)
