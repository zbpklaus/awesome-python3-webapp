# -*- coding: utf-8 -*-
# @Time    : 2018/9/26 08:26
# @Author  : Klaus
# @File    : test_db.py
# @Software: PyCharm


import asyncio
import orm
import time

from models import User, Blog, Comment

async def test(loop):
    await orm.create_pool(loop, user='root', password='zbp8331712', db='awesome')
    u = User(name='Test', email='test@example.com', passwd='1234567890', image='about:blank', id='110', admin=True, created_at=time.time)
    await u.save()


loop = asyncio.get_event_loop()
loop.run_until_complete(test(loop))
loop.run_forever()

