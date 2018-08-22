from simple_cache.redis_con import * 
import time
import hashlib


class Cache(object):

    def __init__(self, **kwargs):
        self.conn = redis_con(**kwargs)

    def hash_data(self, sql):
        md5obj=hashlib.md5()
        md5obj.update(sql.encode())
        return md5obj.hexdigest()

    def find_cache(self,table_name, sql):
        if self.conn._exist_data(table_name +self.hash_data(sql)):
            info = self.conn._get_data(name=table_name +self.hash_data(sql), key='timeout')
            if json.loads(info.decode()) < time.time():
                return {'status':400, 'msg':'timeout', 'info':''}
            info = self.conn._get_data(name=table_name +self.hash_data(sql), key='info')
            return {'status':200, 'msg':'success', 'info':json.loads(info.decode())}
        return {'status':400, 'msg':'no Cache', 'info':''}

    def set_cache(self,table_name, sql, info, timeout):
        self.conn._set_data(table_name + self.hash_data(sql), \
                        {'timeout':time.time()+timeout, 'sql':sql, 'info':json.dumps(info)})

    def flush_all(self):
        pass
cache_config = {"host": '10.21.8.37',
                "port": 6379,
                "password": '@_redis&redis_@'}
