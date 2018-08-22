import redis
import json
class redis_con(object):

    def __init__ (self, **kwargs):
        self.config = kwargs
        self.status = False

    def _check_con(func):
        def check(self, *args, **kwargs):
            if not self.status:
                self._connect()
            return func(self, *args, **kwargs)
        return check

    def _connect(self):
        self.conn = redis.Redis(**self.config)
        self.status = True

    @_check_con
    def _get_data(self, name, key):
        return self.conn.hget(name,key)

    @_check_con
    def _set_data(self, name, info):
        return self.conn.hmset(name ,info)

    @_check_con
    def _del_data(self, name, filter=''):
        return self.conn.delete(name)

    @_check_con
    def _exist_data(self, name):
        return self.conn.exists(name)

    @_check_con
    def _return_keys(self, filter=''):
        return  [key for key in self.conn.keys() if filter in key.decode()]

if __name__ == '__main__':
    a = redis_con(host= '10.21.8.37' ,port= 6379,password='@_redis&redis_@')
    a._connect()
    import time
    a._set_data("selec13",{"timeout":time.time()+300,"info":json.dumps([{'id':1},{"id":2}])})
    a._set_data("selec12",{"timeout":time.time()+300,"info":json.dumps([{'id':1},{"id":2}])})

    info = a._get_data("select * from table where id = 1","timeout")
    print(info)
    # print(type(json.loads(info)['timeout']))
    print(a._exist_data("selec12"))
    # print(a._del_data("select * from table where id = 1"))
    print(a._exist_data("selec13"))
    info = a._return_keys("")
    print(a._del_data(info))
    print(a._return_keys("selec1"))
    print(a.conn.scan())
