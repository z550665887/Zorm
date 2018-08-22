#coding:utf-8
import pymysql

class sql_connect(object):

    def __init__(self, **kwargs):
        self.config = kwargs
        self.conn=''
        self.conn_status = False

    def _connect(self):
        self.conn = pymysql.connect(**self.config)
        self.conn_status = True

    def _close(self):
        
        self.conn.close()
        self.conn_status = False

    def _execute(self,sql):
        if self.conn_status == False:
            self._connect()
        cur = self.conn.cursor()
        data=cur.fetchmany(cur.execute(sql))
        # print(data)
        cur.close()
        self.conn.commit()
        return data

    def _execute_secure(self, sql, params):
        if self.conn_status == False:
            self._connect()
        cur = self.conn.cursor()
        # sql = cur.mogrify(sql,params)
        # print(sql)
        data=cur.fetchmany(cur.execute(sql, params))
        # print(data)
        cur.close()
        self.conn.commit()
        return data


    def _executemany(self, sql, params):
        if self.conn_status == False:
            self._connect()
        cur = self.conn.cursor()
        data = cur.executemany(sql,params)
        cur.close()
        self.conn.commit()
        return data


    def _lastId(self):
        sql = "SELECT LAST_INSERT_ID()"
        if self.conn_status == False:
            self._connect()
        cur = self.conn.cursor()
        data = cur.fetchmany(cur.execute(sql))
        # print(data)
        cur.close()
        self.conn.commit()
        return data

