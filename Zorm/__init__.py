# coding:utf-8
# from simple_orm_mysql.store import SqlStore
from src.simple_orm_mysql.store import *
from src.simple_orm_mysql.errors import *
from src.simple_orm_mysql.queue_set import *
from src.simple_cache.__init__ import *


class Utils(object):
    def join_where(self, kwargs):
        str_list = []
        index = 0
        for key, value in kwargs.items():
            if not index == len(kwargs):
                _ = lambda key, value: "%s='%s'" % (key, value) if type(value) == str else "%s=%s" % (key, value)
                str_list.append(_(key, value))
                index += 1
        where_sql = ' and '.join(str_list)
        return where_sql

    def join_where_like(self, kwargs):
        str_list = []
        index = 0
        for key, value in kwargs.items():
            if not index == len(kwargs):
                _ = lambda key, value: "%s like '%s'" % (key, value)
                str_list.append(_(key, value))
                index += 1
        where_sql = ' and '.join(str_list)
        return where_sql

    def join_where_in(self, kwargs):
        str_list = []
        index = 0
        for key, value in kwargs.items():
            if not index == len(kwargs):
                _ = lambda key, value: "%s in (%s)" % (key, ','.join([str(x)  for x in value]))
                str_list.append(_(key, value))
                index += 1
        where_sql = ' and '.join(str_list)
        return where_sql
class Syntax(object):
    def __init__(self, model, kwargs):
        self.model = model
        self.params = kwargs.values()
        equations = [key + ' = "%s"' % val if type(val) == str  else key + ' = %s' % val for key, val in kwargs.items()]
        # print(equations,"equations")
        self.where_expr = 'where ' + ' and '.join(equations) if len(equations) > 0 else ''

    def update(self, **kwargs):
        # _keys = []
        # _params = []
        info = []
        for key, val in kwargs.items():
            # print(key, val)
            # print(self.model.field_names)
            if val is None or key not in [x.replace("`", "") for x in self.model.field_names]:
                continue
            info.append([key, val])
            # _keys.append(key)
            # _params.append(val)
        # _params.extend(self.params)
        # print(info)
        sql = 'update %s set %s %s;' % (self.model.table_name, ', '.join(
            [data[0] + ' = "%s"' % data[1] if type(data[1]) == str  else data[0] + ' = %s' % data[1] for data in info]),
                                        self.where_expr)
        # print(sql)
        # return sql, _params
        if self.model.cursor:
            return self.model.cursor._execute(sql)
        else:
            return False

    def limit(self, rows, offset=None):
        self.where_expr += ' limit %s%s' % (
            '%s, ' % offset if offset is not None else '', rows)
        return self

    def select(self):
        sql = 'select %s from %s %s;' % (', '.join(self.model.fields.keys()), self.model.table_name, self.where_expr)
        for row in Database.execute(sql, self.params).fetchall():
            inst = self.model()
            for idx, f in enumerate(row):
                setattr(inst, self.model.fields.keys()[idx], f)
            yield inst

    def count(self):
        sql = 'select count(*) from %s %s;' % (self.model.table_name, self.where_expr)
        (row_cnt,) = Database.execute(sql, self.params).fetchone()
        return row_cnt


class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        print(cls, name, bases, attrs)
        print(attrs['Meta'].database)
        print('Found model: %s' % name)
        mappings = dict()
        for k, v in attrs.iteritems():
            if isinstance(v, Field):
                print('Found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
        for k in mappings.iterkeys():
            attrs.pop(k)
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__table__'] = name  # 假设表名和类名一致
        return type.__new__(cls, name, bases, attrs)


class Model(Utils):
    __metaclass__ = ModelMetaclass

    def __init__(self, rid=0, **kwargs):
        if not getattr(self.__class__, 'table_name'):
            self.table_name = self.__class__.__name__.lower()
        if getattr(self.__class__, 'db_config'):
            self.db_config = getattr(self.__class__, 'db_config')
        else:
            raise NotFindArgv('not find db_config argv')
        try:
            if not getattr(self.__class__, 'cache_use'):
                self.cache_use = False
            if not self.cache_use is False and not self.cache_use is True:
                raise FieldError('cache_use must be True or False')
            elif self.cache_use is True:
                if getattr(self.__class__, 'cache_config'):
                    info = getattr(self.__class__, 'cache_config')
                    self.cache_con = Cache(**info)
                else:
                    raise NotFindArgv('not find cache_config argv')
                try:
                    if getattr(self.__class__, 'cache_timeout'):
                        self.cache_timeout = getattr(self.__class__, 'cache_timeout')
                except:
                    self.cache_timeout = 3600
        except:
            raise NotFindArgv('cache config wrong')
        for name in self.field_names:
            field = getattr(self.__class__, name.replace("`", ""))
            setattr(self, name.replace("`", ""), field.default)
        for key, value in kwargs.items():
            setattr(self, key.replace("`", ""), value)
        if getattr(self.__class__, 'debug'):
            self.debug()
        self.build_conn()

    def build_conn(self):
        self.cursor = sql_connect(**self.db_config)

    def debug(self):
        for name in dir(self.__class__):
            print(name, '-------', getattr(self.__class__, name))

    @property
    def field_names(self):
        names = []
        for name in dir(self.__class__):
            var = getattr(self.__class__, name.replace("`", ""))
            if isinstance(var, Field):
                names.append("`%s`" % name)
        return names

    @property
    def field_names_nomark(self):
        names = []
        for name in dir(self.__class__):
            var = getattr(self.__class__, name.replace("`", ""))
            if isinstance(var, Field):
                names.append("%s" % name)
        return names

    @property
    def field_values(self):
        values = []
        for name in self.field_names:
            value = getattr(self, name.replace("`", ""))
            if isinstance(value, Model):
                value = value.id
            if isinstance(value, str):
                value = value.replace("'", "''")
                try:
                    value = value.decode("gbk")
                except:
                    pass
                try:
                    value = value.decode("utf8")
                except:
                    pass
            values.append("'%s'" % value)
        return values

    def insert(self):
        field_names_sql = ", ".join(self.field_names)
        field_values_sql = ", ".join(self.field_values)

        sql = "insert into %s(%s) values(%s)" % (self.table_name, field_names_sql, field_values_sql)
        return self.cursor._execute(sql)

    # def insert(self, **kwargs):



    def insert_many(self, keys, params):
        sql = "insert into %s(%s) values (%s)" %(self.table_name, ','.join(keys), ','.join(['%s' for x in range(len(keys))]))
        # print(sql, params)
        # import sys
        # sys.exit(0)
        print(sql)
        return self.cursor._executemany(sql,params)

    def update(self):
        name_value = []
        for name, value in zip(self.field_names, self.field_values):
            name_value.append("%s=%s" % (name, value))
        name_value_sql = ", ".join(name_value)

        sql = "update `%s` set %s where id = %d" % (self.table_name, name_value_sql, self.id)
        return self.cursor._execute(sql)

    def save(self):
        self.insert()

    def delete(self, **kwargs):
        where_sql = self.join_where(kwargs)
        sql = "delete from %s where %s" % (self.table_name, where_sql)
        return self.cursor._execute(sql)

    def get(self, **kwargs):
        where_sql = self.join_where(kwargs)
        field_names_sql = ", ".join(self.field_names)
        sql = "select %s from %s where %s" % (field_names_sql, self.table_name, where_sql)
        return self.execute(sql)

    def like(self,  limit=0, **kwargs):
        field_names_sql = ", ".join(self.field_names)
        where_sql = self.join_where_like(kwargs)
        sql = self.return_select_sql(where_sql, field_names_sql, limit)
        return self.execute(sql)

    def filter_in(self,  limit=0, **kwargs):
        field_names_sql = ", ".join(self.field_names)
        where_sql = self.join_where_in(kwargs)
        sql = self.return_select_sql(where_sql, field_names_sql, limit)
        return self.execute(sql)

    def filter(self, limit=0, **kwargs):
        where_sql = self.join_where(kwargs)
        field_names_sql = ", ".join(self.field_names)
        sql = self.return_select_sql(where_sql, field_names_sql, limit)
        # print(sql)
        # print(field_names_sql)
        # print(self.table_name)
        # print(where_sql)
        return self.execute(sql)

    def return_select_sql(self, where_sql, field_names_sql, limit):
        if not limit:
            if where_sql:
                sql = "select %s from %s where %s" % (field_names_sql, self.table_name, where_sql)
            else:
                sql = "select %s from %s" % (field_names_sql, self.table_name)
        else:
            if where_sql:
                sql = "select %s from %s where %s limit %s, %s" % (field_names_sql, self.table_name, where_sql, limit[0], limit[1])
            else:
                sql = "select %s from %s limit %s, %s" % (field_names_sql, self.table_name, limit[0], limit[1])
        return sql

    # def filter_limit(self, limit=10, **kwargs):
    #     where_sql = self.join_where(kwargs)
    #     field_names_sql = ", ".join(self.field_names)
    #     if where_sql:
    #         sql = "select %s from %s where %s limit %s" % (field_names_sql, self.table_name, where_sql, limit)
    #     else:
    #         sql = "select %s from %s limit %s" % (field_names_sql, self.table_name, limit)
    #     return self.execute(sql)

    def all(self):
        field_names_sql = ", ".join(self.field_names)
        sql = "select %s from %s" % (field_names_sql, self.table_name)
        return self.execute(sql)

    def where(self, **kwargs):
        return Syntax(self, kwargs)

    def execute(self, sql):
        # print(sql)
        if self.cache_use:
            data = self.cache_con.find_cache(self.table_name, sql)
            if data['status'] == 200:
                return queue_set(self.field_names, data['info']).info
            else:
                info = self.cursor._execute(sql)
                # print(info)
                self.cache_con.set_cache(self.table_name, sql, info, self.cache_timeout)
                return queue_set(self.field_names, info).info
        else:
            return queue_set(self.field_names, self.cursor._execute(sql)).info

    def execute_secure(self, sql, params):
        print(sql,params)
        return queue_set(self.field_names, self.cursor._execute_secure(sql, params)).info

    def last_id(self):
        return self.cursor._lastId()

    def raw_execute(self, sql):
        # print(sql)
        return self.cursor._execute(sql)

class Field(object):
    field_type = ""
    field_level = 0
    default = ""
    auto_is = False
    def field_sql(self, field_name):
        return '"%s" %s' % (field_name, self.field_type)

class CharField(Field):
    def __init__(self, max_length=255, default="", auto_is = False):
        self.field_type = "varchar(%d)" % max_length
        self.default = default
        self.max_length = max_length
        self.auto_is = auto_is

class IntField(Field):
    def __init__(self, default=None, auto_is = False):
        self.default = default
        self.auto_is = auto_is

class TextField(Field):
    def __init__(self, default=None, auto_is = False):
        self.default = default
        self.auto_is = auto_is

def ValidField(max_length):
    if max_length > 255:
        raise FieldError('max_lenth lt 255')

