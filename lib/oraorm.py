#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Michael Liao'

'''
Database operation module. This module is independent with web module.
'''

import time, logging
import oradb


class Field(object):

    _count = 0

    def __init__(self, **kw):
        self.name = kw.get('name', None)
        self._default = kw.get('default', None)
        self.primary_key = kw.get('primary_key', False)
        self.nullable = kw.get('nullable', False)
        self.updatable = kw.get('updatable', True)
        self.insertable = kw.get('insertable', True)
        self.ddl = kw.get('ddl', '')
        self._order = Field._count
        Field._count = Field._count + 1

    @property
    def default(self):
        d = self._default
        return d() if callable(d) else d

    def __str__(self):
        s = ['<%s:%s,%s,default(%s),' % (self.__class__.__name__, self.name, self.ddl, self._default)]
        self.nullable and s.append('N')
        self.updatable and s.append('U')
        self.insertable and s.append('I')
        s.append('>')
        return ''.join(s)


class StringField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'varchar(255)'
        super(StringField, self).__init__(**kw)


class IntegerField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = 0
        if not 'ddl' in kw:
            kw['ddl'] = 'number(10)'
        super(IntegerField, self).__init__(**kw)


class FloatField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = 0.0
        if not 'ddl' in kw:
            kw['ddl'] = 'number'
        super(FloatField, self).__init__(**kw)


class BooleanField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = False
        if not 'ddl' in kw:
            kw['ddl'] = 'number(1)'
        super(BooleanField, self).__init__(**kw)


class TextField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'varchar2(4000)'
        super(TextField, self).__init__(**kw)


class BlobField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = ''
        if not 'ddl' in kw:
            kw['ddl'] = 'blob'
        super(BlobField, self).__init__(**kw)


class VersionField(Field):

    def __init__(self, name=None):
        super(VersionField, self).__init__(name=name, default=0, ddl='bigint')


_triggers = frozenset(['pre_insert', 'pre_update', 'pre_delete'])


def _gen_sql(table_name, mappings):
    pk = {}
    sql = ['-- generating SQL for %s:' % table_name, 'create table %s (' % table_name]
    for k, v in sorted(mappings.items(), lambda x, y: cmp(x[1]._order, y[1]._order)):
        if not hasattr(v, 'ddl'):
            raise StandardError('no ddl in field "%s".' % v.name)
        ddl = v.ddl
        nullable = v.nullable
        if v.primary_key:
            pk[k] = v
        sql.append(nullable and '  %s %s,' % (v.name, ddl) or '  %s %s not null,' % (v.name, ddl))
    # sql.append('  primary key(%s)' % pk)
    # sql = sql[:-1]
    sql.append(');')
    sql.append('alert table %s add constraint PK_%s primary key (%s);' % (table_name, table_name, pk))
    return '\n'.join(sql)

class ModelMetaclass(type):
    '''
    Metaclass for model objects.
    '''
    def __new__(cls, name, bases, attrs):
        # skip base Model class:
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)

        # store all subclasses info:
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        if not name in cls.subclasses:
            cls.subclasses[name] = name
        else:
            logging.warning('Redefine class: %s' % name)

        logging.info('Scan ORMapping %s...' % name)
        mappings = dict()
        primary_key = {}
        # primary_key_attr = None
        for k, v in attrs.iteritems():
            if isinstance(v, Field):
                if not v.name:
                    v.name = k.upper()
                else:
                    v.name = v.name.upper()
                logging.info('Found mapping: %s => %s' % (k, v))
                # check duplicate primary key:
                if v.primary_key:
                    # if primary_key:
                    #     raise TypeError('Cannot define more than 1 primary key in class: %s' % name)
                    if v.updatable:
                        logging.warning('NOTE: change primary key to non-updatable.')
                        v.updatable = False
                    if v.nullable:
                        logging.warning('NOTE: change primary key to non-nullable.')
                        v.nullable = False
                    primary_key[k] = v
                    # primary_key_attr = k
                mappings[k] = v
        # check exist of primary key:
        if not primary_key:
            raise TypeError('Primary key not defined in class: %s' % name)
        for k in mappings.iterkeys():
            attrs.pop(k)
        if not '__table__' in attrs:
            attrs['__table__'] = name.lower()
        attrs['__mappings__'] = mappings
        attrs['__primary_key__'] = primary_key
        # attrs['__primary_key_attr__'] = primary_key_attr
        attrs['__sql__'] = lambda self: _gen_sql(attrs['__table__'], mappings)
        for trigger in _triggers:
            if not trigger in attrs:
                attrs[trigger] = None
        return type.__new__(cls, name, bases, attrs)

class Model(dict):
    '''
    Base class for ORM.

    >>> class User(Model):
    ...     id = IntegerField(primary_key=True)
    ...     name = StringField()
    ...     email = StringField(updatable=False)
    ...     passwd = StringField(default=lambda: '******')
    ...     last_modified = FloatField()
    ...     def pre_insert(self):
    ...         self.last_modified = time.time()
    >>> u = User(id=10190, name='Michael', email='orm@db.org')
    >>> r = u.insert()
    >>> u.email
    'orm@db.org'
    >>> u.passwd
    '******'
    >>> u.last_modified > (time.time() - 2)
    True
    >>> f = User.get(10190)
    >>> f.name
    u'Michael'
    >>> f.email
    u'orm@db.org'
    >>> f.email = 'changed@db.org'
    >>> r = f.update() # change email but email is non-updatable!
    >>> len(User.find_all())
    1
    >>> g = User.get(10190)
    >>> g.email
    u'orm@db.org'
    >>> r = g.delete()
    >>> len(db.select('select * from user where id=10190'))
    0
    >>> import json
    >>> print User().__sql__()
    -- generating SQL for user:
    create table `user` (
      `id` bigint not null,
      `name` varchar(255) not null,
      `email` varchar(255) not null,
      `passwd` varchar(255) not null,
      `last_modified` real not null,
      primary key(`id`)
    );
    '''
    __metaclass__ = ModelMetaclass
    db = None

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)
        for k,v in self.__mappings__.items():
            if k not in kw:
                self[k] = v.default

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    @classmethod
    def get_dbfield_name(cls):
        a_fields = []
        # pks = []
        for k, v in sorted(cls.__mappings__.items(), lambda x, y: cmp(x[1]._order, y[1]._order)):
            a_fields.append(v.name)
            # if v.primary_key:
            #     pks.append('%s=:%s' % (v.name, k))
        return a_fields

    @classmethod
    def get_dbpk_name(cls):
        a_pks = []
        for k, v in sorted(cls.__primary_key__.items(), lambda x, y: cmp(x[1]._order, y[1]._order)):
            a_pks.append(v.name)
        return a_pks

    @classmethod
    def dump2db(cls, d_args):
        d = {}
        if not d_args:
            return d
        for k,v in d_args.items():
            d[cls.__mappings__[k].name] = v
        return d

    @classmethod
    def load_from_db(cls, d_args):
        dic = {}
        for k,v in cls.__mappings__.items():
            dic[k] = d_args.get(v.name, None)
        return cls(**dic) if dic else None

    @classmethod
    def get(cls, pk):
        '''
        Get by primary key.
        '''
        a_fields = cls.get_dbfield_name()
        a_pks = []
        for k, v in sorted(cls.__primary_key__.items(), lambda x, y: cmp(x[1]._order, y[1]._order)):
            a_pks.append('%s=:%s' % (v.name, k))
        sql = 'select %s from %s where %s' % (','.join(a_fields), cls.__table__, ' and '.join(a_pks))
        # sql = 'select * from %s where' % cls.__table__
        # pkfields = []
        # for k,v in cls.__primary_key__.items():
        #     pkfields.append('%s=:%s' % (v.name, k))
        #     if k not in pk:
        #         raise AttributeError(r"no pk field %s" % k)
        # sql = '%s %s' % (sql, ' and '.join(pkfields))

        d = cls.db.select_one(sql, pk)
        return cls.load_from_db(d)

    @classmethod
    def find_first(cls, where, d_args):
        '''
        Find by where clause and return one result. If multiple results found, 
        only the first one returned. If no result found, return None.
        '''
        a_fields = cls.get_dbfield_name()
        d = cls.db.select_one('select %s from %s %s' % (','.join(a_fields), cls.__table__, where), d_args)
        return cls.load_from_db(**d)

    @classmethod
    def find_all(cls, *args):
        '''
        Find all and return list.
        '''
        a_fields = cls.get_dbfield_name()
        L = cls.db.select('select %s from %s' % (','.join(a_fields), cls.__table__))
        return [cls.load_from_db(**d) for d in L]

    @classmethod
    def find_by(cls, where, d_args):
        '''
        Find by where clause and return list.
        '''
        a_fields = cls.get_dbfield_name()
        L = cls.db.select('select %s from %s %s' % (','.join(a_fields), cls.__table__, where), d_args)
        return [cls.load_from_db(**d) for d in L]

    @classmethod
    def count_all(cls):
        '''
        Find by 'select count(pk) from table' and return integer.
        '''
        return cls.db.select_int('select count(%s) from %s' % (cls.__primary_key__.name, cls.__table__))

    @classmethod
    def count_by(cls, where, d_args):
        '''
        Find by 'select count(pk) from table where ... ' and return int.
        '''
        return cls.db.select_int('select count(%s) from %s %s' % (cls.__primary_key__.name, cls.__table__, where), d_args)

    def update(self):
        self.pre_update and self.pre_update()
        L = []
        d_field = {}
        P = []
        for k, v in self.__mappings__.iteritems():
            if v.updatable:
                if hasattr(self, k):
                    d_field[v.name] = getattr(self, k)
                else:
                    d_field[v.name] = v.default
                    setattr(self, k, v.default)
                L.append('%s=:%s' % (v.name, v.name))
            if v.primary_key:
                d_field[v.name] = getattr(self, k)
                P.append('%s=:%s' % (v.name, v.name))

                # args.append(arg)
            # if v.primary_key:
            #     d_arg[v.name] = getattr(self, k)
        # pk = self.__primary_key__.name
        # d_arg[pk] = getattr(self, self.__primary_key_attr__)
        # args.append(getattr(self, pk))
        sql = 'update %s set %s where %s' % (self.__table__, ','.join(L), ' and '.join(P))
        self.db.update(sql, d_field)
        return self

    def delete(self):
        self.pre_delete and self.pre_delete()
        # pk = self.__primary_key__.name
        a_pk_name = []
        d_pk_value = {}
        for k, v in sorted(self.__primary_key__.items(), lambda x, y: cmp(x[1]._order, y[1]._order)):
            a_pk_name.append('%s=:%s' % (v.name, v.name))
            d_pk_value[v.name] = getattr(self, k)
        sql = 'delete from %s where %s' % (self.__table__, ' and '.join(a_pk_name))
        self.db.update(sql, d_pk_value)
        return self

    def insert(self):
        self.pre_insert and self.pre_insert()
        params = {}
        for k, v in self.__mappings__.iteritems():
            if v.insertable:
                if not hasattr(self, k):
                    setattr(self, k, v.default)
                params[v.name] = getattr(self, k)
        self.db.insert(self.__table__, params)
        return self

    def get_update_sql(self):
        self.pre_update and self.pre_update()
        L = []
        d_field = {}
        P = []
        for k, v in self.__mappings__.iteritems():
            if v.updatable:
                if hasattr(self, k):
                    d_field[v.name] = getattr(self, k)
                    L.append("%s=':%s'" % (v.name, v.name))
                # else:
                #     d_field[v.name] = v.default
                #     setattr(self, k, v.default)
                L.append("%s=':%s'" % (v.name, v.name))
            if v.primary_key:
                d_field[v.name] = getattr(self, k)
                P.append("%s=':%s'" % (v.name, v.name))
        sql = "update %s set %s where %s" % (self.__table__, ','.join(L), ' and '.join(P))
        for k,v in d_field:
            str = ':%s' % k
            sql = sql.replace(str, v)
        return sql

    def get_insert_sql(self):
        self.pre_insert and self.pre_insert()
        a_fields = []
        d_params = {}
        for k, v in self.__mappings__.iteritems():
            if v.insertable:
                if not hasattr(self, k):
                    setattr(self, k, v.default)
                a_fields.append(v.name)
                d_params[v.name] = getattr(self, k)
        sql = "insert into %s(%s) values(%s)" % (self.__table__, ",".join(a_fields), ",".join(["':%s'" % col for col in a_fields]))
        for k,v in d_params:
            str = ':%s' % k
            sql = sql.replace(str, v)
        return sql

    def get_delete_sql(self):
        self.pre_delete and self.pre_delete()
        a_pk_name = []
        d_pk_value = {}
        for k, v in sorted(self.__primary_key__.items(), lambda x, y: cmp(x[1]._order, y[1]._order)):
            a_pk_name.append("%s=':%s'" % (v.name, v.name))
            d_pk_value[v.name] = getattr(self, k)
        sql = 'delete from %s where %s' % (self.__table__, ' and '.join(a_pk_name))
        for k,v in d_pk_value:
            str = ':%s' % k
            sql = sql.replace(str, v)
        return sql


class Tmp_ps(Model):
    ps_id = IntegerField(ddl='NUMBER(15)', primary_key=True)
    bill_id = StringField(ddl='VARCHAR2(64)')
    ps_param = StringField(ddl='VARCHAR2(4000)')

if __name__=='__main__':
    logging.basicConfig(level=logging.DEBUG)
    # oradb.create_engine('www-data', 'www-data', 'test')
    # oradb.update('drop table if exists user')
    # oradb.update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
    # import doctest
    # doctest.testmod()

    dbConf = {'user': 'kt4', 'password': 'kt4', 'host': '10.7.5.164', 'port': 1521, 'sid': 'ngtst02', 'service_name': ''}
    ktdb = oradb.Db(dbConf)
    Tmp_ps.db = ktdb

    sql = 'select sysdate from dual'

    with ktdb:
        sysdate = ktdb.select_one('select sysdate from dual')
        logging.info(sysdate)

        pk = {'ps_id': 103258}
        tmp_ps = Tmp_ps.get(pk)
        logging.info('type: %s' % type(tmp_ps))
        print(tmp_ps)
        print('ps_id: %d, bill_id: %s, ps_param: %s' % (tmp_ps.ps_id, tmp_ps.bill_id, tmp_ps.ps_param))
        tmp_ps.ps_param += ' test;'
        tmp_ps.update()
        tmp_ps.ps_id -= 1
        tmp_ps.insert()

        # time.sleep(1)
        # with _CursorCtx(sql, ktdb.db_ctx) as curCtx:
        #     sysdate1 = curCtx.select_one(None)
        #     print('date1: %s' % sysdate1)
        #     sysdate2 = curCtx.select_one(None)
        #     print('date2: %s' % sysdate2)
        #
        # time.sleep(2)
        # with ktdb.open_cursor(sql) as curCtx:
        #     sysdate3 = curCtx.select_one(None)
        #     print('date3: %s' % sysdate3)
        #     sysdate4 = curCtx.select_one(None)
        #     print('date4: %s' % sysdate4)
        #
        # time.sleep(1)
        # with ktdb.open_curgrp() as curGrp:
        #     curGrp.get_cur('sysdate', sql)
        #     sysdate5 = curGrp.select_one('sysdate')
        #     print('date5: %s' % sysdate5)
        #     sysdate6 = curGrp.select_one('sysdate')
        #     print('date6: %s' % sysdate6)

