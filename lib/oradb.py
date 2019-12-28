#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Michael Liao'

'''
Database operation module.
'''

import time, uuid, functools, threading, logging

# Dict object:


class Dict(dict):
    '''
    Simple dict but support access as x.y style.

    >>> d1 = Dict()
    >>> d1['x'] = 100
    >>> d1.x
    100
    >>> d1.y = 200
    >>> d1['y']
    200
    >>> d2 = Dict(a=1, b=2, c='3')
    >>> d2.c
    '3'
    >>> d2['empty']
    Traceback (most recent call last):
        ...
    KeyError: 'empty'
    >>> d2.empty
    Traceback (most recent call last):
        ...
    AttributeError: 'Dict' object has no attribute 'empty'
    >>> d3 = Dict(('a', 'b', 'c'), (1, 2, 3))
    >>> d3.a
    1
    >>> d3.b
    2
    >>> d3.c
    3
    '''
    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value


def next_id(t=None):
    '''
    Return next id as 50-char string.

    Args:
        t: unix timestamp, default to None and using time.time().
    '''
    if t is None:
        t = time.time()
    return '%015d%s000' % (int(t * 1000), uuid.uuid4().hex)


def _profiling(start, sql=''):
    t = time.time() - start
    if t > 0.1:
        logging.warning('[PROFILING] [DB] %s: %s' % (t, sql))
    else:
        logging.info('[PROFILING] [DB] %s: %s' % (t, sql))


class DBError(Exception):
    pass


class MultiColumnsError(DBError):
    pass


class _LasyConnection(object):

    def __init__(self, eng=engine):
        self.connection = None
        self.engine = eng
        # if not eng:
        #     self.engine = engine

    def cursor(self):
        if self.connection is None:
            connection = self.engine.connect()
            logging.info('open connection <%s>...' % hex(id(connection)))
            self.connection = connection
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            connection = self.connection
            self.connection = None
            logging.info('close connection <%s>.' % hex(id(connection)))
            connection.close()


class _DbCtx(threading.local):
    '''
    Thread local object that holds connection info.
    '''
    def __init__(self, eng=engine):
        self.connection = None
        self.transactions = 0
        self.engine = eng
        # if not eng:
        #     self.engine = engine

    def is_init(self):
        return not self.connection is None

    def init(self):
        logging.info('open lazy connection...')
        self.connection = _LasyConnection(self.engine)
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        '''
        Return cursor
        '''
        return self.connection.cursor()


# thread-local db context:
_db_ctx = _DbCtx()

# global engine object:
engine = None


class _Engine(object):

    def __init__(self, connect):
        self._connect = connect

    def connect(self):
        return self._connect()


def create_engine(dbconf, user, password, database, host='127.0.0.1', port=1521, **kw):
    import cx_Oracle as orcl
    # global engine
    # if engine is not None:
    #     raise DBError('Engine is already initialized.')
    host, port, sid, service_name = dbconf.pop('host'), dbconf.pop('port'), dbconf.get('sid', None), dbconf.get(
        'service_name', None)
    if 'service_name' in dbConf: dbConf.pop('service_name')
    if 'sid' in dbConf: dbConf.pop('sid')
    dsn = None
    if sid:
        dsn = orcl.makedsn(host, port, sid=sid)
    elif service_name:
        dsn = orcl.makedsn(host, port, service_name=dbconf.get('service_name'))
    dbconf['dsn'] = dsn
    engine = _Engine(lambda: orcl.connect(**dbconf))
    logging.info('Init oracle engine <%s> ok.' % hex(id(engine)))
    return engine


class _ConnectionCtx(object):
    '''
    _ConnectionCtx object that can open and close connection context. _ConnectionCtx object can be nested and only the most 
    outer connection has effect.

    with connection():
        pass
        with connection():
            pass
    '''
    def __init__(self, db_ctx=None):
        global _db_ctx
        self.db_ctx = db_ctx
        if not db_ctx:
            self.db_ctx = _db_ctx

    def __enter__(self):
        # global _db_ctx
        self.should_cleanup = False
        if not self.db_ctx.is_init():
            self.db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exctype, excvalue, traceback):
        # global _db_ctx
        if self.should_cleanup:
            self.db_ctx.cleanup()


class _CursorCtx(object):
    '''
    _ConnectionCtx object that can open and close connection context. _ConnectionCtx object can be nested and only the most
    outer connection has effect.

    with connection():
        pass
        with connection():
            pass
    '''
    def __init__(self, sql, db_ctx=None):
        global _db_ctx
        self.sql = sql
        self.cur = None
        self.db_ctx = db_ctx
        if not db_ctx:
            self.db_ctx = _db_ctx
        # if not self.db_ctx.is_init():
        #     raise DBError('db_ctx is not initialized.')

    def __enter__(self):
        logging.info('enter cursor_ctx <%s>...', hex(id(self)))
        self.db_ctx_cleanup = False
        self.cur_cleanup = False
        if not self.db_ctx.is_init():
            self.db_ctx.init()
            self.db_ctx_cleanup = True
        if not self.cur:
            self.cur = self.db_ctx.connection.cursor()
            self.cur.prepare(self.sql)
            self.cur_cleanup = True
        return self

    def __exit__(self, exctype, excvalue, traceback):
        logging.info('exit cursor_ctx <%s>.', hex(id(self)))
        if self.cur_cleanup:
            self.cur.close()
            self.cur = None
        if self.db_ctx_cleanup:
            self.db_ctx.cleanup()

    def _select(self, first, d_arg):
        logging.info('SQL: %s, ARGS: %s' % (self.sql, d_arg))
        try:
            if d_arg:
                self.cur.execute(None, d_arg)
            else:
                self.cur.execute(None)
            if self.cur.description:
                names = [x[0] for x in self.cur.description]
            if first:
                values = self.cur.fetchone()
                if not values:
                    return None
                return Dict(names, values)
            return [Dict(names, x) for x in self.cur.fetchall()]
        finally:
            # if self.cur:
            #     self.cur.close()
            logging.debug('select ok.')

    def select_one(self, d_arg=None):
        '''
        Execute select SQL and expected one result.
        If no result found, return None.
        If multiple results found, the first one returned.
        '''
        return self._select(True, d_arg)

    def select_int(self, d_arg=None):
        '''
        Execute select SQL and expected one int and only one int result.
        '''
        d = self._select(True, d_arg)
        if len(d) != 1:
            raise MultiColumnsError('Expect only one column.')
        return d.values()[0]

    def select(self, d_arg=None):
        '''
        Execute select SQL and return list or empty list if no result.
        '''
        return self._select(False, d_arg)

    def _update(self, d_arg):
        # sql = sql.replace('?', '%s')
        logging.info('SQL: %s, ARGS: %s' % (self.sql, d_arg))
        try:
            if d_arg:
                self.cur.execute(None, d_arg)
            else:
                self.cur.execute(None)
            r = self.cur.rowcount
            if self.db_ctx.transactions == 0:
                # no transaction enviroment:
                logging.info('auto commit')
                self.db_ctx.connection.commit()
            return r
        finally:
            logging.debug('execute sql ok.')

    def insert(self, d_arg=None):
        '''
        Execute insert SQL.
        Traceback (most recent call last):
          ...
        IntegrityError: 1062 (23000): Duplicate entry '2000' for key 'PRIMARY'
        '''
        return self._update(d_arg)

    def update(self, d_arg=None):
        r'''
        Execute update or delete SQL.
        '''
        return self._update(d_arg)


class _CursorGrpCtx(object):
    '''
    _ConnectionCtx object that can open and close connection context. _ConnectionCtx object can be nested and only the most
    outer connection has effect.

    with connection():
        pass
        with connection():
            pass
    '''
    def __init__(self, d_sql=None, db_ctx=None):
        global _db_ctx
        self.db_ctx = db_ctx
        self.dSql = d_sql
        self.dCur = None
        if not db_ctx:
            self.db_ctx = _db_ctx
        if d_sql:
            for k, v in d_sql.items():
                self.get_cur(k, v)
        # if not self.db_ctx.is_init():
        #     raise DBError('db_ctx is not initialized.')

    def __enter__(self):
        logging.info('enter cursorGrp_ctx <%s>...', hex(id(self)))
        self.db_ctx_cleanup = False
        self.cur_cleanup = False
        if not self.db_ctx.is_init():
            self.db_ctx.init()
            self.db_ctx_cleanup = True
        if not self.dCur:
            # self.cur = self.db_ctx.connection.cursor()
            # self.cur.prepare(self.sql)
            # self.dSql = {}
            self.dCur = {}
            self.cur_cleanup = True
        return self

    def get_cur(self, cur_name, cur_sql):
        if cur_name not in self.dCur:
            logging.info('open cursor %s...', cur_name)
            self.dSql[cur_name] = cur_sql
            self.dCur[cur_name] = self.db_ctx.connection.cursor()
            self.dCur[cur_name].prepare(cur_sql)
            logging.info('opened cursor %s: <%s>', cur_name, hex(id(self.dCur[cur_name])))
        return self.dCur[cur_name]

    def __exit__(self, exctype, excvalue, traceback):
        logging.info('exit cursorGrp_ctx <%s>.', hex(id(self)))
        if self.cur_cleanup:
            for sn, cur in self.dCur.items():
                logging.info('close cursor %s: <%s>', sn, hex(id(cur)))
                cur.close()
                self.dSql.pop(sn)
            self.dCur.clear()
            self.dCur = None
            self.dSql = None
        if self.db_ctx_cleanup:
            self.db_ctx.cleanup()

    def _select(self, sql_name, sql, first, d_arg):
        logging.info('SQL: %s, ARGS: %s' % (sql_name, d_arg))
        try:
            if sql_name not in self.dCur:
                self.get_cur(sql_name, sql)
            cur = self.dCur[sql_name]
            if d_arg:
                cur.execute(None, d_arg)
            else:
                cur.execute(None)
            if cur.description:
                names = [x[0] for x in cur.description]
            if first:
                values = cur.fetchone()
                if not values:
                    return None
                return Dict(names, values)
            return [Dict(names, x) for x in cur.fetchall()]
        finally:
            # if self.cur:
            #     self.cur.close()
            logging.debug('select %s ok.', sql_name)

    def select_one(self, sql_name, sql=None, d_arg=None):
        '''
        Execute select SQL and expected one result.
        If no result found, return None.
        If multiple results found, the first one returned.
        '''
        return self._select(sql_name, sql, True, d_arg)

    def select_int(self, sql_name, sql=None, d_arg=None):
        '''
        Execute select SQL and expected one int and only one int result.
        '''
        d = self._select(sql_name, sql, True, d_arg)
        if len(d) != 1:
            raise MultiColumnsError('Expect only one column.')
        return d.values()[0]

    def select(self, sql_name, sql=None,  d_arg=None):
        '''
        Execute select SQL and return list or empty list if no result.
        '''
        return self._select(sql_name, sql, False, d_arg)

    def _update(self, sql_name, sql, d_arg):
        # sql = sql.replace('?', '%s')
        logging.info('SQL: %s, ARGS: %s' % (sql_name, d_arg))
        try:
            if sql_name not in self.dCur:
                self.get_cur(sql_name, sql)
            cur = self.dCur[sql_name]
            if d_arg:
                cur.execute(None, d_arg)
            else:
                cur.execute(None)
            r = cur.rowcount
            if self.db_ctx.transactions == 0:
                # no transaction enviroment:
                logging.info('auto commit')
                self.db_ctx.connection.commit()
            return r
        finally:
            logging.debug('execute %s ok.', sql_name)

    def insert(self, sql_name, sql=None, d_arg=None):
        '''
        Execute insert SQL.
        Traceback (most recent call last):
          ...
        IntegrityError: 1062 (23000): Duplicate entry '2000' for key 'PRIMARY'
        '''
        return self._update(sql_name, sql, d_arg)

    def update(self, sql_name, sql=None, d_arg=None):
        r'''
        Execute update or delete SQL.
        '''
        return self._update(sql_name, sql, d_arg)



def connection(db_ctx=None):
    '''
    Return _ConnectionCtx object that can be used by 'with' statement:

    with connection():
        pass
    '''
    return _ConnectionCtx(db_ctx)


def with_connection(func, db_ctx=None):
    '''
    Decorator for reuse connection.

    @with_connection
    def foo(*args, **kw):
        f1()
        f2()
        f3()
    '''
    db = db_ctx
    if not db_ctx:
        db = _db_ctx
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _ConnectionCtx(db):
            return func(*args, **kw)
    return _wrapper


class _TransactionCtx(object):
    '''
    _TransactionCtx object that can handle transactions.

    with _TransactionCtx():
        pass
    '''

    def __init__(self, db_ctx=None):
        global _db_ctx
        self.db_ctx = db_ctx
        if not db_ctx:
            self.db_ctx = _db_ctx

    def __enter__(self):
        # global _db_ctx
        self.should_close_conn = False
        if not self.db_ctx.is_init():
            # needs open a connection first:
            self.db_ctx.init()
            self.should_close_conn = True
        self.db_ctx.transactions = self.db_ctx.transactions + 1
        logging.info('begin transaction...' if self.db_ctx.transactions==1 else 'join current transaction...')
        return self

    def __exit__(self, exctype, excvalue, traceback):
        # global _db_ctx
        self.db_ctx.transactions = self.db_ctx.transactions - 1
        try:
            if self.db_ctx.transactions==0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                self.db_ctx.cleanup()

    def commit(self):
        # global _db_ctx
        logging.info('commit transaction...')
        try:
            self.db_ctx.connection.commit()
            logging.info('commit ok.')
        except:
            logging.warning('commit failed. try rollback...')
            self.db_ctx.connection.rollback()
            logging.warning('rollback ok.')
            raise

    def rollback(self):
        # global _db_ctx
        logging.warning('rollback transaction...')
        self.db_ctx.connection.rollback()
        logging.info('rollback ok.')


def transaction(db_ctx=None):
    '''
    Create a transaction object so can use with statement:

    with transaction():
        pass

    >>> def update_profile(id, name, rollback):
    ...     u = dict(id=id, name=name, email='%s@test.org' % name, passwd=name, last_modified=time.time())
    ...     insert('user', **u)
    ...     r = update('update user set passwd=? where id=?', name.upper(), id)
    ...     if rollback:
    ...         raise StandardError('will cause rollback...')
    >>> with transaction():
    ...     update_profile(900301, 'Python', False)
    >>> select_one('select * from user where id=?', 900301).name
    u'Python'
    >>> with transaction():
    ...     update_profile(900302, 'Ruby', True)
    Traceback (most recent call last):
      ...
    StandardError: will cause rollback...
    >>> select('select * from user where id=?', 900302)
    []
    '''
    return _TransactionCtx(db_ctx)


def with_transaction(func):
    '''
    A decorator that makes function around transaction.

    >>> @with_transaction
    ... def update_profile(id, name, rollback):
    ...     u = dict(id=id, name=name, email='%s@test.org' % name, passwd=name, last_modified=time.time())
    ...     insert('user', **u)
    ...     r = update('update user set passwd=? where id=?', name.upper(), id)
    ...     if rollback:
    ...         raise StandardError('will cause rollback...')
    >>> update_profile(8080, 'Julia', False)
    >>> select_one('select * from user where id=?', 8080).passwd
    u'JULIA'
    >>> update_profile(9090, 'Robert', True)
    Traceback (most recent call last):
      ...
    StandardError: will cause rollback...
    >>> select('select * from user where id=?', 9090)
    []
    '''
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        _start = time.time()
        with _TransactionCtx():
            return func(*args, **kw)
        _profiling(_start)
    return _wrapper


def _select(sql, first, d_arg):
    ' execute select SQL and return unique result or list results.'
    global _db_ctx
    cursor = None
    # sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, d_arg))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, d_arg)
        if cursor.description:
            names = [x[0] for x in cursor.description]
        if first:
            values = cursor.fetchone()
            if not values:
                return None
            return Dict(names, values)
        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()


@with_connection
def select_one(sql, d_arg):
    '''
    Execute select SQL and expected one result. 
    If no result found, return None.
    If multiple results found, the first one returned.

    >>> u1 = dict(id=100, name='Alice', email='alice@test.org', passwd='ABC-12345', last_modified=time.time())
    >>> u2 = dict(id=101, name='Sarah', email='sarah@test.org', passwd='ABC-12345', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> insert('user', **u2)
    1
    >>> u = select_one('select * from user where id=?', 100)
    >>> u.name
    u'Alice'
    >>> select_one('select * from user where email=?', 'abc@email.com')
    >>> u2 = select_one('select * from user where passwd=? order by email', 'ABC-12345')
    >>> u2.name
    u'Alice'
    '''
    return _select(sql, True, d_arg)


@with_connection
def select_int(sql, d_arg):
    '''
    Execute select SQL and expected one int and only one int result. 

    >>> n = update('delete from user')
    >>> u1 = dict(id=96900, name='Ada', email='ada@test.org', passwd='A-12345', last_modified=time.time())
    >>> u2 = dict(id=96901, name='Adam', email='adam@test.org', passwd='A-12345', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> insert('user', **u2)
    1
    >>> select_int('select count(*) from user')
    2
    >>> select_int('select count(*) from user where email=?', 'ada@test.org')
    1
    >>> select_int('select count(*) from user where email=?', 'notexist@test.org')
    0
    >>> select_int('select id from user where email=?', 'ada@test.org')
    96900
    >>> select_int('select id, name from user where email=?', 'ada@test.org')
    Traceback (most recent call last):
        ...
    MultiColumnsError: Expect only one column.
    '''
    d = _select(sql, True, d_arg)
    if len(d)!=1:
        raise MultiColumnsError('Expect only one column.')
    return d.values()[0]


@with_connection
def select(sql, d_arg):
    '''
    Execute select SQL and return list or empty list if no result.

    >>> u1 = dict(id=200, name='Wall.E', email='wall.e@test.org', passwd='back-to-earth', last_modified=time.time())
    >>> u2 = dict(id=201, name='Eva', email='eva@test.org', passwd='back-to-earth', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> insert('user', **u2)
    1
    >>> L = select('select * from user where id=?', 900900900)
    >>> L
    []
    >>> L = select('select * from user where id=?', 200)
    >>> L[0].email
    u'wall.e@test.org'
    >>> L = select('select * from user where passwd=? order by id desc', 'back-to-earth')
    >>> L[0].name
    u'Eva'
    >>> L[1].name
    u'Wall.E'
    '''
    return _select(sql, False, d_arg)


@with_connection
def _update(sql, d_arg):
    global _db_ctx
    cursor = None
    # sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, d_arg))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, d_arg)
        r = cursor.rowcount
        if _db_ctx.transactions==0:
            # no transaction enviroment:
            logging.info('auto commit')
            _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()


def insert(table, d_arg):
    '''
    Execute insert SQL.

    >>> u1 = dict(id=2000, name='Bob', email='bob@test.org', passwd='bobobob', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> u2 = select_one('select * from user where id=?', 2000)
    >>> u2.name
    u'Bob'
    >>> insert('user', **u2)
    Traceback (most recent call last):
      ...
    IntegrityError: 1062 (23000): Duplicate entry '2000' for key 'PRIMARY'
    '''
    cols = d_arg.keys()
    sql = 'insert into %s(%s) values(%s)' % (table, ','.join(cols), ','.join([':%s' % col for col in cols]))
    return _update(sql, d_arg)


def update(sql, d_arg):
    r'''
    Execute update SQL.

    >>> u1 = dict(id=1000, name='Michael', email='michael@test.org', passwd='123456', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> u2 = select_one('select * from user where id=?', 1000)
    >>> u2.email
    u'michael@test.org'
    >>> u2.passwd
    u'123456'
    >>> update('update user set email=?, passwd=? where id=?', 'michael@example.org', '654321', 1000)
    1
    >>> u3 = select_one('select * from user where id=?', 1000)
    >>> u3.email
    u'michael@example.org'
    >>> u3.passwd
    u'654321'
    >>> update('update user set passwd=? where id=?', '***', '123\' or id=\'456')
    0
    '''
    return _update(sql, d_arg)


class Db(object):
    '''db interface for all db process
    dbconf = {
                    'user': 'user_name_str',
                    'passwd': 'passwd_str',
                    'host': 'xxx.xxx.xxx.xxx_str',
                    'port': port_int,
                    'sid': 'sid_str',
                    'service_name': 'service_name_str'}
    '''
    #user, password, database, host='127.0.0.1', port=3306, **kw)
    def __init__(self, dbconf):
        import cx_Oracle as orcl
        host, port, sid, service_name = dbconf.pop('host'), dbconf.pop('port'), dbconf.get('sid',None), dbconf.get('service_name',None)
        if 'service_name' in dbconf: dbconf.pop('service_name')
        if 'sid' in dbconf: dbconf.pop('sid')
        dsn = None
        if sid:
            dsn = orcl.makedsn(host, port, sid=sid)
        elif service_name:
            dsn = orcl.makedsn(host, port, service_name=dbconf.get('service_name'))
        dbconf['dsn'] = dsn
        eng = _Engine(lambda: orcl.connect(**dbconf))
        logging.info('Init oracle engine <%s> ok.' % hex(id(eng)))
        self.db_ctx = _DbCtx(eng)

    def __enter__(self):
        # global _db_ctx
        self.should_cleanup = False
        if not self.db_ctx.is_init():
            self.db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exctype, excvalue, traceback):
        # global _db_ctx
        if self.should_cleanup:
            self.db_ctx.cleanup()

    def connection(self):
        '''
        Return _ConnectionCtx object that can be used by 'with' statement:

        with connection():
            pass
        '''
        return _ConnectionCtx(self.db_ctx)

    def transaction(self):
        '''
        Create a transaction object so can use with statement:

        with transaction():
            pass

        >>> def update_profile(id, name, rollback):
        ...     u = dict(id=id, name=name, email='%s@test.org' % name, passwd=name, last_modified=time.time())
        ...     insert('user', **u)
        ...     r = update('update user set passwd=? where id=?', name.upper(), id)
        ...     if rollback:
        ...         raise StandardError('will cause rollback...')
        >>> with transaction():
        ...     update_profile(900301, 'Python', False)
        >>> select_one('select * from user where id=?', 900301).name
        u'Python'
        >>> with transaction():
        ...     update_profile(900302, 'Ruby', True)
        Traceback (most recent call last):
          ...
        StandardError: will cause rollback...
        >>> select('select * from user where id=?', 900302)
        []
        '''
        return _TransactionCtx(self.db_ctx)

    def _select(self, sql, first, d_arg):
        ' execute select SQL and return unique result or list results.'
        cursor = None
        logging.info('SQL: %s, ARGS: %s' % (sql, d_arg))
        try:
            cursor = self.db_ctx.connection.cursor()
            if d_arg:
                cursor.execute(sql, d_arg)
            else:
                cursor.execute(sql)
            if cursor.description:
                names = [x[0] for x in cursor.description]
            if first:
                values = cursor.fetchone()
                if not values:
                    return None
                return Dict(names, values)
            return [Dict(names, x) for x in cursor.fetchall()]
        finally:
            if cursor:
                cursor.close()

    def select_one(self, sql, d_arg=None):
        '''
        Execute select SQL and expected one result.
        If no result found, return None.
        If multiple results found, the first one returned.

        >>> u1 = dict(id=100, name='Alice', email='alice@test.org', passwd='ABC-12345', last_modified=time.time())
        >>> u2 = dict(id=101, name='Sarah', email='sarah@test.org', passwd='ABC-12345', last_modified=time.time())
        >>> insert('user', **u1)
        1
        >>> insert('user', **u2)
        1
        >>> u = select_one('select * from user where id=?', 100)
        >>> u.name
        u'Alice'
        >>> select_one('select * from user where email=?', 'abc@email.com')
        >>> u2 = select_one('select * from user where passwd=? order by email', 'ABC-12345')
        >>> u2.name
        u'Alice'
        '''
        with _ConnectionCtx(self.db_ctx):
            return self._select(sql, True, d_arg)

    def select_int(self, sql, d_arg=None):
        '''
        Execute select SQL and expected one int and only one int result.

        >>> n = update('delete from user')
        >>> u1 = dict(id=96900, name='Ada', email='ada@test.org', passwd='A-12345', last_modified=time.time())
        >>> u2 = dict(id=96901, name='Adam', email='adam@test.org', passwd='A-12345', last_modified=time.time())
        >>> insert('user', **u1)
        1
        >>> insert('user', **u2)
        1
        >>> select_int('select count(*) from user')
        2
        >>> select_int('select count(*) from user where email=?', 'ada@test.org')
        1
        >>> select_int('select count(*) from user where email=?', 'notexist@test.org')
        0
        >>> select_int('select id from user where email=?', 'ada@test.org')
        96900
        >>> select_int('select id, name from user where email=?', 'ada@test.org')
        Traceback (most recent call last):
            ...
        MultiColumnsError: Expect only one column.
        '''
        with _ConnectionCtx(self.db_ctx):
            d = self._select(sql, True, d_arg)
        if len(d) != 1:
            raise MultiColumnsError('Expect only one column.')
        return d.values()[0]

    def select(self, sql, d_arg=None):
        '''
        Execute select SQL and return list or empty list if no result.

        >>> u1 = dict(id=200, name='Wall.E', email='wall.e@test.org', passwd='back-to-earth', last_modified=time.time())
        >>> u2 = dict(id=201, name='Eva', email='eva@test.org', passwd='back-to-earth', last_modified=time.time())
        >>> insert('user', **u1)
        1
        >>> insert('user', **u2)
        1
        >>> L = select('select * from user where id=?', 900900900)
        >>> L
        []
        >>> L = select('select * from user where id=?', 200)
        >>> L[0].email
        u'wall.e@test.org'
        >>> L = select('select * from user where passwd=? order by id desc', 'back-to-earth')
        >>> L[0].name
        u'Eva'
        >>> L[1].name
        u'Wall.E'
        '''
        with _ConnectionCtx(self.db_ctx):
            return self._select(sql, False, d_arg)

    def _update(self, sql, d_arg):
        cursor = None
        # sql = sql.replace('?', '%s')
        logging.info('SQL: %s, ARGS: %s' % (sql, d_arg))
        with _ConnectionCtx(self.db_ctx):
            try:
                cursor = self.db_ctx.connection.cursor()
                if d_arg:
                    cursor.execute(sql, d_arg)
                else:
                    cursor.execute(sql)
                r = cursor.rowcount
                if self.db_ctx.transactions == 0:
                    # no transaction enviroment:
                    logging.info('auto commit')
                    self.db_ctx.connection.commit()
                return r
            finally:
                if cursor:
                    cursor.close()

    def insert(self, table, d_arg=None):
        '''
        Execute insert SQL.

        >>> u1 = dict(id=2000, name='Bob', email='bob@test.org', passwd='bobobob', last_modified=time.time())
        >>> insert('user', **u1)
        1
        >>> u2 = select_one('select * from user where id=?', 2000)
        >>> u2.name
        u'Bob'
        >>> insert('user', **u2)
        Traceback (most recent call last):
          ...
        IntegrityError: 1062 (23000): Duplicate entry '2000' for key 'PRIMARY'
        '''
        cols = d_arg.keys()
        sql = 'insert into %s(%s) values(%s)' % (table, ','.join(cols), ','.join([':%s' % col for col in cols]))
        return self._update(sql, d_arg)

    def update(self, sql, d_arg=None):
        r'''
        Execute update SQL.

        >>> u1 = dict(id=1000, name='Michael', email='michael@test.org', passwd='123456', last_modified=time.time())
        >>> insert('user', **u1)
        1
        >>> u2 = select_one('select * from user where id=?', 1000)
        >>> u2.email
        u'michael@test.org'
        >>> u2.passwd
        u'123456'
        >>> update('update user set email=?, passwd=? where id=?', 'michael@example.org', '654321', 1000)
        1
        >>> u3 = select_one('select * from user where id=?', 1000)
        >>> u3.email
        u'michael@example.org'
        >>> u3.passwd
        u'654321'
        >>> update('update user set passwd=? where id=?', '***', '123\' or id=\'456')
        0
        '''
        return self._update(sql, d_arg)

    def open_cursor(self, sql):
        return _CursorCtx(sql, self.db_ctx)

    def open_curgrp(self, d_sql=None):
        return _CursorGrpCtx(d_sql, self.db_ctx)


if __name__=='__main__':
    logging.basicConfig(level=logging.DEBUG)
    # engine = create_engine('www-data', 'www-data', 'test')
    # update('drop table if exists user')
    # update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
    # import doctest
    # doctest.testmod()
    # , 'service_name':None
    dbConf = {'user':'zg', 'password':'ngboss4,123', 'host':'10.7.5.132', 'port':1521, 'sid':'ng1tst01'}
    ktdb = Db(dbConf)
    sql = 'select sysdate from dual'
    with connection(ktdb.db_ctx):
        sysdate = ktdb.select_one('select sysdate from dual')
        print(sysdate)

        time.sleep(1)
        with _CursorCtx(sql, ktdb.db_ctx) as curCtx:
            sysdate1 = curCtx.select_one(None)
            print('date1: %s' % sysdate1)
            sysdate2 = curCtx.select_one(None)
            print('date2: %s' % sysdate2)

        time.sleep(2)
        with ktdb.open_cursor(sql) as curCtx:
            sysdate3 = curCtx.select_one(None)
            print('date3: %s' % sysdate3)
            sysdate4 = curCtx.select_one(None)
            print('date4: %s' % sysdate4)

        time.sleep(1)
        with ktdb.open_curgrp() as curGrp:
            curGrp.get_cur('sysdate', sql)
            sysdate5 = curGrp.select_one('sysdate')
            print('date5: %s' % sysdate5)
            sysdate6 = curGrp.select_one('sysdate')
            print('date6: %s' % sysdate6)
