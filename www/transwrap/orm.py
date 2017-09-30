#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: ‘wujn‘
@file: orm.py
@time: 2017/9/28 11:23
"""
import db
import time
import logging

_triggers = frozenset(['pre_insert', 'pre_update', 'pre_delete'])


def _gen_sql(table_name, mappings):
    """
    类 ==> 表时 生成创建表的sql
    """
    p_key = None
    sql = ['create table `%s` (' % table_name]
    for f in sorted(mappings.values(), lambda x, y: cmp(x._order, y._order)):
        if not hasattr(f, 'ddl'):
            raise StandardError('no ddl in field "%s".' % f)
        ddl = f.ddl
        nullable = f.nullable
        if f.primary_key:
            p_key = f.name
        sql.append('  `%s` %s,' % (f.name, ddl) if nullable else '  `%s` %s not null,' % (f.name, ddl))
    sql.append(' primary key(`%s`)' % p_key)
    sql.append(');')
    return '\n'.join(sql)



class Field(object):
    """
    保存数据库表的字段名和字段类型
    _count：Field每实例化一次就加一
    self._order：表示是该类的第几个实例
        这样，在定义字段时，每个字段（field实例）都有order属性
        最后生成__sql时（见_gen_sql 函数），这些字段就是按序排列
    self._default: 用于让orm自己填入缺省值，缺省值可以是 可调用对象，比如函数
    其他实例的属性都用来描述字段属性，例如名字、是否主键、能否为空等
    """
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
        Field._count += 1

    def __str__(self):
        """
        :return:实例对象的描述信息
        如：
            <IntegerField:id,bigint,default(0),UI>
        分别对应 <类：实例：实例ddl属性：实例default信息，3种标志位：N U I>
        """
        s = ['<%s:%s,%s,default(%s),' % (self.__class__, self.name, self.ddl, self._default)]
        self.nullable and s.append('N')
        self.updatable and s.append('U')
        self.insertable and s.append('I')
        s.append('>')
        return ''.join(s)

    @property
    def default(self):
        """
        使用property装饰器快速访问缺省的对象
        :return: 如果可以执行就返回default()，否则返回default的值
        """
        d = self._default
        return d() if callable(d) else d


class StringField(Field):
    """
    保存string类型字段的类
    """

    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = ''
        if 'ddl' not in kw:
            kw['ddl'] = 'varchar(255)'
        super(StringField, self).__init__(**kw)


class IntegerField(Field):
    """
    保存int类型字段的值
    """

    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = 0
        if 'ddl' not in kw:
            kw['ddl'] = 'bigint'
        super(IntegerField, self).__init__(**kw)


class FloatField(Field):
    """
    保存float类型字段的值
    """

    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = 0.0
        if 'ddl' not in kw:
            kw['ddl'] = 'real'
        super(FloatField, self).__init__(**kw)


class BoolenField(Field):
    """
    保存布尔类型字段的值
    """

    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = False
        if 'ddl' not in kw:
            kw['ddl'] = 'bool'
        super(BoolenField, self).__init__(**kw)


class TextField(Field):
    """
    保存Text类型字段的属性
    """

    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = ''
        if 'ddl' not in kw:
            kw['ddl'] = 'text'
        super(TextField, self).__init__(**kw)


class BlobField(Field):
    """
    保存Blob类型字段的属性
    """

    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = ''
        if 'ddl' not in kw:
            kw['ddl'] = 'blob'
        super(BlobField, self).__init__(**kw)


class VersionField(Field):
    """
    保存Version类型字段的属性
    """

    def __init__(self, name=None):
        super(VersionField, self).__init__(name=name, default=0, ddl='bigint')


class ModelMetaClass(type):
    """
    是一个元类，主要有以下作用：
    1、防止对Module类的修改
    2、属性与字段的mapping
        迭代类的属性字典，判断是不是Field类，添加name，进行标志位的检查，提取类属性和字段类的mapping
        提取完成后删除这些类属性,避免冲突
        添加__mappings__属性
    3、类与表的mapping
        添加__table__属性，即表名(为类名的小写)
    """

    def __new__(cls, name, bases, attrs):
        if name == 'Module':
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
        primary_key = None

        for k, v in attrs.iteritems():
            if isinstance(v, Field):
                if not v.name:
                    v.name = k

                if v.primary_key:
                    if primary_key:
                        raise TypeError('Cannot define more than 1 primary key in class: %s' % name)
                    if v.updatable:
                        logging.warning('NOTE: change primary key to non-updatable.')
                        v.updatable = False
                    if v.nullable:
                        logging.warning('NOTE: change primary key to non-nullable.')    # pk默认不能为空
                        v.nullable = False
                    primary_key = v
                mappings[k] = v

        if not primary_key:
            raise TypeError('Primary key not defined in class: %s' % name)
        for k in mappings.iterkeys():
            attrs.pop(k)
        if not '__table__' in attrs:
            attrs.__table__ = name.lower()
        attrs['__mappings__'] = mappings
        attrs['__primary_key__'] = primary_key
        attrs['__sql__'] = lambda self: _gen_sql(attrs['__table__'], mappings)
        for trigger in _triggers:
            if not trigger in attrs:
                attrs[trigger] = None
        return type.__new__(cls, name, bases, attrs)


class Module(dict):
    """
    Base class for ORM.
    ORM的基类
    运用了大量的@classmethod,可以直接调用类的函数，而不需要在实例化之后调用
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
    """
    __metaclass__ = ModelMetaClass

    def __init__(self, **kw):
        super(Module, self).__init__(**kw)

    def __getattr__(self, key):
        """
                get时生效，比如 a[key],  a.get(key)
                get时 返回属性的值
                """
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        """
        set时生效，比如 a[key] = value, a = {'key1': value1, 'key2': value2}
        set时添加属性
        """
        self[key] = value

    @classmethod
    def get(cls, pk):
        """
        Get by primary key.
        """
        d = db.select_one('select * from %s where %s=?' % (cls.__table__, cls.__primary_key__.name), pk)
        return cls(**d) if d else None

    @classmethod
    def find_first(cls, where, *args):
        """
        通过where语句进行条件查询，返回1个查询结果。如果有多个查询结果
        仅取第一个，如果没有结果，则返回None
        """
        d = db.select_one('select * from %s %s' % (cls.__table__, where), *args)
        return cls(**d) if d else None

    @classmethod
    def find_all(cls, *args):
        """
        查询所有字段， 将结果以一个列表返回
        """
        L = db.select('select * from `%s`' % cls.__table__)
        return [cls(**d) for d in L]

    @classmethod
    def find_by(cls, where, *args):
        """
        通过where语句进行条件查询，将结果以一个列表返回
        """
        L = db.select('select * from `%s` %s' % (cls.__table__, where), *args)
        return [cls(**d) for d in L]

    @classmethod
    def count_all(cls):
        """
        执行 select count(pk) from table语句，返回一个数值
        """
        return db.select('select count(`%s`) from `%s`' % (cls.__primay_key__.name, cls.__table__))

    @classmethod
    def count_by(cls, where, *args):
        """
        通过select count(pk) from table where ...语句进行查询， 返回一个数值
        """
        return db.select_int('select count(`%s`) from `%s` %s' % (cls.__primary_key__.name, cls.__table__, where), *args)

    def update(self):
        """
        如果该行的字段属性有 updatable，代表该字段可以被更新
        用于定义的表（继承Model的类）是一个 Dict对象，键值会变成实例的属性
        所以可以通过属性来判断 用户是否定义了该字段的值
            如果有属性， 就使用用户传入的值
            如果无属性， 则调用字段对象的 default属性传入
            具体见 Field类 的 default 属性
        通过的db对象的update接口执行SQL
            SQL: update `user` set `passwd`=%s,`last_modified`=%s,`name`=%s where id=%s,
                    ARGS: (u'******', 1441878476.202391, u'Michael', 10190
        """
        self.pre_update and self.pre_update()
        L = []
        args = []
        for k, v in self.__mappings__.iteritems():
            if v.updatable:
                if hasattr(self, k):
                    arg = getattr(self, k)
                else:
                    arg = v.default
                    setattr(self, k, arg)
                L.append('`%s`=?' % k)
                args.append(arg)
        pk = self.__primary_key__.name
        args.append(getattr(self, pk))
        db.update('update `%s` set %s where %s=?' % (self.__table__, ','.join(L), pk), *args)
        return self

    def delete(self):
        """
        通过db对象的 update接口 执行SQL
        SQL: delete from `user` where `id`=%s, ARGS: (10190,)
            """
        self.pre_delete and self.pre_delete()
        pk = self.__primary_key__.name
        args = (getattr(self, pk),)
        db.update('delete from `%s` where `%s`=?' % (self.__table__, pk), *args)
        return self

    def insert(self):
        """
        通过db对象的insert接口执行SQL
            SQL: insert into `user` (`passwd`,`last_modified`,`id`,`name`,`email`) values (%s,%s,%s,%s,%s),
                　　　　　ARGS: ('******', 1441878476.202391, 10190, 'Michael', 'orm@db.org')
        """
        self.pre_insert and self.pre_insert()
        params = {}
        for k, v in self.__mappings__.iteritems():
            if v.insertable:
                if not hasattr(self, k):
                    setattr(self, k, v.default)
                params[v.name] = getattr(self, k)
        db.insert('%s' % self.__table__, **params)
        return self

    if __name__ == '__main__':
        logging.basicConfig(level=logging.DEBUG)
        db.create_engine('root', 'SERVYOU', 'test')
        db.update('drop table if exists user')
        db.update('create table user (id int primary key, name text, email text, passwd text, last_modified real)')
        import doctest
        doctest.testmod()
