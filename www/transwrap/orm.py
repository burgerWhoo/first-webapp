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
        s = ['<%s:%s,%s,default(%s),'%(self.__class, self.name, self.ddl, self._default)]
        self.nullable and s.append('N')
        self.updatable and s.append('U')
        self.insertable and s.append('I')
        s.append('>')
        return ''.join(s)




class Model(dict):
    """

    """

class ModelMetaClass():
