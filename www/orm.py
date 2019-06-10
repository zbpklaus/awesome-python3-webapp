# -*- coding: utf-8 -*-
# @Time    : 2018/8/15 18:17
# @Author  : Klaus
# @File    : orm.py
# @Software: PyCharm
import asyncio, logging
import aiomysql

# 因为在web app骨架时我们使用异步，所以所有地方都需要使用异步"一次使用异步，处处使用异步"
# 设置日志记录级别为INFO，日志级别总共为FATAL、CRITICAL、ERROR、WARNING、INFO、DEBUG，级别从大到小，记录信息从小到多
logging.basicConfig(level=logging.INFO)


# 将我们所需要的信息打印到log中，方便调试
def log(sql, args=()):
    logging.info('SQL: %s' % sql)


# 创建连接池，每个http请求都从连接池连接到数据库
# 使用连接池的好处是不必频繁的打开和关闭数据库连接，而是能复用就尽量复用
# 连接池变量由全局变量__pool存储，缺省情况下为utf8,自动提交事务（autocommit）
# async = @asyncio.coroutine, await = yield from
# dict有一个get方法，如果key存在，返回对应值，如果key不存在则返回指定的值，例如host，若host中无host，则返回locahost
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


# 销毁连接池
# __pool.close()关闭进程池，close()不是一个协程，所以不用await
# wait_closed()是一个coroutine
async def destory_pool():
    global __pool
    if __pool is not None:
        __pool.close()
        await __pool.wait_closed()


# select语句
# SQL语句的占位符是?，而MySQL的占位符是%s，select()函数在内部自动替换。注意要始终坚持使用带参数的SQL，而不是自己拼接SQL字符串，这样可以防止SQL注入攻击。
# 使用Cursor对象执行select语句时，通过featchall()可以拿到结果集。如果传入size，则拿到指定数量的结果集。结果集是一个list，每个元素都是一个tuple，对应一行记录。
# tuple一旦初始化就不能修改，例：classmates = ('Michael', 'Bob', 'Tracy')，tuple只有一个元素时定义如：t = (1,)
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    # 建立游标conn
    # 调用一个子协程：async with __pool.get(), __pool已经创建了进程池并和进程池连接了，进程池的创建被封装到了create_pool(loop, **kw)
    # __pool.get()是因为此方法是在同一文件的函数定义里，所以可以直接写__pool.get()， get()的使用是因为dict有一个get方法，所以可以使用get(),
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
        logging.info('rows returned: %s' % len(rs))
        return rs


# insert,update,delete语句
# 使用Cursor对象执行insert，update，delete语句时，执行结果由rowcount返回影响的行数，就可以拿到执行结果
async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected


# 用于输出**元类**中创建sql_insert语句中的占位符
# 这个函数主要是把查询字段计数 替换成sql识别的?
# 比如说：insert into  `User` (`password`, `email`, `name`, `id`) values (?,?,?,?)  看到了么 后面这四个问号
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)


# 定义Field类，负责保存(数据库)表的字段名和字段类型
class Field(object):

    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


# 以下每一种Field分别代表数据库中不同的数据属性，总共五个存储类型
class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


# 定义元类，控制Model对象的创建

# class Model(dict,metaclass=ModelMetaclass):

# -*-定义Model的元类

# 所有的元类都继承自type
# ModelMetaclass元类定义了所有Model基类(继承ModelMetaclass)的子类实现的操作

# -*-ModelMetaclass的工作主要是为一个数据库表映射成一个封装的类做准备：
# ***读取具体子类(user)的映射信息
# 创造类的时候，排除对Model类的修改
# 在当前类中查找所有的类属性(attrs)，如果找到Field属性，就将其保存到__mappings__的dict中，同时从类属性中删除Field(防止实例属性遮住类的同名属性)
# 将数据库表名保存到__table__中

# 完成这些工作就可以在Model中定义各种数据库的操作方法
# metaclass是类的模板，所以必须从`type`类型派生：
class ModelMetaclass(type):
    # 1.当前准备创建的类的对象  2.类的名字 3.类继承的父类集合 4.类的方法集合
    # __new__控制__init__的执行，所以在其执行之前
    # cls:代表要__init__的类，此参数在实例化时由Python解释器自动提供(例如下文的User和Model)
    # bases：代表继承父类的集合
    # attrs：类的方法集合
    def __new__(cls, name, bases, attrs):
        # 排除model 是因为要排除对model类的修改
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 如果没设置__table__属性，tablename就是类的名字
        tableName = attrs.get('__table__', None) or name  #如果存在表名，则返回表名，否则返回 name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        # 获取Field所有主键名和Field
        mappings = dict()  #保存映射关系
        fields = []  #保存除主键外的属性
        primaryKey = None
        # k是列名，值是field子类
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info(' found mapping: %s ==> %s' % (k, v))
                # 把键值对存入mapping字典中
                mappings[k] = v
                if v.primary_key:
                    # 找到主键
                    if primaryKey:
                        raise Exception('Duplicate primary key for field: %s' % k)#一个表只能有一个主键，当再出现一个主键的时候就报错
                    primaryKey = k# 也就是说主键只能被设置一次
                else:
                    fields.append(k)  # 保存除主键外的属性
        if not primaryKey:  #如果主键不存在也将会报错，在这个表中没有找到主键，一个表只能有一个主键，而且必须有一个主键
            raise Exception('Primary key not found')
        # 删除类属性
        for k in mappings.keys():
            attrs.pop(k) #从类属性中删除Field属性,否则，容易造成运行时错误（实例的属性会遮盖类的同名属性）
        # 保存除主键外的名为``（运算出字符串）列表形式
        # 将除主键外的其他属性变成`id`, `name`这种形式
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__table'] = tableName  # 表的名字
        attrs['__primary_key__'] = primaryKey  # 主键属性名
        attrs['__fields__'] = fields  # 除主键外的属性名
        # 反引号和repr()函数功能一致
        # 构造默认的增删改查语句
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s` = ?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


# 定义ORM所有映射的基类：Model
# Model类的任意子类可以映射一个数据库表
# Model类可以看作是对所有数据库表操作的基本定义的映射


# 基于字典查询形式
# Model从dict继承，拥有字典的所有功能，同时实现特殊方法__getattr__和__setattr__，能够实现属性操作
# 实现数据库操作的所有方法，定义为class方法，所有继承自Model都具有数据库操作方法

class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        # 返回对象的属性,如果没有对应属性则会调用__getattr__
        # 直接调回内置函数，注意这里没有下划符,注意这里None的用处,是为了当user没有赋值数据时，返回None，调用于update
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)  #第三个参数None，可以在没有返回数值时，返回None，调用于save
        if value is None:
            field = self.__mappings__[key]
            if field.default is None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                # 把默认属性设置进去
                setattr(self, key, value)
        return value


    # 类方法的第一个参数是cls,而实例方法的第一个参数是self
    @classmethod
    # 类方法有类变量cls传入，从而可以用cls做一些相关的处理。并且有子类继承时，调用该类方法时，传入的类变量cls是子类，而非父类。
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
            orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)

         # dict 提供get方法 指定放不存在时候返回后学的东西 比如a.get('Fuck',None)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)  #tuple融入list
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)  #返回的rs是一个元素是tuple的list
        # **r 是关键字参数，构成了一个cls类的列表，其实就是每一条记录对应的类实例
        return [cls(**r) for r in rs]  #调试的时候尝试了一下return rs，输出结果一样

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        # 将列名重命名为_num_
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        # 限制结果数量为1
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']


    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        #rs是一个list，里面是一个dict
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])  #返回一条记录，以dict的形式返回，因为cls的夫类继承了dict类


    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)



    async def update(self):
        args = list(map(self.getValue, self.__fields__))  #获得的value是User2实例的属性值，也就是传入的name，email，password值

        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows !=1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)



    async def remove(self):
        args = [self.getValue(self.__primary_key__)]  #这里不能使用list()-->'int' object is not iterable
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)





