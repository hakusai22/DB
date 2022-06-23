# 表管理器(TBM)

## 引言

TBM主要实现两个功能, 1)利用VM维护表的结构, 2)解析并执行对应的数据库语句. 下面分别进行描述.

## 表和记录的结构

首先, 一个数据库中会有多张表存在, TBM会将这些表以链表的方式组织起来. 同时维护一个BootPointer, 指向第一张表所在的位置, BootPointer的位置为固定的.

对于每张表, TBM需要维护的信息大致有下面这些:

1. Table Name: 表名;
2. IsDroped: 该表是否已经被drop掉;
3. Next Table Address: 下一张表的地址;
4. NoFields: 该表字段的个数;
5. Field0, Field1, ...: 每个字段的信息;

对于表中的每个字段, TBM需要维护的信息大致有下面这些:

1. Field Name: 字段名;
2. Type Name: 该字段类型;
3. Index Boot Address: 该字段索引的地址;(详细见之后的IM章节)

需要说明的是, 目前NYADB支持的字段类型有3种: uint32, uint64, string. 对于这3种类型就不多赘述了.

表由一条一条的记录(entry)组成, 这些记录的被VM所管理, 每当TBM向VM中插入一条数据时, VM会返回一个句柄, TBM下次就利用这个句柄(实际上是一个地址), 从VM中读取或者修改该条记录.

而记录本身的结构, 将由TBM来维护, TBM维护记录的方式非常简单: 直接将记录的值, 转换成二进制的字节数组, 然后将该字节数组, 当做数据, 交给VM进行存储和管理. 之后, 当需要某条数据时, 则通过其地址, 从VM将其读出, 然后反解析出其值.

## 数据库语句的解析

先来看一下NYADB支持的文法和例子:

```
  {begin statement}
        begin [isolation level (read committed | repeatable read)]
            begin isolation level read committed

    {commit statement}
        commit

    {abort statement}
        abort

    {create statement}
        create table {table name}
        {field name} {field type}
        {field name} {field type}
        ...
        {field name} {field type}
        [(index {field name list})]
            create table students
            id int32,
            name string,
            age int32,
            (index id name)

    {drop statement}
        drop table {table name}
            drop table students

    {read statement}
        read (*|{field name list}) from {table name} [{where statement}]
            read * from student where id = 1
            read name from student where id > 1 and id < 4
            read name, age, id from student where id = 12

    {insert statement}
        insert into {table name} values {value list}
            insert into student values 5 "Zhang Yuanjia" 22

    {delete statement}
        delete from {table name} {where statement}
            delete from student where name = "Zhang Yuanjia"

    {update statement}
        update {table name} set {field name}={value} [{where statement}]
            update student set name = "ZYJ" where id = 5

    {where statement}
        where {field name} (>|<|=) {value} [(and|or) {field name} (>|<|=) {value}]
            where age > 10 or age < 3

    {field name} {table name}
        [a-zA-Z][a-zA-Z0-9]*

    {field type}
        uint32 uint64 string

    {value}
        .*
    
```

NYADB会对上述的文法进行解析, 解析的方法也很简单: 首先将语句给token化, 然后之后就是if-else的方法进行判断和进行语句组装了. token化的方法也很简单, 分析出文法的词素, 然后写一个小自动机, 不断的切分就行了.

**限制和TODO:** 目前文法的文法非常简单, 但也非常局限. 如Where只支持对一个字段的索引, 以及不能递归的表达逻辑等. 我之后会逐渐丰富这些文法. 另外目前对Read并没有做读取筛选, 既不管你在field字段输入了多少, 最后结果都等同于Read * from ...

## TBM中无锁

和VM不同, 在数据访问的层面上, TBM完全不需要锁. 当然在实现时, 或许有一些共享的变量, 需要用锁去保护, 但是这里不予以讨论. TBM完全将并发控制交给了VM.

试想, 如果有两个事务, 同时准备update同一条记录, 会发生怎样的情况? 情况会是: 后执行的那个事务会被VM阻塞, 然后根据前一个事务的执行情况, 来决定是否需要被回滚. 总而言之, TBM中即使无锁, 其事务操作的可串行性也依然会被保证, 因为这被VM保证了.

## TBM中语句的执行

为了演示TBM, VM和IM是怎么合作工作的, 下面来模拟一下read和insert操作.

假设现在数据库需要执行: read * from student where id = 5 这条语句. 那么执行的过程大致如下:

1. TBM对语句进行解析.
2. TBM调用IM, 查询得到id = 5的那条记录的句柄(也就是一个地址).
3. TBM根据得到的地址, 从VM中读出数据.
4. TBM将从VM读出的二进制数据进行解析, 解析出记录原本的内容.
5. 将还原后的记录的内容返回给用户.

假设现在数据库需要执行: insert into student values "zhangyuanjia" 2012141461290 这条语句. 那么执行的过程大致如下:

1. TBM对语句进行解析, 并生成记录.
2. TBM将记录二进制化, 并调用VM, 将其存入DB中, 并得到一个句柄.
3. TBM调用IM, 将句柄, 作为value让IM给存入索引中.
4. 返回.

## 总结

TBM主要维护表结构, 和解析并执行语句. TBM中不会有锁, 它将事务的并发控制交给了VM去做