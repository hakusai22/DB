# 事务管理器(TM)

TM是所有模块中最简单的, 我直接用一小段描述他. TM对事务进行管理, 能够让其他模块查询事务的状态. 事务的状态有下面这么几个:

1. active: 事务正在进行;
2. commited: 事务已经提交;
3. aborted: 事务已经被撤销;

在DM那一章中, 我们还提到了terminated, committed和aborted都是terminated.

那TM是怎么样标识各个事务的呢? TM会为每个事务, 分配一个特定的XID, 作为其标识. 且这个XID是递增的, 后开始的事务XID要大于先开始的事务.

XID是从1开始的, XID为0的事务, 被称作超级事务(Super Transaction), 超级事务将永远被当做committed. 因此如果上级模块想在不申请事务的情况下, 对数据库做一些修改, 则它可以将XID设置为0(在IM模块中将会使用到这一点).

TM章, 完:)