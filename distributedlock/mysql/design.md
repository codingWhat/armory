## 由浅入深探索分布式锁设计

### 设计要点
- 互斥
- 支持过期时间，避免死锁
- 可重入
- 可支持通知机制，避免轮训重试
- 公平性


### 设计思路
- 通知机制，采用事务 + `for update`
- 互斥，唯一索引 + `owner`
- 可重入，`reentrant`保证


### 详细设计
表设计:
```sql
CREATE  TABLE tbl_distribute_lock (
    id int not null auto_increment primary key,
    rkey varchar(20) not null comment "资源key",
    expiredAt datetime not null comment  "过期时间",
    owner varchar(20) not null comment "锁持有者",
    reentrant varchar(1024) not null default 0 comment "重入对象集合",
    unique key x (rkey)
) engine =innodb default charset =utf8mb4;
```

#### V1版本:
Lock:   
```insert into tbl_distribute_lock (rkey, owner) values (xxx, xxx);```

UnLock:  
```delete from tbl_distribute_lock where rkey=xxx and owner = xxx;```

缺点:
- 死锁问题. 如果client挂了，那就没法释放锁，会导致死锁，所以必须要用 `on duplicate key update `解决冲突。


#### V2版本 - 支持过期时间:
Lock:   
如果client挂了，新增判断锁是否过期，如果过期，则更新锁信息
```sql
    insert into tbl_distribute_lock (rkey, owner, expiredAt) values (xxx, xxx,xxx)
    on duplicate key update 
   `expiredAt` = IF(`expiredAt` < NOW(), NOW() + INTERVAL ? SECOND, expiredAt),  
   `owner` = IF(`expiredAt` < NOW(), VALUES(`owner`), `owner`)
```
UnLock:  
```delete from tbl_distribute_lock where rkey=xxx and owner = xxx;```

#### V3版本 - 支持重入1
Lock:  
```sql
    insert into tbl_distribute_lock (rkey, owner, expiredAt) values (xxx, xxx,xxx)
    on duplicate key update 
   `expiredAt` = IF(`expiredAt` < NOW() OR VALUES(`owner`) = `owner`, NOW() + INTERVAL ? SECOND, expiredAt),  
   `owner` = IF(`expiredAt` < NOW() OR VALUES(`owner`) = `owner`, VALUES(`owner`), `owner`)
```
UnLock:  
```delete from tbl_distribute_lock where rkey=xxx and owner = xxx;```

缺点:
没有实现重入细节，比如重入计数，unlock只是粗暴的删除。

#### V4版本  - 支持重入2
Lock:
先判断是否存在当前owner存活的key
```sql
 select reentrant from tbl_distribute_lock where expiredAt > NOW() and rkey=? and ownner = ?
```
如果存在并且reentrant 当前的reentrant，
则更新
```sql
update tbl_distribute_lock set reentrant = CONCAT(`reentrant`, ?), expiredAt = NOW() + INTERVAL ? SECOND where rkey=? and ownner = ?
```

UnLock:
先获取当前重入信息
```sql
select reentrant from tbl_distribute_lock where expiredAt > NOW() and rkey=? and ownner = ?
```
如果reentrant和当前reentrant一致， 则:
```delete from tbl_distribute_lock where rkey=xxx and owner = xxx;```
否则, 移除当前reentrant,更新:
```update tbl_distribute_lock set reentrant=? where rkey=xxx and owner = xxx;```

V1-V4的缺点:
- 需要client端轮训尝试，无形中产生了很多无效请求。

#### V5 - 支持通知
通知机制的原理是锁持有者需要保持Session,开启事务，对锁`for update`操作。
当其他owner执行insert时会被阻塞，原因是锁持有者执行了`for update`加了互斥锁。

解锁时，当前锁持有者提交事务，其他owner开始抢占锁  
Lock:
```sql
    insert into tbl_distribute_lock (rkey, owner, expiredAt) values (xxx, xxx,xxx)
    on duplicate key update 
   `expiredAt` = IF(`expiredAt` < NOW() OR VALUES(`owner`) = `owner`, NOW() + INTERVAL ? SECOND, expiredAt),  
   `owner` = IF(`expiredAt` < NOW() OR VALUES(`owner`) = `owner`, VALUES(`owner`), `owner`)
```
锁持有者, 通过mysql的互斥锁机制实现通知机制。
```sql
    begin;
    select rkey from tbl_distribute_lock where rkey = ? and owner = ? for update
```

UnLock: 
```sql
    delete from tbl_distribute_lock where rkey= ? and owner = ?;
    commit;
```


### 异常处理
#### 如果Lock失败
- 插入失败, 轮训退避重试
- for update失败，需要清理数据

#### 如果UnLock失败
- Lock时依靠`on duplicate key update` 解决冲突
