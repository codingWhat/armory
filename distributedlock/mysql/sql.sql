CREATE table tbl_lock (
    id int not null auto_increment primary key,
    rkey varchar(30) not null comment '资源key',
    owner varchar(30) not null comment '锁持有者',
    expireAt timestamp not null comment '锁过期时间' DEFAULT CURRENT_TIMESTAMP,
    reentrant  int unsigned not null comment '重入次数' default 0
) engine= innodb default charset = utf8mb4;

