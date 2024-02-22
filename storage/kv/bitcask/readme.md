## Bitcask-乞丐版

重点实现bitcask思想

####  介绍
[论文地址](https://riak.com/assets/bitcask-intro.pdf)

## 数据结构
- 内存: HashMap
```
map[key] -> &PosInfo { FileID, Offset}

FileID: 文件ID
Offset: 文件中的位置
```

- 磁盘
```
Type(int8) | CRC(int32) | TS(int32) | KeySize(int32) | ValueSize(int32) | Key | Value
```