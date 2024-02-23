## Bitcask-乞丐版

重点实现bitcask思想

- 一致性 WAL
- 顺序IO
- 内存中存储数据的在磁盘的索引位置，直接一次io读取数据
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