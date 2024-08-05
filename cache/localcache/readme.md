# 实现高性能的本地缓存库
在日常高流量场景中(读多写少场景)，经常会使用本地缓存来应对热点流量，保障系统的稳定。在Go的生态中缓存库基本有两类，有GC和无GC，我这里对常见的库做了整理。

| 缓存库        | 活跃度                   | 过期时间 | GC                                | 无GC实现原理                                                   | 设计原理                                                       | 淘汰机制               | 设计缺陷            | 
|------------|-----------------------|------|-----------------------------------|-----------------------------------------------------------|------------------------------------------------------------|--------------------|-----------------|
| freecache  | start:5k,fork:390     | 支持   | <font color="red"> Zero GC</font> | 减少指针数，固定512个(256个segment，每个segment两个指针,RingBuf、[]entryPtr) | (分片+互斥锁) + 内置map + slice + ringbuffer                      | LRU                | 不支持自动扩容         |
| bigcache   | start:7.4k,fork:591   | 支持   | <font color="red"> Zero GC</font> | 无指针的map, map[uint64]uint32                                | (分片+读写锁) + map[uint64]uint32 + fifo-buffer                 | FIFO               | 不支持对key设置过期时间   |
| fastcache  | start:2.1k,fork:175   | 不支持  | <font color="red"> Zero GC</font> | 无指针的map, map[uint64]uint32                                                     | (分片+读写锁) + map[uint64]uint64 + ringbuffer(chunks [][]byte) | FIFO               | 不支持过期时间         |
| groupcache | start:12.9k,fork:1.4k | 不支持 | 有                                 | -                                                         | 单元格3                                                       | LRU                | 不支持过期时间         |
| go-cache   | start:8k,fork:865     | 支持 | 有                              | -                                                         | 全局读写锁 + map[string]item                                    | 定期清理过期数据           | 锁竞争严重           |
| ristretto  | start:5.5k,fork:365   | 支持 | 有                              | -                                                         | 分片+读写锁                                                     | TinyLFU/SampledLFU | -               |
| offheap    | start:363,fork:37     | 不支持 | <font color="red"> Zero GC</font> | 堆外内存，syscall.Mmap                                         | 内置HashTable  + syscall.Mmap                     | -                  | 冲突时通过本地探测法，影响性能 |

## 目标
- 高性能
- 使用简单
- 支持按key设置过期时间
- 支持自动淘汰(LRU)

支持API:
```golang

type Cache interface {
	Set(k string, v any, ttl time.Duration) bool
	Get(k string) (v any, err error)
	Del(k string)
	Len() uint64
	Close()
}
```
