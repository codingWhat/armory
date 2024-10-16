
> 在日常高流量场景中(读多写少场景)，经常会使用本地缓存来应对热点流量，保障系统的稳定。可是你有没有好奇过它底层是怎么实现的呢？数据是如何管理的呢？如果你来设计一个缓存库，你会如何设计呢?

# 他山之石，可以攻玉
在开始之前，借助开源社区了解主流缓存库的种类、设计思想以及适用场景是一个明智的做法。通过这样的调研，可以了解到不同缓存库的特点和优势，并从中汲取经验，以设计出符合自己需求的缓存库。
为了方便学习和理解，我对主流库做了详细调研并整理出以下多维度对比图，帮助你更清晰地了解不同缓存库之间的差异和优势。

| 缓存库        | 社区活跃度                 | 过期时间 | GC                                | 无GC实现原理                                                                | 设计原理                                                       | 淘汰机制               | 设计缺陷            | 
|------------|-----------------------|------|-----------------------------------|------------------------------------------------------------------------|------------------------------------------------------------|--------------------|-----------------|
| freecache  | start:5k,fork:390     | 支持   | <font color="red"> Zero GC</font> | 减少指针数，固定512个(256个segment，每个segment两个指针,RingBuf(内部[]byte指针)、[]entryPtr) | (分片+互斥锁) + 内置map + slice + ringbuffer                      | LRU                | 不支持自动扩容         |
| bigcache   | start:7.4k,fork:591   | 支持   | <font color="red"> Zero GC</font> | 无指针的map, map[uint64]uint32                                             | (分片+读写锁) + map[uint64]uint32 + fifo-buffer                 | FIFO               | 不支持对key设置过期时间   |
| fastcache  | start:2.1k,fork:175   | 不支持  | <font color="red"> Zero GC</font> | 无指针的map, map[uint64]uint32                                             | (分片+读写锁) + map[uint64]uint64 + ringbuffer(chunks [][]byte) | FIFO               | 不支持过期时间         |
| groupcache | start:12.9k,fork:1.4k | 不支持 | 有                                 | -                                                                      | 全局读写锁 + LRU链表                                              | LRU                | 不支持过期时间         |
| go-cache   | start:8k,fork:865     | 支持 | 有                              | -                                                                      | 全局读写锁 + map[string]item                                    | 定期清理过期数据           | 锁竞争严重           |
| ristretto  | start:5.5k,fork:365   | 支持 | 有                              | -                                                                      | 分片+读写锁                                                     | TinyLFU/SampledLFU | -               |
| offheap    | start:363,fork:37     | 不支持 | <font color="red"> Zero GC</font> | 堆外内存，syscall.Mmap                                                      | 内置HashTable  + syscall.Mmap                                | -                  | 冲突时通过本地探测法，影响性能 |

上述中比较有意思的是Zero-Gc这个概念，我总结下关键信息:  
<strong>如何实现Zero-GC?</strong>  
1. 完全避免GC: 采用syscall.MMap申请堆外内存，gc就不会扫描
2. 规避GC扫描策略: 数组(固定了指针数量) + 非指针map[uint64]uint32 + []byte(参考freecache) 或者 slice + 非指针的map + ringbuffer(参考bigcache) 

<strong>如何选择？</strong>  
就笔者的经验来看，比如在http服务中，从缓存库读取[]byte内容，写入socket，这种场景Zero-GC库确实很高效比较适合(这也正是bigCache诞生的背景)。但是在业务逻辑中的海量操作都要经过序列化是不可接受的，会占用很多cpu资源，而非zero-gc就算gc会影响程序的性能，但是缓存项毕竟会淘汰不是无限的，再加上go现在的gc优化的也不错，所以权衡之下优先采用非Zero-GC库。

综上，没有一个缓存库适用于所有场景和问题, 每个缓存库的诞生都是为了解决特定场景下的特定问题。 这些问题主要分为以下几类:
- 锁竞争。全局锁导致大量请求都在抢锁、休眠，严重影响性能
- 数据淘汰。内存资源有限，必须要按特定策略淘汰数据
- GC问题。存储海量对象时，GC扫描的影响不容小觑
----

# 实践出真知
接下来围绕上述三个问题来设计我们自己的高性能本地缓存库。
## 设计目标
- 高性能, 减少锁竞争
- 使用简单，配置不能太复杂，要开箱即用
- 支持按key设置过期时间
- 支持自动淘汰(LRU)
- 不要求Zero-GC, 但也应该尽量减少GC

## 设计思路
- 锁竞争: 读写锁 + 数据分片
- 数据淘汰: LRU
- 高性能: 合并写操作; 批量更新; 
- GC优化: 我们的目标是减少GC，尽量减少对象分配

## 详细设计
### API设计:
```golang

type Cache interface {
	Set(k string, v any, ttl time.Duration) bool
	Get(k string) (v any, err error)
	Del(k string)
	Len() uint64
	Close()
}
```
### 核心数据结构
#### cache
cache中核心结构为store、policy、expireKeyTimers模块, store负责存储引擎的实现，policy负责淘汰机制，expireKeyTimers管理过期清理的定时任务，这三者共同组成了缓存库基础骨架。
```golang
type cache struct {
	size int

	store            store   // 读写锁 + 数据分片
	policy           policy  //链表淘汰策略，LRU等
	ekt              *expireKeyTimers //维护key的定期清理任务
	accessUniqBuffer *uniqRingBuffer // 合并Get操作，降低对链表的移动

	accessEvtCh chan []*list.Element //批量Get操作，支持批量更新链表
	updateEvtCh chan *entExtendFunc  //合并对链表的Update
	addEvtCh    chan *entExtendFunc  //合并写操作(包含链表和map)
	delEvtCh    chan *keyExtendFunc  //合并对链表的Del

	isSync     bool //同步标识，会阻塞等待至写成功之后
	setTimeout time.Duration //阻塞等待超时时间
}
```
#### store - 存储引擎实现
store 提供增删改查的接口，可以根据自己的需求实现对应的接口，比如我们这里用就是shardedMap, 通过分片来降低锁的粒度, 减少锁竞争。
```azure
type store interface {
    set(k string, v any)
    get(k string) (any, bool)
    del(k string)
    len() uint64
    clear()
}
type shardedMap struct {
    shards []*safeMap
}
    
type safeMap struct {
	mu   sync.RWMutex
	data map[string]any
}

```

#### policy - 淘汰机制
淘汰机制主要是在对数据增删改查时，通过一定的策略来移动链表元素，以保证活跃的缓存项留在内存中，同时淘汰不活跃的缓存项。常见淘汰策略有LRU、LFU等。LRU较简单，可以通过标准库中的list实现policy接口实现。
```golang
// 缓存项，包含 key,value,过期时间
type entry struct {
    key      string
    val      any
    expireAt time.Time
    mu sync.RWMutex
}
type policy interface {
	isFull() bool
	add(*entry) (*list.Element, *list.Element) // 返回新增, victim:淘汰的entry
	remove(*list.Element)
	update(*entry, *list.Element)
	renew(*list.Element)
	batchRenew([]*list.Element)
}
```

#### expireKeyTimers - 过期时间
这个模块主要维护过期key的定时清理任务。底层主要依赖第三方[时间轮库](https://github.com/RussellLuo/timingwheel)来管理定时任务
```golang
type expireKeyTimers struct {
	mu     sync.RWMutex
	timers map[string]*timingwheel.Timer

	tick      time.Duration
	wheelSize int64
	tw        *timingwheel.TimingWheel
}
```
### hash函数选型
[常见hash函数压测对比](https://github.com/smallnest/hash-bench)
![常见hash函数](hash_func.png)

----
fnv64 vs xxhash  
测试机器: mac-m1, go benchmark结果

| hash函数 | fnv64a  | github.com/cespare/xxhash/v2  |
|--------|---------|---------|
| 8字节    | 5.130 ns/op | 8.817 ns/op |
| 16字节   | 7.928 ns/op|   7.464 ns/op |
| 32字节   | 17.17 ns/op  | 14.22 ns/op|


### 高性能优化
#### 写操作
<strong>隔离:</strong>  按channel隔离增、删、改  
<strong>同步转异步:</strong>  链表并发写操作，改为异步单协程更新
<strong>支持非阻塞</strong>

#### 读操作
<strong>批量操作:</strong> 采用ringbuffer，批量更新链表

#### 内存优化
采用sync.Pool池化ringbuffer对象，避免频繁创建对象

### 压测对比
| 压测case                                              | 操作次数      | 单次耗时                |
|-----------------------------------------------------|-----------|---------------------|
| BenchmarkSyncMapSetParallelForStruct-10             | 	 1330215 | 	       907.6 ns/op |
| BenchmarkFreeCacheSetParallelForStruct-10           | 	 2191702 | 	       549.8 ns/op |
| BenchmarkBigCacheSetParallelForStruct-10            | 	 2250765 | 	       528.6 ns/op |
| <strong>BenchmarkLCSetParallelForStruct-10</strong> | 	  590721 | 	      2114 ns/op   |
| BenchmarkSyncMapGetParallelForStruct-10             |      	 3960368|	       313.6 ns/op|
| BenchmarkFreeCacheGetParallelForStruct-10           |    	 2146406|	       545.5 ns/op|
| BenchmarkBigCacheGetParallelForStruct-10            |     	 2329698|	       516.4 ns/op|
| <strong>BenchmarkLCGetParallelForStruct-10</strong> |           	 2386426|	       475.1 ns/op|

经过压测分析，在写入数据由于我们采用的是同步模式，对比其他库有一些弱势，但是在读取时由于不需要序列化操作，相比Zero-Gc要快些。

### 未来展望
从上述压测结果可以分析得出在高并发写场景下，跟其他主流库相比吞吐较低，主要原因是因为底层channel并发操作性能不佳，虽说本地缓存面向的是读多写少的场景，但如果有些场景对写的要求很高，可以用无锁队列替换。