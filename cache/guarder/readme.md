# 极简缓存治理框架

## 背景
在海量请求场景下，缓存扮演着非常重要的角色，一般都会采用cache-Aside模式，如果读cache miss, 就会从持久化存储中加载数据到缓存中。如果接口多了会有大量的重复代码，仔细思考
其实缓存场景无非就是如下几类，为何不设计一个通用框架，避免重复工作呢。
### 常见缓存场景
1. Local Cache -> RemoteCache
2. Local Cache -> RemoteCache -> DB

### 缓存问题
 - 缓存雪崩(缓存集中过期/redis宕机)
 - 缓存穿透(恶意请求不存在的key，导致大量请求穿透到DB，拖垮DB)
 - 缓存击穿(热key突然失效，大量请求穿透)
### 缓存解决方案
#### 缓存雪崩
- 过期时间随机, `base time` + `rand time`
#### 缓存穿透
一般有如下两种策略，不过各有优缺点，本组件采用第一种。
- 缓存空数据(缺点: 占一定空间, 优点:实现简单)
- 可以将存量数据存到布隆过滤器(缺点，需要额外存储，实现较复杂，需要数据同步)

#### 缓存击穿
缓存可以设置不过期，如果一定要设置不过期:
- 缓存模式1, 支持主动异步更新缓存
- 缓存模式2, 支持缓存`singleflight`请求合并
### 热点治理(todo)
 - 热点识别 
 - 缓存预热
 - 负载均衡
 - 可观测
## 目标
1. 接入简单，杜绝重复代码
2. 自定义配置缓存降级模式
3. 支持Get/Hash/Zset/等操作高可用缓存接口


## 已完成命令
- [x] MGet/ZRevrange 常用命令 

## Example
- 模式1, 适合高并发场景，支持异步更新缓存，不会造成goroutine积压。
```golang

func main() {
    ctx := context.Background()
    redisClient := redis2.NewRedigoClient(newPool())
    client := New(redisClient,
        WithEnableLocalCache(), //使用默认的本地缓存， 可以用	WithLocalCache()替换
        WithLocalCacheTTL(5*time.Second),
    )
	
	// 异步更新缓存
    client.AddCronTask(ctx, "reload-cache-actively", 2*time.Second, func () {
    fmt.Println("reload-cache-actively", time.Now().Unix())
    _, _ = client.ReloadZrevRange(ctx, "aaa", 0, 1)
    })
    defer func () {
        client.RemoveCronTask(ctx, "reload-cache-actively")
        client.Close()
    }()

    for i := 0; i < 10; i++ {
    revRange, err := client.ZRevRange(ctx, "aaa", 0, 1)
    fmt.Println(revRange, err)
    time.Sleep(5 * time.Second)
    }
}



```
- 模式2, 更适合较低并发场景，请求穿透到DB时会通过`singleflight`合并，保护下游DB
```golang

func main() {
    ctx := context.Background()
    redisClient := redis2.NewRedigoClient(newPool())
    client := New(redisClient,
        WithEnableLocalCache(), //使用默认的本地缓存， 可以用	WithLocalCache()替换
        WithLocalCacheTTL(5*time.Second),
    )

    for i := 0; i < 10; i++ {
        ret, err := client.ZRevRangeWithLoad(ctx, "aaa", 0, 1, func(ctx context.Context, client interface{}, key ...string) (map[string]interface{}, error) {
        // todo db ops
        return nil, nil
        })
        fmt.Println(revRange, err)
        time.Sleep(5 * time.Second)
    }
}
```