<strong>google sre-breaker</strong> 出自书籍《Google SRE》中Handling Overload节

![谷歌自适应断路器-核心算法](/images/sre_breaker.png)
传统断路器在Open状态时有固定的等待窗口，窗口内会拒绝所有请求，以此来保障服务的稳定性，不过在短暂抖动的场景下，这种策略反而会降低服务的可用性(比如等待窗口10s, 可是服务3s就恢复了，还得等待7s才会进入half-open), GoogleSRE提供了一个解决方案-自适应客户端限流，通过滑窗口统计成功请求数、请求总数采用上图算法决策是否放行探针，较传统方式能更快的感知到下游服务的恢复情况
算法: f(x) = max(0, (requests - K * accepts)/(requests+1))

<strong>算法剖析：</strong>
初始状态: requests == accepts,假设K=0.5,
无故障时: f(x) >  0, 当故障开始时, requests变大, accepts保持不变, f(x)一直 > 0
假设K=1
无故障时: f(x) == 0, 当故障开始时, requests变大, accepts保持不变，当requests > 1 * accepts时 f(x) > 0
假设K=2
无故障时: f(x) == 0, 当故障开始时, requests变大, accepts保持不变, 当requests > 2 * accepts时, f(x) > 0

综上，K是调节熔断刚性的因子，当K>=1,偏柔性，比如K=1, 此时相当于能容忍accepts个请求通过，当K<1, 则偏刚性，直接拒绝了。


<strong>总结:</strong>
- 少了很多自定义配置，开发只需要调节K这个变量; K越小越激进
- 实时性更好点，不会有固定的等待窗口


<strong>代码实现</strong>  
可以参考[B站-breaker](https://github.com/go-kratos/kratos/blob/v1.0.x/pkg/net/netutil/breaker/sre_breaker.go)

