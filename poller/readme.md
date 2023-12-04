### 基于Epoll的网络模型设计

### API
1. 系统库: syscall
2. 第三方库: golang.org/x/sys/unix

- func EpollCreate(size int) (fd int, err error)
- func EpollCtl(epfd int, op int, fd int, event *EpollEvent) (err error)
```
EPOLL_CTL_ADD：添加文件描述符。
EPOLL_CTL_MOD：修改文件描述符。
EPOLL_CTL_DEL：删除文件描述符
```
- func EpollWait(epfd int, events []EpollEvent, timeout int) (n int, err error)
```
type EpollEvent struct {
    Events uint32
    Fd     int32
    Pad    int32
}
其中，Events字段表示事件类型，可以是以下值之一：

EPOLLIN：文件描述符可读。
EPOLLOUT：文件描述符可写。
EPOLLERR：文件描述符发生错误。
EPOLLHUP：文件描述符被挂起。
```
___
### Reactor
#### 协程职责划分
- 主协程负责链接的建立和管理
- 子协程有两种模式
  1. 类似redis，保证请求的有序性，单线程处理请求
  2. 多线程并发处理
___
#### 协程池设计
___
#### 队列设计
___
### 架构设计
单协程处理批量的事件
___
### 详细设计 