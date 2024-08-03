# 参考B站

https://github.com/go-kratos/aegis/blob/main/topk/heavykeeper.go

```
topk := NewHeavyKeeper(10, 10000, 5, 0.925, 0)
topk.Add("target", 1)

for i, node := range topk.List() {
    fmt.Println( node.Key, node.Count)
}
```