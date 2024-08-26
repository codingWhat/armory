package cache

type Cache interface {
	Set(k string, v any)
	Get(k string) any
}
