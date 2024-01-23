package netpoll

type LoadBalance interface {
	Next() interface{}
}
