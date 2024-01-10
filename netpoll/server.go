package netpoll

type Options struct {
	Loops int
}

type Option func(*Options)

func WithLoops(num int) Option {
	return func(ops *Options) {
		ops.Loops = num
	}
}

type Server struct {
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Run(addr string) {
	//
	//listener, err := net.Listen("tcp", addr)
	//if err != nil {
	//	return
	//}
	//

}

func GetFd() {

}
