package main

//平滑的加权轮训算法

type Node struct {
	IP        string
	CurWeight int
	Weight    int
}

func NewNode(ip string, weight int) *Node {
	return &Node{
		IP:        ip,
		CurWeight: weight,
		Weight:    weight,
	}
}

type SmoothWeightedRoundRobin struct {
	nodes     map[string]*Node
	sumWeight int
}

func NewSmoothWeightedRoundRobin() *SmoothWeightedRoundRobin {
	return &SmoothWeightedRoundRobin{nodes: make(map[string]*Node)}
}

func (s *SmoothWeightedRoundRobin) Add(node *Node) bool {
	if node.IP == "" {
		return false
	}

	s.nodes[node.IP] = node
	s.sumWeight += node.Weight
	return true
}

func (s *SmoothWeightedRoundRobin) Select() string {

	var maxNode *Node
	//对每个节点计算权重(初始权重+当前权重)，取出最大权重的那个节点，并重新计算其权重
	//计算公式: 当前权重-所有节点权重和
	for _, node := range s.nodes {
		node.CurWeight = node.CurWeight + node.Weight
		if maxNode == nil {
			maxNode = node
		} else if maxNode.CurWeight < node.CurWeight {
			maxNode = node
		}
	}

	if maxNode != nil {
		maxNode.CurWeight = maxNode.CurWeight - s.sumWeight
		return maxNode.IP
	}
	return ""
}
