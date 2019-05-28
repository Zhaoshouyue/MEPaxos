package masterproto

type RegisterArgs struct {
	Addr string
	Port int
}

type RegisterReply struct {
	ReplicaId int
	NodeList  []string
	Ready     bool
}

type GetLeaderArgs struct {
}

type GetLeaderReply struct {
	LeaderId int
}

type GetReplicaListArgs struct {
}

type GetReplicaListReply struct {
	ReplicaList []string
	Ready       bool
}

type PaxosModeInfoArgs struct {
	Repli         int32
	TotalNo       int
	ConfilctNo    int
	Min_k         float64
	RepliToLeader []float64
}

type PaxosModeInfoReply struct {
}

type EPaxosModeInfoArgs struct {
	Repli         int32
	TotalNo       int
	Min_k         float64
}

type EPaxosModeInfoReply struct {
}

type EPaxosModeLeaderInfoArgs struct {
	Repli         int32
	TotalNo       int
	ConfilctNo    []int
	Min_k         float64
	RepliToLeader []float64
}

type EPaxosModeLeaderInfoReply struct {
}