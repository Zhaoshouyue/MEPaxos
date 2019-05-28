package megenericsmrproto

import (
	"state"
)

const (
	PROPOSE uint8 = iota
	PROPOSE_REPLY
	READ
	READ_REPLY
	PROPOSE_AND_READ
	PROPOSE_AND_READ_REPLY
	GENERIC_SMR_BEACON
	GENERIC_SMR_BEACON_REPLY
	PAXOSPROPOSE
)

type Propose struct {
	CommandId int32
	Command   state.Command
	Timestamp int64
}

type RepliPropose struct {
	CommandId int32
	Command   state.Command
	Timestamp int64
	ConnToCli int64
}

type ProposeReply struct {
	OK        uint8
	CommandId int32
}

type ProposeReplyTS struct {
	OK        uint8
	CommandId int32
	Value     state.Value
	Timestamp int64
}

type ProposeRepliReplyTS struct {
	OK        uint8
	CommandId int32
	Value     state.Value
	Timestamp int64
	ConnToCli int64
}

type Read struct {
	CommandId int32
	Key       state.Key
}

type ReadReply struct {
	CommandId int32
	Value     state.Value
}

type ProposeAndRead struct {
	CommandId int32
	Command   state.Command
	Key       state.Key
}

type ProposeAndReadReply struct {
	OK        uint8
	CommandId int32
	Value     state.Value
}

// handling stalls and failures

type Beacon struct {
	Timestamp uint64
}

type BeaconReply struct {
	Timestamp uint64
}

type PingArgs struct {
	ActAsLeader uint8
}

type PingReply struct {
}

type BeTheLeaderArgs struct {
}

type BeTheLeaderReply struct {
}

type PaxosPrepare struct {
	MasterVersion int
}

type EPaxosPrepare struct {
	MasterVersion int
}

type PaxosPrepareReply struct {
	RepliVersion int
	Maxnumber int32
}

type EPaxosPrepareReply struct {
	RepliVersion int
	Maxnumber int32
}

type PaxosPropose struct {
	LeaderId int
	MasterVersion int
	Maxnumber []int32
}

type EPaxosPropose struct {
	MasterVersion int
	Maxnumber     int32
}

type PaxosProposeReply struct {
}

type EPaxosProposeReply struct {
}

/*type PaxosVersionId struct {
	RepliVersion int
}

type PaxosVersionIdReply struct {
}*/

type PaxosCancel struct {
	LeaderId int
	MasterVersion int
}

type EPaxosCancel struct {
	MasterVersion int
}

type PaxosCancelReply struct {
	LeaderId int
	RepliVersion int
}

type EPaxosCancelReply struct {
	RepliVersion int
}
