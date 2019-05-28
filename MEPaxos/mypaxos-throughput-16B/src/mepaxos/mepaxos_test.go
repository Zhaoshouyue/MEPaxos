package mepaxos

import (
	"mepaxosproto"
	"fmt"
	"megenericsmr"
	"state"
	"testing"
)

func initReplica() *Replica {
	peers := make([]string, 3)
	r := &Replica{megenericsmr.NewReplica(0, peers, false, false, false),
		make(chan *Propose, CHAN_BUFFER_SIZE),
		make(chan *mepaxosproto.Prepare, CHAN_BUFFER_SIZE),
		make(chan *mepaxosproto.PreAccept, CHAN_BUFFER_SIZE),
		make(chan *mepaxosproto.PreAccept, CHAN_BUFFER_SIZE),
		make(chan *mepaxosproto.Accept, CHAN_BUFFER_SIZE),
		make(chan *mepaxosproto.Commit, CHAN_BUFFER_SIZE),
		make(chan *mepaxosproto.PrepareReply, CHAN_BUFFER_SIZE),
		make(chan *mepaxosproto.PreAcceptReply, CHAN_BUFFER_SIZE),
		make(chan *mepaxosproto.PreAcceptOK, CHAN_BUFFER_SIZE),
		make(chan *mepaxosproto.AcceptReply, CHAN_BUFFER_SIZE),
		make([][]*Instance, 3),
		make([]int32, 3),
		false,
		0,
		make([]int32, 3),
		nil}

	for i := 0; i < r.N; i++ {
		r.InstanceSpace[i] = make([]*Instance, 1024*1024)
		r.crtInstance[i] = 0
		r.ExecedUpTo[i] = -1
	}

	r.exec = &Exec{r}

	return r
}

func (r *Replica) MakeInstance(q, i int, seq int32, deps [3]int32) {
	command := &state.Command{state.PUT, state.Key(q), state.Value(i)}
	r.InstanceSpace[q][i] = &Instance{command, 0, mepaxosproto.COMMITTED, seq, deps, nil, 0, 0}
}

func TestExec(t *testing.T) {

	r := initReplica()

	r.MakeInstance(0, 0, 2, [3]int32{0, 0, 0})
	r.MakeInstance(1, 0, 1, [3]int32{0, 0, 0})
	r.MakeInstance(2, 0, 0, [3]int32{0, 0, 0})

	r.MakeInstance(0, 1, 0, [3]int32{0, 1, 0})
	r.MakeInstance(1, 1, 2, [3]int32{0, 0, 1})
	r.MakeInstance(2, 1, 0, [3]int32{1, 0, 0})

	r.MakeInstance(0, 2, 0, [3]int32{1, 1, 1})
	r.MakeInstance(1, 2, 0, [3]int32{1, 1, 2})
	r.MakeInstance(2, 2, 0, [3]int32{2, 1, 1})

	r.MakeInstance(0, 3, 0, [3]int32{2, 2, 2})
	r.MakeInstance(1, 3, 0, [3]int32{0, 0, 0})
	r.MakeInstance(2, 3, 0, [3]int32{0, 0, 0})

	r.MakeInstance(0, 4, 1, [3]int32{3, 5, 0})
	r.MakeInstance(1, 4, 2, [3]int32{0, 0, 0})
	r.MakeInstance(2, 4, 3, [3]int32{0, 0, 0})

	r.MakeInstance(0, 5, 4, [3]int32{4, 5, 5})
	r.MakeInstance(1, 5, 5, [3]int32{5, 5, 5})
	r.MakeInstance(2, 5, 6, [3]int32{5, 0, 5})

	r.exec.executeCommand(0, 5)
	r.exec.executeCommand(0, 5)

	fmt.Println("Test ended\n")
}
