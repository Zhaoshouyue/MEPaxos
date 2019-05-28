package mepaxos

import (
	"bloomfilter"
	"dlog"
	"encoding/binary"
	"mepaxosproto"
	"fastrpc"
	"megenericsmr"
	"megenericsmrproto"
	"masterproto"
	"paxosproto"
	"io"
	"log"
	"math"
	"state"
	"sync"
	"time"
	"net/rpc"
)

const MAX_DEPTH_DEP = 10
const TRUE = uint8(1)
const FALSE = uint8(0)
const DS = 5
const ADAPT_TIME_SEC = 10

const MAX_BATCH = 1000

const COMMIT_GRACE_PERIOD = 10 * 1e9 //10 seconds

const BF_K = 4
const BF_M_N = 32.0

var bf_PT uint32

const DO_CHECKPOINTING = false
const HT_INIT_SIZE = 200000
const CHECKPOINT_PERIOD = 10000
const PAXOS_NO = 300000

var cpMarker []state.Command
var cpcounter = 0
var conflicted, weird, slow, happy int = 0, 0, 0, 0
var paxosNo int = 0 //the number of command of this replica transmit in Paxos mode
var timeSet bool = false //start set time (use in the change of EPaxos and Paxos, for time out)
var timeNo int = 0 //use in time out 
var beforeAlgorithm int = 0 //for recording the algorithm before changing algorithm,-1 represent EPaxos

type Replica struct {
	*megenericsmr.Replica
	prepareChan           chan fastrpc.Serializable
	preAcceptChan         chan fastrpc.Serializable
	acceptChan            chan fastrpc.Serializable
	commitChan            chan fastrpc.Serializable
	commitShortChan       chan fastrpc.Serializable
	prepareReplyChan      chan fastrpc.Serializable
	preAcceptReplyChan    chan fastrpc.Serializable
	preAcceptOKChan       chan fastrpc.Serializable
	acceptReplyChan       chan fastrpc.Serializable
	tryPreAcceptChan      chan fastrpc.Serializable
	tryPreAcceptReplyChan chan fastrpc.Serializable
	paxosprepareChan      chan fastrpc.Serializable
	paxosacceptChan       chan fastrpc.Serializable
	paxosprepareReplyChan chan fastrpc.Serializable
	paxosacceptReplyChan  chan fastrpc.Serializable
	paxosdoneReplyChan    chan fastrpc.Serializable
	paxoscommitChan       chan fastrpc.Serializable
	paxoscommitShortChan  chan fastrpc.Serializable
	prepareRPC            uint8
	prepareReplyRPC       uint8
	preAcceptRPC          uint8
	preAcceptReplyRPC     uint8
	preAcceptOKRPC        uint8
	acceptRPC             uint8
	acceptReplyRPC        uint8
	commitRPC             uint8
	commitShortRPC        uint8
	tryPreAcceptRPC       uint8
	tryPreAcceptReplyRPC  uint8
	paxosprepareRPC       uint8
	paxosacceptRPC        uint8
	paxosprepareReplyRPC  uint8
	paxosacceptReplyRPC   uint8
	paxosdoneReplyRPC     uint8
	paxoscommitRPC        uint8
	paxoscommitShortRPC   uint8
	InstanceSpace         [][]*Instance // the space of all instances (used and not yet used)
	crtInstance           []int32       // highest active instance numbers that this replica knows about
	CommittedUpTo         [DS]int32     // highest committed instance per replica that this replica knows about
	ExecedUpTo            []int32       // instance up to which all commands have been executed (including iteslf)
	exec                  *Exec
	conflicts             []map[state.Key]int32
	maxSeqPerKey          map[state.Key]int32
	maxSeq                int32
	latestCPReplica       int32
	latestCPInstance      int32
	clientMutex           *sync.Mutex // for synchronizing when sending replies to clients from multiple go-routines
	instancesToRecover    chan *instanceId
	mcli                  *rpc.Client // the connection with master
	//noEpaxos              bool //when it's true, represents don't start EPaxos message anymore
	maxnum                []int32 //the EPaxos max instance number of each Replica
	pmaxnum               int32 //the Paxos max instance number of leader
	defaultBallot         int32 // default ballot for new instances (0 until a Prepare(ballot, instance->infinity) from a leader)
	paxosInstanceSpace    []*PaxosInstance
	paxoscrtInstance      int32 // highest active instance number that this replica knows about
	paxoscommittedUpTo    int32 //the instance number that has committed
	repliPassCommand      [][]int//the number of commands of each replica pass to leader in Paxos mode
	largestConflict       []int //the conflicts which has the largest conflict for each command in Paxos mode
	largestCReplica       []int //the replica which has the largest conflict in Paxos mode
	repliConflict         []int //the number of conflict for each replica
	otherConflict         []int //the number of the conflict of the other replica except the replica which has the largest commands number in Paxos mode
}

type Instance struct {
	Cmds           []state.Command
	ballot         int32
	Status         int8
	Seq            int32
	Deps           [DS]int32
	lb             *LeaderBookkeeping
	Index, Lowlink int
	bfilter        *bloomfilter.Bloomfilter
}

type InstanceStatus int

const (
	PREPARING InstanceStatus = iota
	PREPARED
	ACCEPTED
	COMMITTED
)

type PaxosInstance struct {
	cmds   []state.Command
	ballot int32
	status InstanceStatus
	lb     *PaxosLeaderBookkeeping
}

type instanceId struct {
	replica  int32
	instance int32
}

type RecoveryInstance struct {
	cmds            []state.Command
	status          int8
	seq             int32
	deps            [DS]int32
	preAcceptCount  int
	leaderResponded bool
}

type LeaderBookkeeping struct {
	clientProposals   []*megenericsmr.Propose
	maxRecvBallot     int32
	prepareOKs        int
	allEqual          bool
	preAcceptOKs      int
	acceptOKs         int
	nacks             int
	originalDeps      [DS]int32
	committedDeps     []int32
	recoveryInst      *RecoveryInstance
	preparing         bool
	tryingToPreAccept bool
	possibleQuorum    []bool
	tpaOKs            int
}

type PaxosLeaderBookkeeping struct {
	clientProposals []*megenericsmr.PaxosPropose
	maxRecvBallot   int32
	prepareOKs      int
	acceptOKs       int
	nacks           int
}

func NewReplica(id int, peerAddrList []string, portnum int, thrifty bool, exec bool, dreply bool, beacon bool, durable bool, mclient *rpc.Client) *Replica {
	r := &Replica{
		megenericsmr.NewReplica(id, peerAddrList, portnum, thrifty, exec, dreply),
		make(chan fastrpc.Serializable, megenericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, megenericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, megenericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, megenericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, megenericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, megenericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, megenericsmr.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, megenericsmr.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, megenericsmr.CHAN_BUFFER_SIZE*3),
		make(chan fastrpc.Serializable, megenericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, megenericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, megenericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, megenericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, 3*megenericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, 3*megenericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, 3*megenericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, megenericsmr.CHAN_BUFFER_SIZE),
		make(chan fastrpc.Serializable, megenericsmr.CHAN_BUFFER_SIZE),
		0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
		make([][]*Instance, len(peerAddrList)),
		make([]int32, len(peerAddrList)),
		[DS]int32{-1, -1, -1, -1, -1},
		make([]int32, len(peerAddrList)),
		nil,
		make([]map[state.Key]int32, len(peerAddrList)),
		make(map[state.Key]int32),
		0,
		0,
		-1,
		new(sync.Mutex),
		make(chan *instanceId, megenericsmr.CHAN_BUFFER_SIZE),
	    mclient,
	    //false,
	    make([]int32, len(peerAddrList)),
	    -1,
	    -1,
	    make([]*PaxosInstance, 15*1024*1024),
	    0,
	    -1,
	    make([][]int, len(peerAddrList)),
	    make([]int, PAXOS_NO),
	    make([]int, PAXOS_NO),
	    make([]int, len(peerAddrList)),
	    make([]int, PAXOS_NO)}

	r.Beacon = beacon
	r.Durable = durable

	for i := 0; i < r.N; i++ {
		r.InstanceSpace[i] = make([]*Instance, 2*1024*1024)
		r.crtInstance[i] = 0
		r.ExecedUpTo[i] = -1
		r.conflicts[i] = make(map[state.Key]int32, HT_INIT_SIZE)
		r.maxnum[i] = -1
		r.repliPassCommand[i] = make([]int, PAXOS_NO)
	}

	for bf_PT = 1; math.Pow(2, float64(bf_PT))/float64(MAX_BATCH) < BF_M_N; {
		bf_PT++
	}

	r.exec = &Exec{r}

	cpMarker = make([]state.Command, 0)

	//register RPCs
	r.prepareRPC = megenericsmr.RegisterRPC(r.Replica, new(mepaxosproto.Prepare), r.prepareChan)
	r.prepareReplyRPC = megenericsmr.RegisterRPC(r.Replica, new(mepaxosproto.PrepareReply), r.prepareReplyChan)
	r.preAcceptRPC = megenericsmr.RegisterRPC(r.Replica, new(mepaxosproto.PreAccept), r.preAcceptChan)
	r.preAcceptReplyRPC = megenericsmr.RegisterRPC(r.Replica, new(mepaxosproto.PreAcceptReply), r.preAcceptReplyChan)
	r.preAcceptOKRPC = megenericsmr.RegisterRPC(r.Replica, new(mepaxosproto.PreAcceptOK), r.preAcceptOKChan)
	r.acceptRPC = megenericsmr.RegisterRPC(r.Replica, new(mepaxosproto.Accept), r.acceptChan)
	r.acceptReplyRPC = megenericsmr.RegisterRPC(r.Replica, new(mepaxosproto.AcceptReply), r.acceptReplyChan)
	r.commitRPC = megenericsmr.RegisterRPC(r.Replica, new(mepaxosproto.Commit), r.commitChan)
	r.commitShortRPC = megenericsmr.RegisterRPC(r.Replica, new(mepaxosproto.CommitShort), r.commitShortChan)
	r.tryPreAcceptRPC = megenericsmr.RegisterRPC(r.Replica, new(mepaxosproto.TryPreAccept), r.tryPreAcceptChan)
	r.tryPreAcceptReplyRPC = megenericsmr.RegisterRPC(r.Replica, new(mepaxosproto.TryPreAcceptReply), r.tryPreAcceptReplyChan)
	r.paxosprepareRPC = megenericsmr.RegisterRPC(r.Replica, new(paxosproto.Prepare), r.paxosprepareChan)
	r.paxosacceptRPC = megenericsmr.RegisterRPC(r.Replica, new(paxosproto.Accept), r.paxosacceptChan)
	r.paxosprepareReplyRPC = megenericsmr.RegisterRPC(r.Replica, new(paxosproto.PrepareReply), r.paxosprepareReplyChan)
	r.paxosacceptReplyRPC = megenericsmr.RegisterRPC(r.Replica, new(paxosproto.AcceptReply), r.paxosacceptReplyChan)
	r.paxosdoneReplyRPC = megenericsmr.RegisterRPC(r.Replica, new(megenericsmrproto.ProposeRepliReplyTS), r.paxosdoneReplyChan)
	r.paxoscommitRPC = megenericsmr.RegisterRPC(r.Replica, new(paxosproto.Commit), r.paxoscommitChan)
	r.paxoscommitShortRPC = megenericsmr.RegisterRPC(r.Replica, new(paxosproto.CommitShort), r.paxoscommitShortChan)

	go r.run()

	return r
}


/* RPC to be called by master */
func (r *Replica) PaxosPrepare(args *megenericsmrproto.PaxosPrepare, reply *megenericsmrproto.PaxosPrepareReply) error {
	if r.VersionId < args.MasterVersion {
		beforeAlgorithm = r.DoPaxosState //set time out
		r.DoPaxosState = 0
		r.VersionId = args.MasterVersion
		reply.Maxnumber = r.crtInstance[r.Id]-1//the highest active instance

		//reset some variables of Paxos mode
		r.pmaxnum = -1 //initiate the leader's largest instance number of Paxos
		conflicted, weird, slow, happy = 0, 0, 0, 0
		paxosNo = 0 //the number of command of this replica transmit in Paxos mode
		r.defaultBallot = -1
		r.paxoscommittedUpTo = -1
		r.paxoscrtInstance = 0
		for i:=0; i<PAXOS_NO; i++ {
			r.largestConflict[i] = 0
			r.largestCReplica[i] = 0
			r.otherConflict[i] = 0
			for j:=0; j<r.N; j++ {
				r.repliPassCommand[j][i] = 0
			}
		}
		for i:=0; i<r.N; i++ {
			r.repliConflict[i] = 0
		}
		for i:=0; i<15*1024*1024; i++ {
			r.paxosInstanceSpace[i] = nil
		}
		timeNo = 0 //set time out
		timeSet = true //set time out
		//log.Printf("replica %d prepare paxos, r.LeaderId is %d\n", r.Id, r.LeaderId)
	}else {
		log.Printf("replica %d not prepare paxos，r.VersionId is %d, args.MasterVersion is %d\n", r.Id, r.VersionId, args.MasterVersion)
		reply.RepliVersion = r.VersionId
	}
	return nil
}

func (r *Replica) EPaxosPrepare(args *megenericsmrproto.EPaxosPrepare, reply *megenericsmrproto.EPaxosPrepareReply) error {
	if r.VersionId < args.MasterVersion {
		beforeAlgorithm = r.DoPaxosState //set time out
		r.DoPaxosState = 0
		r.VersionId = args.MasterVersion
		if r.LeaderId == int(r.Id) {
			reply.Maxnumber = r.paxoscrtInstance - 1
		}

		///////////////////////////////////////////////////////////////
		//r.InstanceSpace = make([][]*Instance, r.N)
		//r.crtInstance = make([]int32, r.N)
		//r.CommittedUpTo = [DS]int32{-1, -1, -1, -1, -1}
		//r.ExecedUpTo = make([]int32, r.N)
		r.exec = nil
		//r.conflicts = make([]map[state.Key]int32, r.N)
		r.maxSeqPerKey = make(map[state.Key]int32)
		r.maxSeq = 0
		r.latestCPReplica = 0
		r.latestCPInstance = -1
		r.clientMutex = new(sync.Mutex)
		r.instancesToRecover = make(chan *instanceId, megenericsmr.CHAN_BUFFER_SIZE)
		for k := 0; k < DS; k++ {
			r.CommittedUpTo[k] = -1
		}
		for i := 0; i < r.N; i++ {
			/*for j := 0; j < 2*1024*1024; j++ {
				r.InstanceSpace[i][j] = nil
			}*/
			r.InstanceSpace[i] = make([]*Instance, 2*1024*1024)
			r.crtInstance[i] = 0
			r.ExecedUpTo[i] = -1
			r.conflicts[i] = make(map[state.Key]int32, HT_INIT_SIZE)
			r.maxnum[i] = -1
		}
		for bf_PT = 1; math.Pow(2, float64(bf_PT))/float64(MAX_BATCH) < BF_M_N; {
			bf_PT++
		}
		r.exec = &Exec{r}
		cpMarker = make([]state.Command, 0)
		cpcounter = 0
		/////////////////////////////////////////////////////////////////////
		timeNo = 0 //set time out
		timeSet = true //set time out
		log.Printf("replica %d prepare Epaxos\n", r.Id)
	}else {
		reply.RepliVersion = r.VersionId
	}
	return nil
}

func (r *Replica) CancelPaxos(args *megenericsmrproto.PaxosCancel, reply *megenericsmrproto.PaxosCancelReply) error {
	if r.VersionId<=args.MasterVersion {
		r.LeaderId = -1
		r.DoPaxosState = -1
		r.VersionId = args.MasterVersion
		//r.noEpaxos = false
		reply.LeaderId = args.LeaderId
		timeSet = false
		log.Printf("replica %d cancel paxos\n", r.Id)
	}else {
		reply.RepliVersion = r.VersionId
	}
	return nil
}

func (r *Replica) CancelEPaxos(args *megenericsmrproto.EPaxosCancel, reply *megenericsmrproto.EPaxosCancelReply) error {
	if r.VersionId<=args.MasterVersion {
		r.DoPaxosState = 1
		r.VersionId = args.MasterVersion
		//r.noEpaxos = true
		timeSet = false
		log.Printf("replica %d cancel Epaxos\n", r.Id)
	}else {
		reply.RepliVersion = r.VersionId
	}
	return nil
}

func (r *Replica) PaxosPropose(args *megenericsmrproto.PaxosPropose, reply *megenericsmrproto.PaxosProposeReply) error {
	r.LeaderId = args.LeaderId
	r.DoPaxosState = 1
	r.VersionId = 0
	r.maxnum = args.Maxnumber
	timeSet = false
	log.Printf("replica %d go to paxos mode\n", r.Id)
	return nil
}

func (r *Replica) EPaxosPropose(args *megenericsmrproto.EPaxosPropose, reply *megenericsmrproto.EPaxosProposeReply) error {

	r.DoPaxosState = -1
	r.VersionId = 0
	r.pmaxnum = args.Maxnumber
	timeSet = false
	log.Printf("replica %d go to Epaxos mode\n", r.Id)
	return nil
}

func (r *Replica) UpdateLeader(args *masterproto.GetLeaderReply, reply *megenericsmrproto.BeTheLeaderArgs) error{
	r.LeaderId = args.LeaderId
	return nil
}

//append a log entry to stable storage
func (r *Replica) recordInstanceMetadata(inst *Instance) {
	if !r.Durable {
		return
	}

	var b [9 + DS*4]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.ballot))
	b[4] = byte(inst.Status)
	binary.LittleEndian.PutUint32(b[5:9], uint32(inst.Seq))
	l := 9
	for _, dep := range inst.Deps {
		binary.LittleEndian.PutUint32(b[l:l+4], uint32(dep))
		l += 4
	}
	r.StableStore.Write(b[:])
}

//write a sequence of commands to stable storage
func (r *Replica) recordCommands(cmds []state.Command) {
	if !r.Durable {
		return
	}

	if cmds == nil {
		return
	}
	for i := 0; i < len(cmds); i++ {
		cmds[i].Marshal(io.Writer(r.StableStore))
	}
}

//sync with the stable store
func (r *Replica) sync() {
	if !r.Durable {
		return
	}

	r.StableStore.Sync()
}

/* Clock goroutine */

//var fastClockChan chan bool
var slowClockChan chan bool
//var isPassProposeChan chan bool

/*func (r *Replica) fastClock() {
	for !r.Shutdown {
		time.Sleep(5 * 1e6) // 5 ms
		fastClockChan <- true
	}
}*/

func (r *Replica) partition(ewma []float64, low int, high int) int {
    var k = ewma[low]
    for low<high {
        for low<high && ewma[high]>=k {
            high--
        }
        if low<high {
            ewma[low] = ewma[high]
            low++
        }
        for low<high && ewma[low]<=k {
            low++
        }
        if low<high {
            ewma[high] = ewma[low]
            high--
        }
    }
    ewma[low] = k
    return low
}

func (r *Replica) find_mink(ewma []float64, s int, t int,m *float64){
    var key int
    key = r.partition(ewma, s, t)
    if key==r.N/2 {
        *m=ewma[key]
    } else if key < r.N/2 {
        r.find_mink(ewma, key+1, t, m)
    } else {
        r.find_mink(ewma, s, key-1, m)
    }
}

func (r *Replica) slowClock() {
    var k = 0 //for counting. When k is 100, replica need to send information to master, so t is 100*150ms=15s
    var mini_k float64 = -1 //the F+1 largest communication latency of all relica
    var a1, a2, b1, b2 int = 0, 0, 0, 0//a1,b1 represent the start amounts of happy and slow, a2, b2 represent the fina amounts of them
    var total int = 0 //total number of commands in 15s
    var c int = 0 //conflict number of commands in 15s
    var min_k *float64 = &mini_k//the k minist latency between the replica with others
    var ave_latency = make([]float64, r.N)//the time of a round of communication with other replica
    var ave_latency_replica = make([]float64, r.N)
    var myflag bool = true //true represents the last algorithm is EPaxos, false represents the last algorithm is Paxos
    var replicaOrder bool = true //when true, do the beacon function, only execute once

	for !r.Shutdown {
		slowClockChan <- true
        time.Sleep(100 * 1e6) // 100ms

        //for change between EPaxos and Paxos, it's about time out
        if timeSet {
        	timeNo = timeNo + 1
        	if timeNo >= 10 { //time out time :100ms*10=1s
        		if beforeAlgorithm == -1 {//EPaxos
        			r.LeaderId = -1
        			r.DoPaxosState = -1
        			beforeAlgorithm = 0
        		}else if beforeAlgorithm == 1 {//Paxos
        			r.DoPaxosState = 1
        			beforeAlgorithm = 0
        		}
        	}
        }
        //
        if r.DoPaxosState == -1 {//the current mode is EPaxos
        	if !myflag {//when myflag is false, the last algorithm is Paxos, we need to initialize some variables
        		k = 0
        		mini_k = -1

        		for i:=0; i<r.N; i++ {
        			r.Ewma[i] = 0
        			r.TotalNumber[i] = 0
        			ave_latency[i] = 0
        		}
        		myflag = true
        	}
        	if k == 0 {//record the start amounts of happy and slow.
        		a1 = happy
        		b1 = slow
        	}
        	k++
        	if k==150 {//when it reached 15 seconds 
        		a2 = happy//record the final amounts of happy and slow.
        		b2 = slow
        		conflicted, weird, slow, happy = 0, 0, 0, 0
        		c = b2 - b1 //the conflict commands' amount in 15s
        		total = a2 - a1 + c //the total commands' amount in 15s
        		//calculate the time of a round of communication with other replicas,if no commucation, set math.MaxFloat64
        		for j:=0; j<r.N; j++ {
        			if r.TotalNumber[j] !=0 {//the amount of beacon
        				ave_latency[j] = float64(r.Ewma[j])/float64(r.TotalNumber[j])//the time of a round of communication with replica j
        			} else if j!=int(r.Id) {
        				ave_latency[j] = math.MaxFloat64
        			}
        		}
        		//log.Printf("the replica latency to other replica is ")
        		//log.Println(ave_latency)

        		//same as the function of Beacon, execute only once
        		if replicaOrder {
        			r.Beacon = false
        			time.Sleep(1000 * 1000 * 1000)
        			for i := 0; i < r.N-1; i++ {
        				min := i
        				for j := i + 1; j < r.N-1; j++ {
        					if r.Ewma[r.PreferredPeerOrder[j]] < r.Ewma[r.PreferredPeerOrder[min]] {
        						min = j
        					}
        				}
        				aux := r.PreferredPeerOrder[i]
        				r.PreferredPeerOrder[i] = r.PreferredPeerOrder[min]
        				r.PreferredPeerOrder[min] = aux
        			}
        			replicaOrder = false
        			log.Println(r.PreferredPeerOrder)
        			log.Println(r.Ewma[r.PreferredPeerOrder[r.N/2]])
        		}

        		for i:=0; i<r.N; i++ {
        			ave_latency_replica[i] = ave_latency[i]
        		}

        		r.find_mink(ave_latency_replica, 0, r.N-1, min_k)
        		if mini_k == -1 {
        			log.Fatal("An error happen when calculate min_k")
        		}
        		//log.Printf("the mini_k is ")
        		//log.Println(mini_k)
        		//calculate the F+1 largest communication latency of all replicas
        		
        		args := new(masterproto.PaxosModeInfoArgs)
        		args.Repli = r.Id
        		args.TotalNo = total
        		args.ConfilctNo = c
        		args.RepliToLeader = ave_latency
        		args.Min_k = mini_k

        		//pass the information to master
        		if err:= r.mcli.Call("Master.GetPaxosModeInfo", args, new(masterproto.PaxosModeInfoReply)); err!=nil {
        			log.Printf("Error connecting to master\n")
        		}
        		//reset some variables
        		for i:=0; i<r.N; i++ {
        			r.Ewma[i] = 0
        			r.TotalNumber[i] = 0
        			ave_latency[i] = 0
        		}
        		k = 0
        		mini_k = -1
        	}
        } else if r.DoPaxosState == 1{ //the current mode is Paxos
        	if myflag {//when myflag is true, the last algorithm is EPaxos, we need to initialize some variables
        		mini_k = -1
        		for i:=0; i<r.N; i++ {
        			r.Ewma[i] = 0
        			r.TotalNumber[i] = 0
        			ave_latency[i] = 0
        		}
        		k = 0
        		myflag = false
        	}
        	k++
        	if k==150 {//when reaching 15s
        		//calculate the time of a round of communication with other replicas,if no commucation, set math.MaxFloat64
        		for j:=0; j<r.N; j++ {
        			if r.TotalNumber[j] !=0 {
        				ave_latency[j] = float64(r.Ewma[j])/float64(r.TotalNumber[j])
        			} else if j!=int(r.Id) {
        				ave_latency[j] = math.MaxFloat64
        			}
        		}
        		//log.Printf("the total Paxos number is %d\n", paxosNo)
        		//log.Printf("the replica latency to other replica is(paxos) ")
        		//log.Println(ave_latency)

        		for i:=0; i<r.N; i++ {
        			ave_latency_replica[i] = ave_latency[i]
        		}

        		r.find_mink(ave_latency_replica, 0, r.N-1, min_k)
        		if mini_k == -1 {
        			log.Fatal("An error happen when calculate min_k")
        		}
        		//log.Printf("the mini_k is(paxos) ")
        		//log.Println(mini_k)

        		//if the replica is leader
        		if r.LeaderId == int(r.Id) {
        			//Calculate conflict number of each replica
        			//find each key words's largest conflict amount and the corresponding replica
        			for i := 0; i < PAXOS_NO; i++ {
        				for j := 0; j < r.N; j++ {
        					if r.repliPassCommand[j][i] >= r.largestConflict[i] {
        						r.largestCReplica[i] = j
        						r.largestConflict[i] = r.repliPassCommand[j][i]
        					}
        				}
        			}
        			for i := 0; i < PAXOS_NO; i++ {
        				//calculate the amount of other replica's commands except the replica which has the largest command number
        				for j := 0; j < r.N; j++ {
        					if j != r.largestCReplica[i] {
        						r.otherConflict[i] = r.otherConflict[i] + r.repliPassCommand[j][i]
        					}
        				}
        				//calculate the largest conflict number
        				if r.largestConflict[i] > r.otherConflict[i] {
        					r.repliConflict[r.largestCReplica[i]] = r.repliConflict[r.largestCReplica[i]] + r.otherConflict[i]
        				} else {
        					r.repliConflict[r.largestCReplica[i]] = r.repliConflict[r.largestCReplica[i]] + r.largestConflict[i]
        				}
        				//calculate other replica's conflict number
        				for j := 0; j < r.N; j++ {
        					if j != r.largestCReplica[i] {
        						r.repliConflict[j] = r.repliConflict[j] + r.repliPassCommand[j][i]
        					}
        				}
        			}

        			/*for i:=0; i<r.N; i++ {
        				log.Printf("the replica %d total conflicts is: %d\n", i, r.repliConflict[i])
        			}*/

        			argsLeader := new(masterproto.EPaxosModeLeaderInfoArgs)
        			argsLeader.Repli = r.Id
        			argsLeader.TotalNo = paxosNo
        			argsLeader.RepliToLeader = ave_latency
        			argsLeader.ConfilctNo = r.repliConflict
        			argsLeader.Min_k = mini_k
        			
        			//pass the information to the master
        			if err:= r.mcli.Call("Master.GetEPaxosModeLeaderInfo", argsLeader, new(masterproto.EPaxosModeLeaderInfoReply)); err!=nil {
        				log.Printf("Error connecting to master\n")
        			}
        		} else { //the replica is not leader
        			//
        			args := new(masterproto.EPaxosModeInfoArgs)
        			args.Repli = r.Id
        			args.TotalNo = paxosNo
        			args.Min_k = mini_k
        			if err:= r.mcli.Call("Master.GetEPaxosModeInfo", args, new(masterproto.EPaxosModeInfoReply)); err!=nil {
        				log.Printf("Error connecting to master\n")
        			} else {
        				log.Printf("Sucess to inform master\n")
        			}
        		}
        		//reset some vaviables
        		for i:=0; i<PAXOS_NO; i++ {
        			r.largestConflict[i] = 0
        			r.largestCReplica[i] = 0
        			r.otherConflict[i] = 0
        			for j:=0; j<r.N; j++ {
        				r.repliPassCommand[j][i] = 0
        			}
        		}
        		for i:=0; i<r.N; i++ {
        			r.repliConflict[i] = 0
        			r.Ewma[i] = 0
        			r.TotalNumber[i] = 0
        			ave_latency[i] = 0
        		}
        		k = 0
        		mini_k = -1
        		paxosNo = 0
        	}
        }
    }
}

/*func (r *Replica) stopAdapting() {           
	time.Sleep(1000 * 1000 * 1000 * ADAPT_TIME_SEC)
	r.Beacon = false
	time.Sleep(1000 * 1000 * 1000)

	for i := 0; i < r.N-1; i++ {
		min := i
		for j := i + 1; j < r.N-1; j++ {
			if r.Ewma[r.PreferredPeerOrder[j]] < r.Ewma[r.PreferredPeerOrder[min]] {
				min = j
			}
		}
		aux := r.PreferredPeerOrder[i]
		r.PreferredPeerOrder[i] = r.PreferredPeerOrder[min]
		r.PreferredPeerOrder[min] = aux
	}

	log.Println(r.PreferredPeerOrder)
}*/

/* ============= */


/***********************************
   Main event processing loop      *
************************************/

func (r *Replica) run() {
	megenericsmr.ConnectToPeers(r.Replica)

	dlog.Println("Waiting for client connections")

	go megenericsmr.WaitForClientConnections(r.Replica)

	if r.Exec {
		go r.executeCommands()
	}

	if r.Id == 0 {
		//init quorum read lease
		quorum := make([]int32, r.N/2+1)
		for i := 0; i <= r.N/2; i++ {
			quorum[i] = int32(i)
		}
		megenericsmr.UpdatePreferredPeerOrder(r.Replica, quorum)
	}

	/*if r.Beacon {
		go r.stopAdapting()
	}*/

	slowClockChan = make(chan bool, 1)
	//fastClockChan = make(chan bool, 1)
	//isPassProposeChan = make(chan bool, 1)
	go r.slowClock()

	//Enabled when batching for 5ms
	/*if MAX_BATCH > 100 {
		go r.fastClock()
		//go r.isPassPropose()
	}*/

	onOffProposeChan := r.ProposeChan
	paxosonOffProposeChan := r.PaxosProposeChan

	for !r.Shutdown {

		//if r.DoPaxosState == -1 || len(r.prepareChan)!=0 || len(r.preAcceptChan)!=0 || len(r.acceptChan)!=0 || len(r.commitChan)!=0 || len(r.commitShortChan)!=0 || len(r.prepareReplyChan)!=0 || len(r.preAcceptReplyChan)!=0 || len(r.preAcceptOKChan)!=0 || len(r.acceptReplyChan)!=0 || len(r.tryPreAcceptChan)!=0 || len(r.tryPreAcceptReplyChan)!=0 || len(r.instancesToRecover)!=0 {
		if r.DoPaxosState !=0 {
			select {
			case propose := <-onOffProposeChan:
				//got a Propose from a client
				if r.DoPaxosState == -1 {
					dlog.Printf("Proposal with op %d\n", propose.Command.Op)
					r.handlePropose(propose)
				} else if r.DoPaxosState == 1 {
					dlog.Printf("Proposal with op %d\n", propose.Command.Op)
					dlog.Printf("Started to pass propose in Command %d\n", propose.CommandId)
					r.passPropose(propose) //do Paxos as a replica
					dlog.Printf("Success to pass propose\n\n")
				}
				
				 //do EPaxos
				//deactivate new proposals channel to prioritize the handling of other protocol messages,
				////and to allow commands to accumulate for batching
				//onOffProposeChan = nil
				break

			/*case <-fastClockChan:
				//activate new proposals channel
				//if r.DoPaxosState == -1 {
				onOffProposeChan = r.ProposeChan
				paxosonOffProposeChan = r.PaxosProposeChan
				//}
				break*/

			case prepareS := <-r.prepareChan:
				prepare := prepareS.(*mepaxosproto.Prepare)
				//got a Prepare message
				dlog.Printf("Received Prepare for instance %d.%d\n", prepare.Replica, prepare.Instance)
				r.handlePrepare(prepare)
				break

			case preAcceptS := <-r.preAcceptChan:
				preAccept := preAcceptS.(*mepaxosproto.PreAccept)
				//got a PreAccept message
				dlog.Printf("Received PreAccept for instance %d.%d\n", preAccept.LeaderId, preAccept.Instance)
				r.handlePreAccept(preAccept)
				break

			case acceptS := <-r.acceptChan:
				accept := acceptS.(*mepaxosproto.Accept)
				//got an Accept message
				dlog.Printf("Received Accept for instance %d.%d\n", accept.LeaderId, accept.Instance)
				r.handleAccept(accept)
				break

			case commitS := <-r.commitChan:
				commit := commitS.(*mepaxosproto.Commit)
				//got a Commit message
				dlog.Printf("Received Commit for instance %d.%d\n", commit.LeaderId, commit.Instance)
				r.handleCommit(commit)
				break

			case commitS := <-r.commitShortChan:
				commit := commitS.(*mepaxosproto.CommitShort)
				//got a Commit message
				dlog.Printf("Received Commit for instance %d.%d\n", commit.LeaderId, commit.Instance)
				r.handleCommitShort(commit)
				break

			case prepareReplyS := <-r.prepareReplyChan:
				prepareReply := prepareReplyS.(*mepaxosproto.PrepareReply)
				//got a Prepare reply
				dlog.Printf("Received PrepareReply for instance %d.%d\n", prepareReply.Replica, prepareReply.Instance)
				r.handlePrepareReply(prepareReply)
				break

			case preAcceptReplyS := <-r.preAcceptReplyChan:
				preAcceptReply := preAcceptReplyS.(*mepaxosproto.PreAcceptReply)
				//got a PreAccept reply
				dlog.Printf("Received PreAcceptReply for instance %d.%d\n", preAcceptReply.Replica, preAcceptReply.Instance)
				r.handlePreAcceptReply(preAcceptReply)
				break

			case preAcceptOKS := <-r.preAcceptOKChan:
				preAcceptOK := preAcceptOKS.(*mepaxosproto.PreAcceptOK)
				//got a PreAccept reply
				dlog.Printf("Received PreAcceptOK for instance %d.%d\n", r.Id, preAcceptOK.Instance)
				r.handlePreAcceptOK(preAcceptOK)
				break

			case acceptReplyS := <-r.acceptReplyChan:
				acceptReply := acceptReplyS.(*mepaxosproto.AcceptReply)
				//got an Accept reply
				dlog.Printf("Received AcceptReply for instance %d.%d\n", acceptReply.Replica, acceptReply.Instance)
				r.handleAcceptReply(acceptReply)
				break

			case tryPreAcceptS := <-r.tryPreAcceptChan:
				tryPreAccept := tryPreAcceptS.(*mepaxosproto.TryPreAccept)
				dlog.Printf("Received TryPreAccept for instance %d.%d\n", tryPreAccept.Replica, tryPreAccept.Instance)
				r.handleTryPreAccept(tryPreAccept)
				break

			case tryPreAcceptReplyS := <-r.tryPreAcceptReplyChan:
				tryPreAcceptReply := tryPreAcceptReplyS.(*mepaxosproto.TryPreAcceptReply)
				dlog.Printf("Received TryPreAcceptReply for instance %d.%d\n", tryPreAcceptReply.Replica, tryPreAcceptReply.Instance)
				r.handleTryPreAcceptReply(tryPreAcceptReply)
				break

			case beacon := <-r.BeaconChan:
				dlog.Printf("Received Beacon from replica %d with timestamp %d\n", beacon.Rid, beacon.Timestamp)
				megenericsmr.ReplyBeacon(r.Replica, beacon)
				break

			case <-slowClockChan:
				if r.Beacon {
					for q := int32(0); q < int32(r.N); q++ {
						if q == r.Id {
							continue
						}
						megenericsmr.SendBeacon(r.Replica, q)
					}
				}
				break
			
			case <-r.OnClientConnect:
				log.Printf("weird %d; conflicted %d; slow %d; happy %d\n", weird, conflicted, slow, happy)
				weird, conflicted, slow, happy = 0, 0, 0, 0

			case iid := <-r.instancesToRecover:
				r.startRecoveryForInstance(iid.replica, iid.instance)
			//}
		//}else if r.DoPaxosState == 1 || len(r.PaxosProposeChan)!=0 || len(r.paxosprepareChan)!=0 || len(r.paxosacceptChan)!=0 || len(r.paxosdoneReplyChan)!=0 || len(r.paxoscommitChan)!=0 || len(r.paxoscommitShortChan)!=0 || len(r.paxosprepareReplyChan)!=0 || len(r.paxosacceptReplyChan)!=0{
			//select {

			/*case propose := <-onOffProposeChan:
				//got a Propose from a client
				dlog.Printf("Proposal with op %d\n", propose.Command.Op)
				dlog.Printf("Started to pass propose in Command %d\n", propose.CommandId)
				r.passPropose(propose) //do Paxos as a replica
				dlog.Printf("Success to pass propose\n\n")
				onOffProposeChan = nil
				break*/

			case paxosModePropose := <-paxosonOffProposeChan:
				if r.LeaderId == int(r.Id){
					dlog.Printf("Start handle paxos propose from replica %d in Command %d\n", paxosModePropose.ReplyForRepli, paxosModePropose.CommandId)
					r.handlePaxosPropose(paxosModePropose) //do Paxos as a leader
					dlog.Printf("Success handle paxos propose\n\n")
					//paxosonOffProposeChan = nil
				}
				break

			/*case <-isPassProposeChan:
				if r.DoPaxosState == 1 {
					onOffProposeChan = r.ProposeChan
				}
				break*/

			/*case <-fastClockChan:
				//activate new proposals channel
				paxosonOffProposeChan = r.PaxosProposeChan
				break*/

			case paxosprepareS := <-r.paxosprepareChan:
				paxosprepare := paxosprepareS.(*paxosproto.Prepare)
				//got a PaxosPrepare message
				dlog.Printf("Start prepare Paxos from replica %d, for instance %d\n", paxosprepare.LeaderId, paxosprepare.Instance)
				r.handlePaxosPrepare(paxosprepare)
				dlog.Printf("Success prepare Paxos\n\n")
				break

			case paxosacceptS := <-r.paxosacceptChan:
				paxosaccept := paxosacceptS.(*paxosproto.Accept)
				//got an Paxos Accept message
				dlog.Printf("Start accept Paxos Accept from replica %d, for instance %d\n", paxosaccept.LeaderId, paxosaccept.Instance)
				r.handlePaxosAccept(paxosaccept)
				dlog.Printf("Success accept Paxos\n\n")
				break

			
			case paxosdoneReplyS := <-r.paxosdoneReplyChan:
				paxosdoneReply := paxosdoneReplyS.(*megenericsmrproto.ProposeRepliReplyTS)
				//got reply from the leader to the replica which transmit client request
				dlog.Printf("Start send confirm to client in command %d\n", paxosdoneReply.CommandId)
				megenericsmr.ReplyProposeTS(
					r.Replica,
					&megenericsmrproto.ProposeReplyTS{
						paxosdoneReply.OK,
						paxosdoneReply.CommandId,
						paxosdoneReply.Value,
						paxosdoneReply.Timestamp},
					r.ClientConn[paxosdoneReply.ConnToCli])
				paxosNo ++
				dlog.Printf("Success send confirm to client\n\n")
				break

			case paxoscommitS := <-r.paxoscommitChan:
				paxoscommit := paxoscommitS.(*paxosproto.Commit)
				//got a Paxos Commit message
				dlog.Printf("Start commit Paxos from replica %d, for instance %d\n", paxoscommit.LeaderId, paxoscommit.Instance)
				r.handlePaxosCommit(paxoscommit)
				dlog.Printf("Success commit Paxos\n\n")
				break

			case paxoscommitS := <-r.paxoscommitShortChan:
				paxoscommit := paxoscommitS.(*paxosproto.CommitShort)
				//got a Paxos Commit message
				dlog.Printf("Start commit Paxos from replica %d, for instance %d\n", paxoscommit.LeaderId, paxoscommit.Instance)
				r.handlePaxosCommitShort(paxoscommit)
				dlog.Printf("Success commit Paxos\n\n")
				break

			case paxosprepareReplyS := <-r.paxosprepareReplyChan:
				paxosprepareReply := paxosprepareReplyS.(*paxosproto.PrepareReply)
				//got a Paxos Prepare reply
				dlog.Printf("Start handle Paxos PrepareReply for instance %d\n", paxosprepareReply.Instance)
				r.handlePaxosPrepareReply(paxosprepareReply)
				dlog.Printf("Success handle Paxos prepare reply\n\n")
				break

			case paxosacceptReplyS := <-r.paxosacceptReplyChan:
				paxosacceptReply := paxosacceptReplyS.(*paxosproto.AcceptReply)
				//got a Paxos Accept reply
				dlog.Printf("Start handle Paxos accept reply for instance %d\n", paxosacceptReply.Instance)
				r.handlePaxosAcceptReply(paxosacceptReply)
				dlog.Printf("Success handle Paxos accept reply\n\n")
				break

			case beacon := <-r.BeaconChan:
				dlog.Printf("Received Beacon from replica %d with timestamp %d\n", beacon.Rid, beacon.Timestamp)
				megenericsmr.ReplyBeacon(r.Replica, beacon)
				break

			case <-slowClockChan:
				if r.Beacon {
					for q := int32(0); q < int32(r.N); q++ {
						if q == r.Id {
							continue
						}
						megenericsmr.SendBeacon(r.Replica, q)
					}
				}
				break
			}
		}else {
		    select {
			case prepareS := <-r.prepareChan:
				prepare := prepareS.(*mepaxosproto.Prepare)
				//got a Prepare message
				dlog.Printf("Received Prepare for instance %d.%d\n", prepare.Replica, prepare.Instance)
				r.handlePrepare(prepare)
				break

			case preAcceptS := <-r.preAcceptChan:
				preAccept := preAcceptS.(*mepaxosproto.PreAccept)
				//got a PreAccept message
				dlog.Printf("Received PreAccept for instance %d.%d\n", preAccept.LeaderId, preAccept.Instance)
				r.handlePreAccept(preAccept)
				break

			case acceptS := <-r.acceptChan:
				accept := acceptS.(*mepaxosproto.Accept)
				//got an Accept message
				dlog.Printf("Received Accept for instance %d.%d\n", accept.LeaderId, accept.Instance)
				r.handleAccept(accept)
				break

			case commitS := <-r.commitChan:
				commit := commitS.(*mepaxosproto.Commit)
				//got a Commit message
				dlog.Printf("Received Commit for instance %d.%d\n", commit.LeaderId, commit.Instance)
				r.handleCommit(commit)
				break

			case commitS := <-r.commitShortChan:
				commit := commitS.(*mepaxosproto.CommitShort)
				//got a Commit message
				dlog.Printf("Received Commit for instance %d.%d\n", commit.LeaderId, commit.Instance)
				r.handleCommitShort(commit)
				break

			case prepareReplyS := <-r.prepareReplyChan:
				prepareReply := prepareReplyS.(*mepaxosproto.PrepareReply)
				//got a Prepare reply
				dlog.Printf("Received PrepareReply for instance %d.%d\n", prepareReply.Replica, prepareReply.Instance)
				r.handlePrepareReply(prepareReply)
				break

			case preAcceptReplyS := <-r.preAcceptReplyChan:
				preAcceptReply := preAcceptReplyS.(*mepaxosproto.PreAcceptReply)
				//got a PreAccept reply
				dlog.Printf("Received PreAcceptReply for instance %d.%d\n", preAcceptReply.Replica, preAcceptReply.Instance)
				r.handlePreAcceptReply(preAcceptReply)
				break

			case preAcceptOKS := <-r.preAcceptOKChan:
				preAcceptOK := preAcceptOKS.(*mepaxosproto.PreAcceptOK)
				//got a PreAccept reply
				dlog.Printf("Received PreAcceptOK for instance %d.%d\n", r.Id, preAcceptOK.Instance)
				r.handlePreAcceptOK(preAcceptOK)
				break

			case acceptReplyS := <-r.acceptReplyChan:
				acceptReply := acceptReplyS.(*mepaxosproto.AcceptReply)
				//got an Accept reply
				dlog.Printf("Received AcceptReply for instance %d.%d\n", acceptReply.Replica, acceptReply.Instance)
				r.handleAcceptReply(acceptReply)
				break

			case tryPreAcceptS := <-r.tryPreAcceptChan:
				tryPreAccept := tryPreAcceptS.(*mepaxosproto.TryPreAccept)
				dlog.Printf("Received TryPreAccept for instance %d.%d\n", tryPreAccept.Replica, tryPreAccept.Instance)
				r.handleTryPreAccept(tryPreAccept)
				break

			case tryPreAcceptReplyS := <-r.tryPreAcceptReplyChan:
				tryPreAcceptReply := tryPreAcceptReplyS.(*mepaxosproto.TryPreAcceptReply)
				dlog.Printf("Received TryPreAcceptReply for instance %d.%d\n", tryPreAcceptReply.Replica, tryPreAcceptReply.Instance)
				r.handleTryPreAcceptReply(tryPreAcceptReply)
				break

			case beacon := <-r.BeaconChan:
				dlog.Printf("Received Beacon from replica %d with timestamp %d\n", beacon.Rid, beacon.Timestamp)
				megenericsmr.ReplyBeacon(r.Replica, beacon)
				break

			case <-slowClockChan:
				if r.Beacon {
					for q := int32(0); q < int32(r.N); q++ {
						if q == r.Id {
							continue
						}
						megenericsmr.SendBeacon(r.Replica, q)
					}
				}
				break
		//case <-r.OnClientConnect:
			//log.Printf("weird %d; conflicted %d; slow %d; happy %d\n", weird, conflicted, slow, happy)
			//weird, conflicted, slow, happy = 0, 0, 0, 0

			case iid := <-r.instancesToRecover:
				r.startRecoveryForInstance(iid.replica, iid.instance)
			//}
		//}else if r.DoPaxosState == 1 || len(r.PaxosProposeChan)!=0 || len(r.paxosprepareChan)!=0 || len(r.paxosacceptChan)!=0 || len(r.paxosdoneReplyChan)!=0 || len(r.paxoscommitChan)!=0 || len(r.paxoscommitShortChan)!=0 || len(r.paxosprepareReplyChan)!=0 || len(r.paxosacceptReplyChan)!=0{
			//select {

			/*case propose := <-onOffProposeChan:
				//got a Propose from a client
				dlog.Printf("Proposal with op %d\n", propose.Command.Op)
				dlog.Printf("Started to pass propose in Command %d\n", propose.CommandId)
				r.passPropose(propose) //do Paxos as a replica
				dlog.Printf("Success to pass propose\n\n")
				onOffProposeChan = nil
				break*/

			case paxosModePropose := <-paxosonOffProposeChan:
				if r.LeaderId == int(r.Id){
					dlog.Printf("Start handle paxos propose from replica %d in Command %d\n", paxosModePropose.ReplyForRepli, paxosModePropose.CommandId)
					r.handlePaxosPropose(paxosModePropose) //do Paxos as a leader
					dlog.Printf("Success handle paxos propose\n\n")
					//paxosonOffProposeChan = nil
				}
				break

			/*case <-isPassProposeChan:
				if r.DoPaxosState == 1 {
					onOffProposeChan = r.ProposeChan
				}
				break*/

			/*case <-fastClockChan:
				//activate new proposals channel
				paxosonOffProposeChan = r.PaxosProposeChan
				break*/

			case paxosprepareS := <-r.paxosprepareChan:
				paxosprepare := paxosprepareS.(*paxosproto.Prepare)
				//got a PaxosPrepare message
				dlog.Printf("Start prepare Paxos from replica %d, for instance %d\n", paxosprepare.LeaderId, paxosprepare.Instance)
				r.handlePaxosPrepare(paxosprepare)
				dlog.Printf("Success prepare Paxos\n\n")
				break

			case paxosacceptS := <-r.paxosacceptChan:
				paxosaccept := paxosacceptS.(*paxosproto.Accept)
				//got an Paxos Accept message
				dlog.Printf("Start accept Paxos Accept from replica %d, for instance %d\n", paxosaccept.LeaderId, paxosaccept.Instance)
				r.handlePaxosAccept(paxosaccept)
				dlog.Printf("Success accept Paxos\n\n")
				break

			
			case paxosdoneReplyS := <-r.paxosdoneReplyChan:
				paxosdoneReply := paxosdoneReplyS.(*megenericsmrproto.ProposeRepliReplyTS)
				//got reply from the leader to the replica which transmit client request
				dlog.Printf("Start send confirm to client in command %d\n", paxosdoneReply.CommandId)
				megenericsmr.ReplyProposeTS(
					r.Replica,
					&megenericsmrproto.ProposeReplyTS{
						paxosdoneReply.OK,
						paxosdoneReply.CommandId,
						paxosdoneReply.Value,
						paxosdoneReply.Timestamp},
					r.ClientConn[paxosdoneReply.ConnToCli])
				paxosNo ++
				dlog.Printf("Success send confirm to client\n\n")
				break

			case paxoscommitS := <-r.paxoscommitChan:
				paxoscommit := paxoscommitS.(*paxosproto.Commit)
				//got a Paxos Commit message
				dlog.Printf("Start commit Paxos from replica %d, for instance %d\n", paxoscommit.LeaderId, paxoscommit.Instance)
				r.handlePaxosCommit(paxoscommit)
				dlog.Printf("Success commit Paxos\n\n")
				break

			case paxoscommitS := <-r.paxoscommitShortChan:
				paxoscommit := paxoscommitS.(*paxosproto.CommitShort)
				//got a Paxos Commit message
				dlog.Printf("Start commit Paxos from replica %d, for instance %d\n", paxoscommit.LeaderId, paxoscommit.Instance)
				r.handlePaxosCommitShort(paxoscommit)
				dlog.Printf("Success commit Paxos\n\n")
				break

			case paxosprepareReplyS := <-r.paxosprepareReplyChan:
				paxosprepareReply := paxosprepareReplyS.(*paxosproto.PrepareReply)
				//got a Paxos Prepare reply
				dlog.Printf("Start handle Paxos PrepareReply for instance %d\n", paxosprepareReply.Instance)
				r.handlePaxosPrepareReply(paxosprepareReply)
				dlog.Printf("Success handle Paxos prepare reply\n\n")
				break

			case paxosacceptReplyS := <-r.paxosacceptReplyChan:
				paxosacceptReply := paxosacceptReplyS.(*paxosproto.AcceptReply)
				//got a Paxos Accept reply
				dlog.Printf("Start handle Paxos accept reply for instance %d\n", paxosacceptReply.Instance)
				r.handlePaxosAcceptReply(paxosacceptReply)
				dlog.Printf("Success handle Paxos accept reply\n\n")
				break

			case beacon := <-r.BeaconChan:
				dlog.Printf("Received Beacon from replica %d with timestamp %d\n", beacon.Rid, beacon.Timestamp)
				megenericsmr.ReplyBeacon(r.Replica, beacon)
				break

			case <-slowClockChan:
				if r.Beacon {
					for q := int32(0); q < int32(r.N); q++ {
						if q == r.Id {
							continue
						}
						megenericsmr.SendBeacon(r.Replica, q)
					}
				}
				break
			}
		}
	}
}

/***********************************
    Paxos related                  *
************************************/

//append a log entry to stable storage
func (r *Replica) paxosrecordInstanceMetadata(inst *PaxosInstance) {
	if !r.Durable {
		return
	}

	var b [5]byte
	binary.LittleEndian.PutUint32(b[0:4], uint32(inst.ballot))
	b[4] = byte(inst.status)
	r.StableStore.Write(b[:])
}

func (r *Replica) passPropose(propose *megenericsmr.Propose) {
	if r.LeaderId == int(r.Id) {
		r.PaxosProposeChan <- &megenericsmr.PaxosPropose{propose.Propose, propose.Reply, -1}
	}else {
		args := megenericsmrproto.RepliPropose{propose.CommandId, propose.Command, propose.Timestamp, propose.Reply}
		r.PeerWriters[r.LeaderId].WriteByte(megenericsmrproto.PAXOSPROPOSE)
		args.Marshal(r.PeerWriters[r.LeaderId])
		r.PeerWriters[r.LeaderId].Flush()
	}
}

func (r *Replica) updateCommittedUpTo() {
	for r.paxosInstanceSpace[r.paxoscommittedUpTo+1] != nil &&
		r.paxosInstanceSpace[r.paxoscommittedUpTo+1].status == COMMITTED {
		r.paxoscommittedUpTo++
	}
}

func (r *Replica) makeUniqueBallot(ballot int32) int32 {
	return (ballot << 4) | r.Id
}

func (r *Replica) paxosreplyPrepare(replicaId int32, reply *paxosproto.PrepareReply) {
        megenericsmr.SendMsg(r.Replica, replicaId, r.paxosprepareReplyRPC, reply)
}

func (r *Replica) paxosreplyAccept(replicaId int32, reply *paxosproto.AcceptReply) {
	megenericsmr.SendMsg(r.Replica, replicaId, r.paxosacceptReplyRPC, reply)
}

func (r *Replica) paxosbcastPrepare(instance int32, ballot int32, toInfinity bool) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Prepare bcast failed:", err)
		}
	}()
	ti := FALSE
	if toInfinity {
		ti = TRUE
	}
	args := &paxosproto.Prepare{r.Id, instance, ballot, ti}

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}
	
	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		sent++
		megenericsmr.SendMsg(r.Replica, r.PreferredPeerOrder[q], r.paxosprepareRPC, args)
		if sent >= n {
			break
		}
	}
}

var paxospa paxosproto.Accept

func (r *Replica) paxosbcastAccept(instance int32, ballot int32, command []state.Command) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Accept bcast failed:", err)
		}
	}()
	paxospa.LeaderId = r.Id
	paxospa.Instance = instance
	paxospa.Ballot = ballot
	paxospa.Command = command
	args := &paxospa
	//args := &paxosproto.Accept{r.Id, instance, ballot, command}

	n := r.N - 1
	if r.Thrifty {
		n = r.N >> 1
	}

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		sent++
		megenericsmr.SendMsg(r.Replica, r.PreferredPeerOrder[q], r.paxosacceptRPC, args)
		if sent >= n {
			break
		}
	}
}

var pc paxosproto.Commit
var pcs paxosproto.CommitShort

func (r *Replica) paxosbcastCommit(instance int32, ballot int32, command []state.Command) {
	defer func() {
		if err := recover(); err != nil {
			log.Println("Commit bcast failed:", err)
		}
	}()
	pc.LeaderId = r.Id
	pc.Instance = instance
	pc.Ballot = ballot
	pc.Command = command
	args := &pc
	pcs.LeaderId = r.Id
	pcs.Instance = instance
	pcs.Ballot = ballot
	pcs.Count = int32(len(command))
	argsShort := &pcs

	//args := &paxosproto.Commit{r.Id, instance, command}

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		if r.Thrifty && sent >= r.N/2 {
			megenericsmr.SendMsg(r.Replica, r.PreferredPeerOrder[q], r.paxoscommitRPC, args)
		} else {
			megenericsmr.SendMsg(r.Replica, r.PreferredPeerOrder[q], r.paxoscommitShortRPC, argsShort)
			sent++
		}
	}
}

func (r *Replica) handlePaxosPropose(propose *megenericsmr.PaxosPropose) {
	for r.paxosInstanceSpace[r.paxoscrtInstance] != nil {
		r.paxoscrtInstance++
	}

	instNo := r.paxoscrtInstance
	r.paxoscrtInstance++

	batchSize := 1//len(r.PaxosProposeChan) + 1
	/*if batchSize > MAX_BATCH {
		batchSize = MAX_BATCH
	}*/

	cmds := make([]state.Command, batchSize)
	proposals := make([]*megenericsmr.PaxosPropose, batchSize)
	cmds[0] = propose.Command
	proposals[0] = propose

	/*for i := 1; i < batchSize; i++ {
		prop := <-r.PaxosProposeChan
		cmds[i] = prop.Command
		proposals[i] = prop
	}*/

	if r.defaultBallot == -1 {
		dlog.Printf("Handle paxos propose through the r.defaultBallot == -1,will send prepare message\n")
		r.paxosInstanceSpace[instNo] = &PaxosInstance{
			cmds,
			r.makeUniqueBallot(0),
			PREPARING,
			&PaxosLeaderBookkeeping{proposals, 0, 0, 0, 0}}
		r.paxosbcastPrepare(instNo, r.makeUniqueBallot(0), true)
		dlog.Printf("Classic round for instance %d\n", instNo)
	} else {
		dlog.Printf("Handle paxos propose through the r.defaultBallot != -1,will send accept message\n")
		r.paxosInstanceSpace[instNo] = &PaxosInstance{
			cmds,
			r.defaultBallot,
			PREPARED,
			&PaxosLeaderBookkeeping{proposals, 0, 0, 0, 0}}

		r.paxosrecordInstanceMetadata(r.paxosInstanceSpace[instNo])
		r.recordCommands(cmds)
		r.sync()

		r.paxosbcastAccept(instNo, r.defaultBallot, cmds)
		dlog.Printf("Fast round for instance %d\n", instNo)
	}
}

func (r *Replica) handlePaxosPrepare(prepare *paxosproto.Prepare) {
	inst := r.paxosInstanceSpace[prepare.Instance]
	var preply *paxosproto.PrepareReply

	if inst == nil {
		ok := TRUE
		if r.defaultBallot > prepare.Ballot {
			ok = FALSE
		}
		dlog.Printf("Handle paxos prepare through the inst == nil,ok is %v\n", ok)
		preply = &paxosproto.PrepareReply{prepare.Instance, ok, r.defaultBallot, make([]state.Command, 0)}
	} else {
		ok := TRUE
		if prepare.Ballot < inst.ballot {
			ok = FALSE
		}
		dlog.Printf("Handle paxos prepare through the inst != nil,ok is %v\n", ok)
		preply = &paxosproto.PrepareReply{prepare.Instance, ok, inst.ballot, inst.cmds}
	}

	r.paxosreplyPrepare(prepare.LeaderId, preply)

	if prepare.ToInfinity == TRUE && prepare.Ballot > r.defaultBallot {
		dlog.Printf("Set the defaultBallot\n")
		r.defaultBallot = prepare.Ballot
	}
}

func (r *Replica) handlePaxosAccept(accept *paxosproto.Accept) {
	inst := r.paxosInstanceSpace[accept.Instance]
	var areply *paxosproto.AcceptReply

	if inst == nil {
		dlog.Printf("The inst == nil\n")
		if accept.Ballot < r.defaultBallot {
			dlog.Printf("The accept.Ballot < r.defaultBallot and return false\n")
			areply = &paxosproto.AcceptReply{accept.Instance, FALSE, r.defaultBallot}
		} else {
			dlog.Printf("The accept.Ballot >= r.defaultBallot and update instance state\n")
			r.paxosInstanceSpace[accept.Instance] = &PaxosInstance{
				accept.Command,
				accept.Ballot,
				ACCEPTED,
				nil}
			areply = &paxosproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}
		}
	} else if inst.ballot > accept.Ballot {
		dlog.Printf("inst != nil and inst.ballot > accept.Ballot and return false\n")
		areply = &paxosproto.AcceptReply{accept.Instance, FALSE, inst.ballot}
	} else if inst.ballot < accept.Ballot {
		dlog.Printf("inst != nil and inst.ballot < accept.Ballot and update instance state\n")
		inst.cmds = accept.Command
		inst.ballot = accept.Ballot
		inst.status = ACCEPTED
		areply = &paxosproto.AcceptReply{accept.Instance, TRUE, inst.ballot}
		if inst.lb != nil && inst.lb.clientProposals != nil {
			//TODO: is this correct?
			// try the proposal in a different instance
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.PaxosProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	} else {
		dlog.Printf("inst != nil and inst.ballot = accept.Ballot and update instance state\n")
		// reordered ACCEPT
		r.paxosInstanceSpace[accept.Instance].cmds = accept.Command
		if r.paxosInstanceSpace[accept.Instance].status != COMMITTED {
			r.paxosInstanceSpace[accept.Instance].status = ACCEPTED
		}
		areply = &paxosproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}
	}

	if areply.OK == TRUE {
		r.paxosrecordInstanceMetadata(r.paxosInstanceSpace[accept.Instance])
		r.recordCommands(accept.Command)
		r.sync()
	}

	r.paxosreplyAccept(accept.LeaderId, areply)
}

func (r *Replica) handlePaxosCommit(commit *paxosproto.Commit) {
	inst := r.paxosInstanceSpace[commit.Instance]

	dlog.Printf("Committing instance %d\n", commit.Instance)

	if inst == nil {
		dlog.Printf("inst == nil\n")
		r.paxosInstanceSpace[commit.Instance] = &PaxosInstance{
			commit.Command,
			commit.Ballot,
			COMMITTED,
			nil}
	} else {
		dlog.Printf("inst != nil\n")
		r.paxosInstanceSpace[commit.Instance].cmds = commit.Command
		r.paxosInstanceSpace[commit.Instance].status = COMMITTED
		r.paxosInstanceSpace[commit.Instance].ballot = commit.Ballot
		if inst.lb != nil && inst.lb.clientProposals != nil {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.PaxosProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	}

	r.updateCommittedUpTo()

	r.paxosrecordInstanceMetadata(r.paxosInstanceSpace[commit.Instance])
	r.recordCommands(commit.Command)
}

func (r *Replica) handlePaxosCommitShort(commit *paxosproto.CommitShort) {
	inst := r.paxosInstanceSpace[commit.Instance]

	dlog.Printf("Short Committing instance %d\n", commit.Instance)

	if inst == nil {
		dlog.Printf("inst == nil\n")
		r.paxosInstanceSpace[commit.Instance] = &PaxosInstance{nil,
			commit.Ballot,
			COMMITTED,
			nil}
	} else {
		dlog.Printf("inst != nil\n")
		r.paxosInstanceSpace[commit.Instance].status = COMMITTED
		r.paxosInstanceSpace[commit.Instance].ballot = commit.Ballot
		if inst.lb != nil && inst.lb.clientProposals != nil {
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				r.PaxosProposeChan <- inst.lb.clientProposals[i]
			}
			inst.lb.clientProposals = nil
		}
	}

	r.updateCommittedUpTo()

	r.paxosrecordInstanceMetadata(r.paxosInstanceSpace[commit.Instance])
}

func (r *Replica) handlePaxosPrepareReply(preply *paxosproto.PrepareReply) {
	inst := r.paxosInstanceSpace[preply.Instance]

	if inst == nil || inst.status != PREPARING {
		dlog.Printf("The inst.status != PREPARING\n")
		// TODO: should replies for non-current ballots be ignored?
		// we've moved on -- these are delayed replies, so just ignore
		return
	}

	if preply.OK == TRUE {
		inst.lb.prepareOKs++
		dlog.Printf("The preply.OK == TRUE\n")
		if preply.Ballot > inst.lb.maxRecvBallot {
			dlog.Printf("The preply.Ballot > inst.lb.maxRecvBallot\n")
			inst.cmds = preply.Command
			inst.lb.maxRecvBallot = preply.Ballot
			if inst.lb.clientProposals != nil {
				// there is already a competing command for this instance,
				// so we put the client proposal back in the queue so that
				// we know to try it in another instance
				for i := 0; i < len(inst.lb.clientProposals); i++ {
					r.PaxosProposeChan <- inst.lb.clientProposals[i]
				}
				inst.lb.clientProposals = nil
			}
		}

		if inst.lb.prepareOKs+1 > r.N>>1 {
			dlog.Printf("inst.lb.prepareOKs+1 > r.N>>1\n")
			inst.status = PREPARED
			inst.lb.nacks = 0
			if inst.ballot > r.defaultBallot {
				dlog.Printf("update leader r.defaultBallot\n")
				r.defaultBallot = inst.ballot
			}
			r.paxosrecordInstanceMetadata(r.paxosInstanceSpace[preply.Instance])
			r.sync()
			r.paxosbcastAccept(preply.Instance, inst.ballot, inst.cmds)
		}
	} else {
		dlog.Printf("The preply.OK != TRUE\n")
		// TODO: there is probably another active leader
		inst.lb.nacks++
		if preply.Ballot > inst.lb.maxRecvBallot {
			dlog.Printf("The preply.Ballot > inst.lb.maxRecvBallot\n")
			inst.lb.maxRecvBallot = preply.Ballot
		}
		if inst.lb.nacks >= r.N>>1 {
			dlog.Printf("inst.lb.nacks >= r.N>>1\n")
			if inst.lb.clientProposals != nil {
				// try the proposals in another instance
				for i := 0; i < len(inst.lb.clientProposals); i++ {
					r.PaxosProposeChan <- inst.lb.clientProposals[i]
				}
				inst.lb.clientProposals = nil
			}
		}
	}
}

func (r *Replica) handlePaxosAcceptReply(areply *paxosproto.AcceptReply) {
	inst := r.paxosInstanceSpace[areply.Instance]

	if inst == nil || inst.status != PREPARED && inst.status != ACCEPTED {
		dlog.Printf("inst.status != PREPARED && inst.status != ACCEPTED\n")
		// we've move on, these are delayed replies, so just ignore
		return
	}

	if areply.OK == TRUE {
		dlog.Printf("areply.OK == TRUE\n")
		inst.lb.acceptOKs++
		if inst.lb.acceptOKs+1 > r.N>>1 {
			inst = r.paxosInstanceSpace[areply.Instance]
			dlog.Printf("inst.lb.acceptOKs+1 > r.N>>1, commit instance and send msg to replica to inform client\n")
			inst.status = COMMITTED
			if inst.lb.clientProposals != nil && !r.Dreply {
				// give client the all clear
				for i := 0; i < len(inst.cmds); i++ {
					propreply := &megenericsmrproto.ProposeRepliReplyTS{
						TRUE,
						inst.lb.clientProposals[i].CommandId,
						state.NIL,
						inst.lb.clientProposals[i].Timestamp,
					    inst.lb.clientProposals[i].Reply}
					if inst.lb.clientProposals[i].ReplyForRepli == -1 {
						r.repliPassCommand[r.Id][inst.lb.clientProposals[i].Command.K]++
						r.paxosdoneReplyChan <- propreply
					}else {
						r.repliPassCommand[inst.lb.clientProposals[i].ReplyForRepli][inst.lb.clientProposals[i].Command.K]++
						megenericsmr.SendMsg(r.Replica, int32(inst.lb.clientProposals[i].ReplyForRepli), r.paxosdoneReplyRPC, propreply)
					}
				}
			}

			r.paxosrecordInstanceMetadata(r.paxosInstanceSpace[areply.Instance])
			r.sync() //is this necessary?

			r.updateCommittedUpTo()

			r.paxosbcastCommit(areply.Instance, inst.ballot, inst.cmds)
		}
	} else {
		dlog.Printf("areply.OK != TRUE\n")
		// TODO: there is probably another active leader
		inst.lb.nacks++
		if areply.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = areply.Ballot
		}
		if inst.lb.nacks >= r.N>>1 {
			// TODO
		}
	}
}


/***********************************
   Command execution thread        *
************************************/

func (r *Replica) executeCommands() {
	const SLEEP_TIME_NS = 1e6
	problemInstance := make([]int32, r.N)
	timeout := make([]uint64, r.N)
	for q := 0; q < r.N; q++ {
		problemInstance[q] = -1
		timeout[q] = 0
	}
	i := int32(0)

	for !r.Shutdown {
		executed := false
		flag := true
		for k:=0; k<r.N; k++ {
			if r.ExecedUpTo[k] < r.maxnum[k]{
				flag = false
			}
		}
		if r.DoPaxosState == 1 && flag{
			for i <= r.paxoscommittedUpTo {
				if r.paxosInstanceSpace[i].cmds != nil {
					inst := r.paxosInstanceSpace[i]
					for j := 0; j < len(inst.cmds); j++ {
						val := inst.cmds[j].Execute(r.State)
						if r.Dreply && inst.lb != nil && inst.lb.clientProposals != nil {
							propreply := &megenericsmrproto.ProposeRepliReplyTS{
								TRUE,
								inst.lb.clientProposals[i].CommandId,
								val,
								inst.lb.clientProposals[i].Timestamp,
								inst.lb.clientProposals[i].Reply}
							if inst.lb.clientProposals[i].ReplyForRepli == -1 {
								r.paxosdoneReplyChan <- propreply
							}else {
								megenericsmr.SendMsg(r.Replica, int32(inst.lb.clientProposals[i].ReplyForRepli), r.paxosdoneReplyRPC, propreply)
							}
						}
					}
					i++
					executed = true
				} else {
					break
				}
			}
			if !executed {
				time.Sleep(1000 * 1000)
			}
		}else{
			for q := 0; q < r.N; q++ {
				inst := int32(0)
				for inst = r.ExecedUpTo[q] + 1; inst < r.crtInstance[q]; inst++ {
					if r.InstanceSpace[q][inst] != nil && r.InstanceSpace[q][inst].Status == mepaxosproto.EXECUTED {
						if inst == r.ExecedUpTo[q]+1 {
							r.ExecedUpTo[q] = inst
						}
						continue
					}
					if r.InstanceSpace[q][inst] == nil || r.InstanceSpace[q][inst].Status != mepaxosproto.COMMITTED {
						if inst == problemInstance[q] {
							timeout[q] += SLEEP_TIME_NS
							if timeout[q] >= COMMIT_GRACE_PERIOD {
								r.instancesToRecover <- &instanceId{int32(q), inst}
								timeout[q] = 0
							}
						} else {
							problemInstance[q] = inst
							timeout[q] = 0
						}
						if r.InstanceSpace[q][inst] == nil {
							continue
						}
						break
					}
					if ok := r.exec.executeCommand(int32(q), inst); ok {
						executed = true
						if inst == r.ExecedUpTo[q]+1 {
							r.ExecedUpTo[q] = inst
						}
					}
				}
			}
			if !executed {
				time.Sleep(SLEEP_TIME_NS)
			}
			//log.Println(r.ExecedUpTo, " ", r.crtInstance)
		}
	}
}

/* Ballot helper functions */

//func (r *Replica) makeUniqueBallot(ballot int32) int32 {
	//return (ballot << 4) | r.Id
//}

func (r *Replica) makeBallotLargerThan(ballot int32) int32 {
	return r.makeUniqueBallot((ballot >> 4) + 1)
}

func isInitialBallot(ballot int32) bool {
	return (ballot >> 4) == 0
}

func replicaIdFromBallot(ballot int32) int32 {
	return ballot & 15
}

/**********************************************************************
                    inter-replica communication
***********************************************************************/

func (r *Replica) replyPrepare(replicaId int32, reply *mepaxosproto.PrepareReply) {
	megenericsmr.SendMsg(r.Replica, replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyPreAccept(replicaId int32, reply *mepaxosproto.PreAcceptReply) {
	megenericsmr.SendMsg(r.Replica, replicaId, r.preAcceptReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *mepaxosproto.AcceptReply) {
	megenericsmr.SendMsg(r.Replica, replicaId, r.acceptReplyRPC, reply)
}

func (r *Replica) replyTryPreAccept(replicaId int32, reply *mepaxosproto.TryPreAcceptReply) {
	megenericsmr.SendMsg(r.Replica, replicaId, r.tryPreAcceptReplyRPC, reply)
}

func (r *Replica) bcastPrepare(replica int32, instance int32, ballot int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Prepare bcast failed:", err)
		}
	}()
	args := &mepaxosproto.Prepare{r.Id, replica, instance, ballot}

	n := r.N - 1
	if r.Thrifty {
		n = r.N / 2
	}
	q := r.Id
	for sent := 0; sent < n; {
		q = (q + 1) % int32(r.N)
		if q == r.Id {
			dlog.Println("Not enough replicas alive!")
			break
		}
		if !r.Alive[q] {
			continue
		}
		megenericsmr.SendMsg(r.Replica, q, r.prepareRPC, args)
		sent++
	}
}

var pa mepaxosproto.PreAccept

func (r *Replica) bcastPreAccept(replica int32, instance int32, ballot int32, cmds []state.Command, seq int32, deps [DS]int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("PreAccept bcast failed:", err)
		}
	}()
	pa.LeaderId = r.Id
	pa.Replica = replica
	pa.Instance = instance
	pa.Ballot = ballot
	pa.Command = cmds
	pa.Seq = seq
	pa.Deps = deps
	args := &pa

	n := r.N - 1
	if r.Thrifty {
		n = r.N / 2
	}

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		megenericsmr.SendMsg(r.Replica, r.PreferredPeerOrder[q], r.preAcceptRPC, args)
		sent++
		if sent >= n {
			break
		}
	}
}

var tpa mepaxosproto.TryPreAccept

func (r *Replica) bcastTryPreAccept(replica int32, instance int32, ballot int32, cmds []state.Command, seq int32, deps [DS]int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("PreAccept bcast failed:", err)
		}
	}()
	tpa.LeaderId = r.Id
	tpa.Replica = replica
	tpa.Instance = instance
	tpa.Ballot = ballot
	tpa.Command = cmds
	tpa.Seq = seq
	tpa.Deps = deps
	args := &pa

	for q := int32(0); q < int32(r.N); q++ {
		if q == r.Id {
			continue
		}
		if !r.Alive[q] {
			continue
		}
		megenericsmr.SendMsg(r.Replica, q, r.tryPreAcceptRPC, args)
	}
}

var ea mepaxosproto.Accept

func (r *Replica) bcastAccept(replica int32, instance int32, ballot int32, count int32, seq int32, deps [DS]int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Accept bcast failed:", err)
		}
	}()

	ea.LeaderId = r.Id
	ea.Replica = replica
	ea.Instance = instance
	ea.Ballot = ballot
	ea.Count = count
	ea.Seq = seq
	ea.Deps = deps
	args := &ea

	n := r.N - 1
	if r.Thrifty {
		n = r.N / 2
	}

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		megenericsmr.SendMsg(r.Replica, r.PreferredPeerOrder[q], r.acceptRPC, args)
		sent++
		if sent >= n {
			break
		}
	}
}

var ec mepaxosproto.Commit
var ecs mepaxosproto.CommitShort

func (r *Replica) bcastCommit(replica int32, instance int32, cmds []state.Command, seq int32, deps [DS]int32) {
	defer func() {
		if err := recover(); err != nil {
			dlog.Println("Commit bcast failed:", err)
		}
	}()
	ec.LeaderId = r.Id
	ec.Replica = replica
	ec.Instance = instance
	ec.Command = cmds
	ec.Seq = seq
	ec.Deps = deps
	args := &ec
	ecs.LeaderId = r.Id
	ecs.Replica = replica
	ecs.Instance = instance
	ecs.Count = int32(len(cmds))
	ecs.Seq = seq
	ecs.Deps = deps
	argsShort := &ecs

	sent := 0
	for q := 0; q < r.N-1; q++ {
		if !r.Alive[r.PreferredPeerOrder[q]] {
			continue
		}
		if r.Thrifty && sent >= r.N/2 {
			megenericsmr.SendMsg(r.Replica, r.PreferredPeerOrder[q], r.commitRPC, args)
		} else {
			megenericsmr.SendMsg(r.Replica, r.PreferredPeerOrder[q], r.commitShortRPC, argsShort)
			sent++
		}
	}
}

/******************************************************************
               Helper functions
*******************************************************************/

func (r *Replica) clearHashtables() {
	for q := 0; q < r.N; q++ {
		r.conflicts[q] = make(map[state.Key]int32, HT_INIT_SIZE)
	}
}

func (r *Replica) updateCommitted(replica int32) {
	for r.InstanceSpace[replica][r.CommittedUpTo[replica]+1] != nil &&
		(r.InstanceSpace[replica][r.CommittedUpTo[replica]+1].Status == mepaxosproto.COMMITTED ||
			r.InstanceSpace[replica][r.CommittedUpTo[replica]+1].Status == mepaxosproto.EXECUTED) {
		r.CommittedUpTo[replica] = r.CommittedUpTo[replica] + 1
	}
}

func (r *Replica) updateConflicts(cmds []state.Command, replica int32, instance int32, seq int32) {
	for i := 0; i < len(cmds); i++ {
		if d, present := r.conflicts[replica][cmds[i].K]; present {
			if d < instance {
				r.conflicts[replica][cmds[i].K] = instance
			}
		} else {
            r.conflicts[replica][cmds[i].K] = instance
        }
		if s, present := r.maxSeqPerKey[cmds[i].K]; present {
			if s < seq {
				r.maxSeqPerKey[cmds[i].K] = seq
			}
		} else {
			r.maxSeqPerKey[cmds[i].K] = seq
		}
	}
}

func (r *Replica) updateAttributes(cmds []state.Command, seq int32, deps [DS]int32, replica int32, instance int32) (int32, [DS]int32, bool) {
	changed := false
	for q := 0; q < r.N; q++ {
		if r.Id != replica && int32(q) == replica {
			continue
		}
		for i := 0; i < len(cmds); i++ {
			if d, present := (r.conflicts[q])[cmds[i].K]; present {
				if d > deps[q] {
					deps[q] = d
					if seq <= r.InstanceSpace[q][d].Seq {
						seq = r.InstanceSpace[q][d].Seq + 1
					}
					changed = true
					break
				}
			}
		}
	}
	for i := 0; i < len(cmds); i++ {
		if s, present := r.maxSeqPerKey[cmds[i].K]; present {
			if seq <= s {
				changed = true
				seq = s + 1
			}
		}
	}

	return seq, deps, changed
}

func (r *Replica) mergeAttributes(seq1 int32, deps1 [DS]int32, seq2 int32, deps2 [DS]int32) (int32, [DS]int32, bool) {
	equal := true
	if seq1 != seq2 {
		equal = false
		if seq2 > seq1 {
			seq1 = seq2
		}
	}
	for q := 0; q < r.N; q++ {
		if int32(q) == r.Id {
			continue
		}
		if deps1[q] != deps2[q] {
			equal = false
			if deps2[q] > deps1[q] {
				deps1[q] = deps2[q]
			}
		}
	}
	return seq1, deps1, equal
}

func equal(deps1 *[DS]int32, deps2 *[DS]int32) bool {
	for i := 0; i < len(deps1); i++ {
		if deps1[i] != deps2[i] {
			return false
		}
	}
	return true
}

func bfFromCommands(cmds []state.Command) *bloomfilter.Bloomfilter {
	if cmds == nil {
		return nil
	}

	bf := bloomfilter.NewPowTwo(bf_PT, BF_K)

	for i := 0; i < len(cmds); i++ {
		bf.AddUint64(uint64(cmds[i].K))
	}

	return bf
}

/**********************************************************************

                            PHASE 1

***********************************************************************/

func (r *Replica) handlePropose(propose *megenericsmr.Propose) {
	//TODO!! Handle client retries

	batchSize := 1//len(r.ProposeChan) + 1
	/*if batchSize > MAX_BATCH {
		batchSize = MAX_BATCH
	}*/

	instNo := r.crtInstance[r.Id]
	r.crtInstance[r.Id]++

	dlog.Printf("Starting instance %d\n", instNo)
	dlog.Printf("Batching %d\n", batchSize)

	cmds := make([]state.Command, batchSize)
	proposals := make([]*megenericsmr.Propose, batchSize)
	cmds[0] = propose.Command
	proposals[0] = propose
	/*for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan
		cmds[i] = prop.Command
		proposals[i] = prop
	}*/

	r.startPhase1(r.Id, instNo, 0, proposals, cmds, batchSize)
}

func (r *Replica) startPhase1(replica int32, instance int32, ballot int32, proposals []*megenericsmr.Propose, cmds []state.Command, batchSize int) {
	//init command attributes

	seq := int32(0)
	var deps [DS]int32
	for q := 0; q < r.N; q++ {
		deps[q] = -1
	}

	seq, deps, _ = r.updateAttributes(cmds, seq, deps, replica, instance)

	r.InstanceSpace[r.Id][instance] = &Instance{
		cmds,
		ballot,
		mepaxosproto.PREACCEPTED,
		seq,
		deps,
		&LeaderBookkeeping{proposals, 0, 0, true, 0, 0, 0, deps, []int32{-1, -1, -1, -1, -1}, nil, false, false, nil, 0}, 0, 0,
		nil}

	r.updateConflicts(cmds, r.Id, instance, seq)

	if seq >= r.maxSeq {
		r.maxSeq = seq + 1
	}

	r.recordInstanceMetadata(r.InstanceSpace[r.Id][instance])
	r.recordCommands(cmds)
	r.sync()

	r.bcastPreAccept(r.Id, instance, ballot, cmds, seq, deps)

	cpcounter += batchSize

	if r.Id == 0 && DO_CHECKPOINTING && cpcounter >= CHECKPOINT_PERIOD {
		cpcounter = 0

		//Propose a checkpoint command to act like a barrier.
		//This allows replicas to discard their dependency hashtables.
		r.crtInstance[r.Id]++
		instance++

		r.maxSeq++
		for q := 0; q < r.N; q++ {
			deps[q] = r.crtInstance[q] - 1
		}

		r.InstanceSpace[r.Id][instance] = &Instance{
			cpMarker,
			0,
			mepaxosproto.PREACCEPTED,
			r.maxSeq,
			deps,
			&LeaderBookkeeping{nil, 0, 0, true, 0, 0, 0, deps, nil, nil, false, false, nil, 0},
			0,
			0,
			nil}

		r.latestCPReplica = r.Id
		r.latestCPInstance = instance

		//discard dependency hashtables
		r.clearHashtables()

		r.recordInstanceMetadata(r.InstanceSpace[r.Id][instance])
		r.sync()

		r.bcastPreAccept(r.Id, instance, 0, cpMarker, r.maxSeq, deps)
	}
}

func (r *Replica) handlePreAccept(preAccept *mepaxosproto.PreAccept) {
	inst := r.InstanceSpace[preAccept.LeaderId][preAccept.Instance]

	if preAccept.Seq >= r.maxSeq {
		r.maxSeq = preAccept.Seq + 1
	}

	if inst != nil && (inst.Status == mepaxosproto.COMMITTED || inst.Status == mepaxosproto.ACCEPTED) {
		//reordered handling of commit/accept and pre-accept
		if inst.Cmds == nil {
			r.InstanceSpace[preAccept.LeaderId][preAccept.Instance].Cmds = preAccept.Command
			r.updateConflicts(preAccept.Command, preAccept.Replica, preAccept.Instance, preAccept.Seq)
			//r.InstanceSpace[preAccept.LeaderId][preAccept.Instance].bfilter = bfFromCommands(preAccept.Command)
		}
		r.recordCommands(preAccept.Command)
		r.sync()
		return
	}

	if preAccept.Instance >= r.crtInstance[preAccept.Replica] {
		r.crtInstance[preAccept.Replica] = preAccept.Instance + 1
	}

	//update attributes for command
	seq, deps, changed := r.updateAttributes(preAccept.Command, preAccept.Seq, preAccept.Deps, preAccept.Replica, preAccept.Instance)
	uncommittedDeps := false
	for q := 0; q < r.N; q++ {
		if deps[q] > r.CommittedUpTo[q] {
			uncommittedDeps = true
			break
		}
	}
	status := mepaxosproto.PREACCEPTED_EQ
	if changed {
		status = mepaxosproto.PREACCEPTED
	}

	if inst != nil {
		if preAccept.Ballot < inst.ballot {
			r.replyPreAccept(preAccept.LeaderId,
				&mepaxosproto.PreAcceptReply{
					preAccept.Replica,
					preAccept.Instance,
					FALSE,
					inst.ballot,
					inst.Seq,
					inst.Deps,
					r.CommittedUpTo})
			return
		} else {
			inst.Cmds = preAccept.Command
			inst.Seq = seq
			inst.Deps = deps
			inst.ballot = preAccept.Ballot
			inst.Status = status
		}
	} else {
		r.InstanceSpace[preAccept.Replica][preAccept.Instance] = &Instance{
			preAccept.Command,
			preAccept.Ballot,
			status,
			seq,
			deps,
			nil, 0, 0,
			nil}
	}

	r.updateConflicts(preAccept.Command, preAccept.Replica, preAccept.Instance, preAccept.Seq)

	r.recordInstanceMetadata(r.InstanceSpace[preAccept.Replica][preAccept.Instance])
	r.recordCommands(preAccept.Command)
	r.sync()

	if len(preAccept.Command) == 0 {
		//checkpoint
		//update latest checkpoint info
		r.latestCPReplica = preAccept.Replica
		r.latestCPInstance = preAccept.Instance

		//discard dependency hashtables
		r.clearHashtables()
	}

	if changed || uncommittedDeps || preAccept.Replica != preAccept.LeaderId || !isInitialBallot(preAccept.Ballot) {
		r.replyPreAccept(preAccept.LeaderId,
			&mepaxosproto.PreAcceptReply{
				preAccept.Replica,
				preAccept.Instance,
				TRUE,
				preAccept.Ballot,
				seq,
				deps,
				r.CommittedUpTo})
	} else {
		pok := &mepaxosproto.PreAcceptOK{preAccept.Instance}
		megenericsmr.SendMsg(r.Replica, preAccept.LeaderId, r.preAcceptOKRPC, pok)
	}

	dlog.Printf("I've replied to the PreAccept\n")
}

func (r *Replica) handlePreAcceptReply(pareply *mepaxosproto.PreAcceptReply) {
	dlog.Printf("Handling PreAccept reply\n")
	inst := r.InstanceSpace[pareply.Replica][pareply.Instance]

	if inst.Status != mepaxosproto.PREACCEPTED {
		// we've moved on, this is a delayed reply
		return
	}

	if inst.ballot != pareply.Ballot {
		return
	}

	if pareply.OK == FALSE {
		// TODO: there is probably another active leader
		inst.lb.nacks++
		if pareply.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = pareply.Ballot
		}
		if inst.lb.nacks >= r.N/2 {
			// TODO
		}
		return
	}

	inst.lb.preAcceptOKs++

	var equal bool
	inst.Seq, inst.Deps, equal = r.mergeAttributes(inst.Seq, inst.Deps, pareply.Seq, pareply.Deps)
	if (r.N <= 3 && !r.Thrifty) || inst.lb.preAcceptOKs > 1 {
		inst.lb.allEqual = inst.lb.allEqual && equal
		if !equal {
			conflicted++
		}
	}

	/*allCommitted := true
	for q := 0; q < r.N; q++ {
		if inst.lb.committedDeps[q] < pareply.CommittedDeps[q] {
			inst.lb.committedDeps[q] = pareply.CommittedDeps[q]
		}
		if inst.lb.committedDeps[q] < r.CommittedUpTo[q] {
			inst.lb.committedDeps[q] = r.CommittedUpTo[q]
		}
		if inst.lb.committedDeps[q] < inst.Deps[q] {
			allCommitted = false
		}
	}*/

	//can we commit on the fast path?
	if inst.lb.preAcceptOKs >= r.N/2 && inst.lb.allEqual && isInitialBallot(inst.ballot) {
		happy++
		dlog.Printf("Fast path for instance %d.%d\n", pareply.Replica, pareply.Instance)
		r.InstanceSpace[pareply.Replica][pareply.Instance].Status = mepaxosproto.COMMITTED
		r.updateCommitted(pareply.Replica)
		if inst.lb.clientProposals != nil && !r.Dreply {
			// give clients the all clear
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				megenericsmr.ReplyProposeTS(
                                        r.Replica,
					&megenericsmrproto.ProposeReplyTS{
						TRUE,
						inst.lb.clientProposals[i].CommandId,
						state.NIL,
						inst.lb.clientProposals[i].Timestamp},
					r.ClientConn[inst.lb.clientProposals[i].Reply])
			}
		}

		r.recordInstanceMetadata(inst)
		r.sync() //is this necessary here?

		r.bcastCommit(pareply.Replica, pareply.Instance, inst.Cmds, inst.Seq, inst.Deps)
	} else if inst.lb.preAcceptOKs >= r.N/2 {
		/*if !allCommitted {
			weird++
		}*/
		slow++
		inst.Status = mepaxosproto.ACCEPTED
		r.bcastAccept(pareply.Replica, pareply.Instance, inst.ballot, int32(len(inst.Cmds)), inst.Seq, inst.Deps)
	}
	//TODO: take the slow path if messages are slow to arrive
}

func (r *Replica) handlePreAcceptOK(pareply *mepaxosproto.PreAcceptOK) {
	dlog.Printf("Handling PreAccept reply\n")
	inst := r.InstanceSpace[r.Id][pareply.Instance]

	if inst.Status != mepaxosproto.PREACCEPTED {
		// we've moved on, this is a delayed reply
		return
	}

	if !isInitialBallot(inst.ballot) {
		return
	}

	inst.lb.preAcceptOKs++

	/*allCommitted := true
	for q := 0; q < r.N; q++ {
		if inst.lb.committedDeps[q] < inst.lb.originalDeps[q] {
			inst.lb.committedDeps[q] = inst.lb.originalDeps[q]
		}
		if inst.lb.committedDeps[q] < r.CommittedUpTo[q] {
			inst.lb.committedDeps[q] = r.CommittedUpTo[q]
		}
		if inst.lb.committedDeps[q] < inst.Deps[q] {
			allCommitted = false
		}
	}*/

	//can we commit on the fast path?
	if inst.lb.preAcceptOKs >= r.N/2 && inst.lb.allEqual && isInitialBallot(inst.ballot) {
		happy++
		r.InstanceSpace[r.Id][pareply.Instance].Status = mepaxosproto.COMMITTED
		r.updateCommitted(r.Id)
		if inst.lb.clientProposals != nil && !r.Dreply {
			// give clients the all clear
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				megenericsmr.ReplyProposeTS(
                                        r.Replica,
					&megenericsmrproto.ProposeReplyTS{
						TRUE,
						inst.lb.clientProposals[i].CommandId,
						state.NIL,
						inst.lb.clientProposals[i].Timestamp},
					r.ClientConn[inst.lb.clientProposals[i].Reply])
			}
		}

		r.recordInstanceMetadata(inst)
		r.sync() //is this necessary here?

		r.bcastCommit(r.Id, pareply.Instance, inst.Cmds, inst.Seq, inst.Deps)
	} else if inst.lb.preAcceptOKs >= r.N/2 {
		/*if !allCommitted {
			weird++
		}*/
		slow++
		inst.Status = mepaxosproto.ACCEPTED
		r.bcastAccept(r.Id, pareply.Instance, inst.ballot, int32(len(inst.Cmds)), inst.Seq, inst.Deps)
	}
	//TODO: take the slow path if messages are slow to arrive
}

/**********************************************************************

                        PHASE 2

***********************************************************************/

func (r *Replica) handleAccept(accept *mepaxosproto.Accept) {
	inst := r.InstanceSpace[accept.LeaderId][accept.Instance]

	if accept.Seq >= r.maxSeq {
		r.maxSeq = accept.Seq + 1
	}

	if inst != nil && (inst.Status == mepaxosproto.COMMITTED || inst.Status == mepaxosproto.EXECUTED) {
		return
	}

	if accept.Instance >= r.crtInstance[accept.LeaderId] {
		r.crtInstance[accept.LeaderId] = accept.Instance + 1
	}

	if inst != nil {
		if accept.Ballot < inst.ballot {
			r.replyAccept(accept.LeaderId, &mepaxosproto.AcceptReply{accept.Replica, accept.Instance, FALSE, inst.ballot})
			return
		}
		inst.Status = mepaxosproto.ACCEPTED
		inst.Seq = accept.Seq
		inst.Deps = accept.Deps
	} else {
		r.InstanceSpace[accept.LeaderId][accept.Instance] = &Instance{
			nil,
			accept.Ballot,
			mepaxosproto.ACCEPTED,
			accept.Seq,
			accept.Deps,
			nil, 0, 0, nil}

		if accept.Count == 0 {
			//checkpoint
			//update latest checkpoint info
			r.latestCPReplica = accept.Replica
			r.latestCPInstance = accept.Instance

			//discard dependency hashtables
			r.clearHashtables()
		}
	}

	r.recordInstanceMetadata(r.InstanceSpace[accept.Replica][accept.Instance])
	r.sync()

	r.replyAccept(accept.LeaderId,
		&mepaxosproto.AcceptReply{
			accept.Replica,
			accept.Instance,
			TRUE,
			accept.Ballot})
}

func (r *Replica) handleAcceptReply(areply *mepaxosproto.AcceptReply) {
	inst := r.InstanceSpace[areply.Replica][areply.Instance]

	if inst.Status != mepaxosproto.ACCEPTED {
		// we've move on, these are delayed replies, so just ignore
		return
	}

	if inst.ballot != areply.Ballot {
		return
	}

	if areply.OK == FALSE {
		// TODO: there is probably another active leader
		inst.lb.nacks++
		if areply.Ballot > inst.lb.maxRecvBallot {
			inst.lb.maxRecvBallot = areply.Ballot
		}
		if inst.lb.nacks >= r.N/2 {
			// TODO
		}
		return
	}

	inst.lb.acceptOKs++

	if inst.lb.acceptOKs+1 > r.N/2 {
		r.InstanceSpace[areply.Replica][areply.Instance].Status = mepaxosproto.COMMITTED
		r.updateCommitted(areply.Replica)
		if inst.lb.clientProposals != nil && !r.Dreply {
			// give clients the all clear
			for i := 0; i < len(inst.lb.clientProposals); i++ {
				megenericsmr.ReplyProposeTS(
                                        r.Replica,
					&megenericsmrproto.ProposeReplyTS{
						TRUE,
						inst.lb.clientProposals[i].CommandId,
						state.NIL,
						inst.lb.clientProposals[i].Timestamp},
					r.ClientConn[inst.lb.clientProposals[i].Reply])
			}
		}

		r.recordInstanceMetadata(inst)
		r.sync() //is this necessary here?

		r.bcastCommit(areply.Replica, areply.Instance, inst.Cmds, inst.Seq, inst.Deps)
	}
}

/**********************************************************************

                            COMMIT

***********************************************************************/

func (r *Replica) handleCommit(commit *mepaxosproto.Commit) {
	inst := r.InstanceSpace[commit.Replica][commit.Instance]

	if commit.Seq >= r.maxSeq {
		r.maxSeq = commit.Seq + 1
	}

	if commit.Instance >= r.crtInstance[commit.Replica] {
		r.crtInstance[commit.Replica] = commit.Instance + 1
	}

	if inst != nil {
		if inst.lb != nil && inst.lb.clientProposals != nil && len(commit.Command) == 0 {
			//someone committed a NO-OP, but we have proposals for this instance
			//try in a different instance
			for _, p := range inst.lb.clientProposals {
				r.ProposeChan <- p
			}
			inst.lb = nil
		}
		inst.Seq = commit.Seq
		inst.Deps = commit.Deps
		inst.Status = mepaxosproto.COMMITTED
	} else {
		r.InstanceSpace[commit.Replica][int(commit.Instance)] = &Instance{
			commit.Command,
			0,
			mepaxosproto.COMMITTED,
			commit.Seq,
			commit.Deps,
			nil,
			0,
			0,
			nil}
		r.updateConflicts(commit.Command, commit.Replica, commit.Instance, commit.Seq)

		if len(commit.Command) == 0 {
			//checkpoint
			//update latest checkpoint info
			r.latestCPReplica = commit.Replica
			r.latestCPInstance = commit.Instance

			//discard dependency hashtables
			r.clearHashtables()
		}
	}
	r.updateCommitted(commit.Replica)

	r.recordInstanceMetadata(r.InstanceSpace[commit.Replica][commit.Instance])
	r.recordCommands(commit.Command)
}

func (r *Replica) handleCommitShort(commit *mepaxosproto.CommitShort) {
	inst := r.InstanceSpace[commit.Replica][commit.Instance]

	if commit.Instance >= r.crtInstance[commit.Replica] {
		r.crtInstance[commit.Replica] = commit.Instance + 1
	}

	if inst != nil {
		if inst.lb != nil && inst.lb.clientProposals != nil {
			//try command in a different instance
			for _, p := range inst.lb.clientProposals {
				r.ProposeChan <- p
			}
			inst.lb = nil
		}
		inst.Seq = commit.Seq
		inst.Deps = commit.Deps
		inst.Status = mepaxosproto.COMMITTED
	} else {
		r.InstanceSpace[commit.Replica][commit.Instance] = &Instance{
			nil,
			0,
			mepaxosproto.COMMITTED,
			commit.Seq,
			commit.Deps,
			nil, 0, 0, nil}

		if commit.Count == 0 {
			//checkpoint
			//update latest checkpoint info
			r.latestCPReplica = commit.Replica
			r.latestCPInstance = commit.Instance

			//discard dependency hashtables
			r.clearHashtables()
		}
	}
	r.updateCommitted(commit.Replica)

	r.recordInstanceMetadata(r.InstanceSpace[commit.Replica][commit.Instance])
}

/**********************************************************************

                      RECOVERY ACTIONS

***********************************************************************/

func (r *Replica) startRecoveryForInstance(replica int32, instance int32) {
	var nildeps [DS]int32

	if r.InstanceSpace[replica][instance] == nil {
		r.InstanceSpace[replica][instance] = &Instance{nil, 0, mepaxosproto.NONE, 0, nildeps, nil, 0, 0, nil}
	}

	inst := r.InstanceSpace[replica][instance]
	if inst.lb == nil {
		inst.lb = &LeaderBookkeeping{nil, -1, 0, false, 0, 0, 0, nildeps, nil, nil, true, false, nil, 0}

	} else {
		inst.lb = &LeaderBookkeeping{inst.lb.clientProposals, -1, 0, false, 0, 0, 0, nildeps, nil, nil, true, false, nil, 0}
	}

	if inst.Status == mepaxosproto.ACCEPTED {
		inst.lb.recoveryInst = &RecoveryInstance{inst.Cmds, inst.Status, inst.Seq, inst.Deps, 0, false}
		inst.lb.maxRecvBallot = inst.ballot
	} else if inst.Status >= mepaxosproto.PREACCEPTED {
		inst.lb.recoveryInst = &RecoveryInstance{inst.Cmds, inst.Status, inst.Seq, inst.Deps, 1, (r.Id == replica)}
	}

	//compute larger ballot
	inst.ballot = r.makeBallotLargerThan(inst.ballot)

	r.bcastPrepare(replica, instance, inst.ballot)
}

func (r *Replica) handlePrepare(prepare *mepaxosproto.Prepare) {
	inst := r.InstanceSpace[prepare.Replica][prepare.Instance]
	var preply *mepaxosproto.PrepareReply
	var nildeps [DS]int32

	if inst == nil {
		r.InstanceSpace[prepare.Replica][prepare.Instance] = &Instance{
			nil,
			prepare.Ballot,
			mepaxosproto.NONE,
			0,
			nildeps,
			nil, 0, 0, nil}
		preply = &mepaxosproto.PrepareReply{
			r.Id,
			prepare.Replica,
			prepare.Instance,
			TRUE,
			-1,
			mepaxosproto.NONE,
			nil,
			-1,
			nildeps}
	} else {
		ok := TRUE
		if prepare.Ballot < inst.ballot {
			ok = FALSE
		} else {
			inst.ballot = prepare.Ballot
		}
		preply = &mepaxosproto.PrepareReply{
			r.Id,
			prepare.Replica,
			prepare.Instance,
			ok,
			inst.ballot,
			inst.Status,
			inst.Cmds,
			inst.Seq,
			inst.Deps}
	}

	r.replyPrepare(prepare.LeaderId, preply)
}

func (r *Replica) handlePrepareReply(preply *mepaxosproto.PrepareReply) {
	inst := r.InstanceSpace[preply.Replica][preply.Instance]

	if inst.lb == nil || !inst.lb.preparing {
		// we've moved on -- these are delayed replies, so just ignore
		// TODO: should replies for non-current ballots be ignored?
		return
	}

	if preply.OK == FALSE {
		// TODO: there is probably another active leader, back off and retry later
		inst.lb.nacks++
		return
	}

	//Got an ACK (preply.OK == TRUE)

	inst.lb.prepareOKs++

	if preply.Status == mepaxosproto.COMMITTED || preply.Status == mepaxosproto.EXECUTED {
		r.InstanceSpace[preply.Replica][preply.Instance] = &Instance{
			preply.Command,
			inst.ballot,
			mepaxosproto.COMMITTED,
			preply.Seq,
			preply.Deps,
			nil, 0, 0, nil}
		r.bcastCommit(preply.Replica, preply.Instance, inst.Cmds, preply.Seq, preply.Deps)
		//TODO: check if we should send notifications to clients
		return
	}

	if preply.Status == mepaxosproto.ACCEPTED {
		if inst.lb.recoveryInst == nil || inst.lb.maxRecvBallot < preply.Ballot {
			inst.lb.recoveryInst = &RecoveryInstance{preply.Command, preply.Status, preply.Seq, preply.Deps, 0, false}
			inst.lb.maxRecvBallot = preply.Ballot
		}
	}

	if (preply.Status == mepaxosproto.PREACCEPTED || preply.Status == mepaxosproto.PREACCEPTED_EQ) &&
		(inst.lb.recoveryInst == nil || inst.lb.recoveryInst.status < mepaxosproto.ACCEPTED) {
		if inst.lb.recoveryInst == nil {
			inst.lb.recoveryInst = &RecoveryInstance{preply.Command, preply.Status, preply.Seq, preply.Deps, 1, false}
		} else if preply.Seq == inst.Seq && equal(&preply.Deps, &inst.Deps) {
			inst.lb.recoveryInst.preAcceptCount++
		} else if preply.Status == mepaxosproto.PREACCEPTED_EQ {
			// If we get different ordering attributes from pre-acceptors, we must go with the ones
			// that agreed with the initial command leader (in case we do not use Thrifty).
			// This is safe if we use thrifty, although we can also safely start phase 1 in that case.
			inst.lb.recoveryInst = &RecoveryInstance{preply.Command, preply.Status, preply.Seq, preply.Deps, 1, false}
		}
		if preply.AcceptorId == preply.Replica {
			//if the reply is from the initial command leader, then it's safe to restart phase 1
			inst.lb.recoveryInst.leaderResponded = true
			return
		}
	}

	if inst.lb.prepareOKs < r.N/2 {
		return
	}

	//Received Prepare replies from a majority

	ir := inst.lb.recoveryInst

	if ir != nil {
		//at least one replica has (pre-)accepted this instance
		if ir.status == mepaxosproto.ACCEPTED ||
			(!ir.leaderResponded && ir.preAcceptCount >= r.N/2 && (r.Thrifty || ir.status == mepaxosproto.PREACCEPTED_EQ)) {
			//safe to go to Accept phase
			inst.Cmds = ir.cmds
			inst.Seq = ir.seq
			inst.Deps = ir.deps
			inst.Status = mepaxosproto.ACCEPTED
			inst.lb.preparing = false
			r.bcastAccept(preply.Replica, preply.Instance, inst.ballot, int32(len(inst.Cmds)), inst.Seq, inst.Deps)
		} else if !ir.leaderResponded && ir.preAcceptCount >= (r.N/2+1)/2 {
			//send TryPreAccepts
			//but first try to pre-accept on the local replica
			inst.lb.preAcceptOKs = 0
			inst.lb.nacks = 0
			inst.lb.possibleQuorum = make([]bool, r.N)
			for q := 0; q < r.N; q++ {
				inst.lb.possibleQuorum[q] = true
			}
			if conf, q, i := r.findPreAcceptConflicts(ir.cmds, preply.Replica, preply.Instance, ir.seq, ir.deps); conf {
				if r.InstanceSpace[q][i].Status >= mepaxosproto.COMMITTED {
					//start Phase1 in the initial leader's instance
					r.startPhase1(preply.Replica, preply.Instance, inst.ballot, inst.lb.clientProposals, ir.cmds, len(ir.cmds))
					return
				} else {
					inst.lb.nacks = 1
					inst.lb.possibleQuorum[r.Id] = false
				}
			} else {
				inst.Cmds = ir.cmds
				inst.Seq = ir.seq
				inst.Deps = ir.deps
				inst.Status = mepaxosproto.PREACCEPTED
				inst.lb.preAcceptOKs = 1
			}
			inst.lb.preparing = false
			inst.lb.tryingToPreAccept = true
			r.bcastTryPreAccept(preply.Replica, preply.Instance, inst.ballot, inst.Cmds, inst.Seq, inst.Deps)
		} else {
			//start Phase1 in the initial leader's instance
			inst.lb.preparing = false
			r.startPhase1(preply.Replica, preply.Instance, inst.ballot, inst.lb.clientProposals, ir.cmds, len(ir.cmds))
		}
	} else {
		//try to finalize instance by proposing NO-OP
		var noop_deps [DS]int32
		// commands that depended on this instance must look at all previous instances
		noop_deps[preply.Replica] = preply.Instance - 1
		inst.lb.preparing = false
		r.InstanceSpace[preply.Replica][preply.Instance] = &Instance{
			nil,
			inst.ballot,
			mepaxosproto.ACCEPTED,
			0,
			noop_deps,
			inst.lb, 0, 0, nil}
		r.bcastAccept(preply.Replica, preply.Instance, inst.ballot, 0, 0, noop_deps)
	}
}

func (r *Replica) handleTryPreAccept(tpa *mepaxosproto.TryPreAccept) {
	inst := r.InstanceSpace[tpa.Replica][tpa.Instance]
	if inst != nil && inst.ballot > tpa.Ballot {
		// ballot number too small
		r.replyTryPreAccept(tpa.LeaderId, &mepaxosproto.TryPreAcceptReply{
			r.Id,
			tpa.Replica,
			tpa.Instance,
			FALSE,
			inst.ballot,
			tpa.Replica,
			tpa.Instance,
			inst.Status})
	}
	if conflict, confRep, confInst := r.findPreAcceptConflicts(tpa.Command, tpa.Replica, tpa.Instance, tpa.Seq, tpa.Deps); conflict {
		// there is a conflict, can't pre-accept
		r.replyTryPreAccept(tpa.LeaderId, &mepaxosproto.TryPreAcceptReply{
			r.Id,
			tpa.Replica,
			tpa.Instance,
			FALSE,
			inst.ballot,
			confRep,
			confInst,
			r.InstanceSpace[confRep][confInst].Status})
	} else {
		// can pre-accept
		if tpa.Instance >= r.crtInstance[tpa.Replica] {
			r.crtInstance[tpa.Replica] = tpa.Instance + 1
		}
		if inst != nil {
			inst.Cmds = tpa.Command
			inst.Deps = tpa.Deps
			inst.Seq = tpa.Seq
			inst.Status = mepaxosproto.PREACCEPTED
			inst.ballot = tpa.Ballot
		} else {
			r.InstanceSpace[tpa.Replica][tpa.Instance] = &Instance{
				tpa.Command,
				tpa.Ballot,
				mepaxosproto.PREACCEPTED,
				tpa.Seq,
				tpa.Deps,
				nil, 0, 0,
				nil}
		}
		r.replyTryPreAccept(tpa.LeaderId, &mepaxosproto.TryPreAcceptReply{r.Id, tpa.Replica, tpa.Instance, TRUE, inst.ballot, 0, 0, 0})
	}
}

func (r *Replica) findPreAcceptConflicts(cmds []state.Command, replica int32, instance int32, seq int32, deps [DS]int32) (bool, int32, int32) {
	inst := r.InstanceSpace[replica][instance]
	if inst != nil && len(inst.Cmds) > 0 {
		if inst.Status >= mepaxosproto.ACCEPTED {
			// already ACCEPTED or COMMITTED
			// we consider this a conflict because we shouldn't regress to PRE-ACCEPTED
			return true, replica, instance
		}
		if inst.Seq == tpa.Seq && equal(&inst.Deps, &tpa.Deps) {
			// already PRE-ACCEPTED, no point looking for conflicts again
			return false, replica, instance
		}
	}
	for q := int32(0); q < int32(r.N); q++ {
		for i := r.ExecedUpTo[q]; i < r.crtInstance[q]; i++ {
			if replica == q && instance == i {
				// no point checking past instance in replica's row, since replica would have
				// set the dependencies correctly for anything started after instance
				break
			}
			if i == deps[q] {
				//the instance cannot be a dependency for itself
				continue
			}
			inst := r.InstanceSpace[q][i]
			if inst == nil || inst.Cmds == nil || len(inst.Cmds) == 0 {
				continue
			}
			if inst.Deps[replica] >= instance {
				// instance q.i depends on instance replica.instance, it is not a conflict
				continue
			}
			if state.ConflictBatch(inst.Cmds, cmds) {
				if i > deps[q] ||
					(i < deps[q] && inst.Seq >= seq && (q != replica || inst.Status > mepaxosproto.PREACCEPTED_EQ)) {
					// this is a conflict
					return true, q, i
				}
			}
		}
	}
	return false, -1, -1
}

func (r *Replica) handleTryPreAcceptReply(tpar *mepaxosproto.TryPreAcceptReply) {
	inst := r.InstanceSpace[tpar.Replica][tpar.Instance]
	if inst == nil || inst.lb == nil || !inst.lb.tryingToPreAccept || inst.lb.recoveryInst == nil {
		return
	}

	ir := inst.lb.recoveryInst

	if tpar.OK == TRUE {
		inst.lb.preAcceptOKs++
		inst.lb.tpaOKs++
		if inst.lb.preAcceptOKs >= r.N/2 {
			//it's safe to start Accept phase
			inst.Cmds = ir.cmds
			inst.Seq = ir.seq
			inst.Deps = ir.deps
			inst.Status = mepaxosproto.ACCEPTED
			inst.lb.tryingToPreAccept = false
			inst.lb.acceptOKs = 0
			r.bcastAccept(tpar.Replica, tpar.Instance, inst.ballot, int32(len(inst.Cmds)), inst.Seq, inst.Deps)
			return
		}
	} else {
		inst.lb.nacks++
		if tpar.Ballot > inst.ballot {
			//TODO: retry with higher ballot
			return
		}
		inst.lb.tpaOKs++
		if tpar.ConflictReplica == tpar.Replica && tpar.ConflictInstance == tpar.Instance {
			//TODO: re-run prepare
			inst.lb.tryingToPreAccept = false
			return
		}
		inst.lb.possibleQuorum[tpar.AcceptorId] = false
		inst.lb.possibleQuorum[tpar.ConflictReplica] = false
		notInQuorum := 0
		for q := 0; q < r.N; q++ {
			if !inst.lb.possibleQuorum[tpar.AcceptorId] {
				notInQuorum++
			}
		}
		if tpar.ConflictStatus >= mepaxosproto.COMMITTED || notInQuorum > r.N/2 {
			//abandon recovery, restart from phase 1
			inst.lb.tryingToPreAccept = false
			r.startPhase1(tpar.Replica, tpar.Instance, inst.ballot, inst.lb.clientProposals, ir.cmds, len(ir.cmds))
		}
		if notInQuorum == r.N/2 {
			//this is to prevent defer cycles
			if present, dq, _ := deferredByInstance(tpar.Replica, tpar.Instance); present {
				if inst.lb.possibleQuorum[dq] {
					//an instance whose leader must have been in this instance's quorum has been deferred for this instance => contradiction
					//abandon recovery, restart from phase 1
					inst.lb.tryingToPreAccept = false
					r.startPhase1(tpar.Replica, tpar.Instance, inst.ballot, inst.lb.clientProposals, ir.cmds, len(ir.cmds))
				}
			}
		}
		if inst.lb.tpaOKs >= r.N/2 {
			//defer recovery and update deferred information
			updateDeferred(tpar.Replica, tpar.Instance, tpar.ConflictReplica, tpar.ConflictInstance)
			inst.lb.tryingToPreAccept = false
		}
	}
}

//helper functions and structures to prevent defer cycles while recovering

var deferMap map[uint64]uint64 = make(map[uint64]uint64)

func updateDeferred(dr int32, di int32, r int32, i int32) {
	daux := (uint64(dr) << 32) | uint64(di)
	aux := (uint64(r) << 32) | uint64(i)
	deferMap[aux] = daux
}

func deferredByInstance(q int32, i int32) (bool, int32, int32) {
	aux := (uint64(q) << 32) | uint64(i)
	daux, present := deferMap[aux]
	if !present {
		return false, 0, 0
	}
	dq := int32(daux >> 32)
	di := int32(daux)
	return true, dq, di
}
