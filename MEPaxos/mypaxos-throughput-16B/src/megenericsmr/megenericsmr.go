package megenericsmr

import (
	"bufio"
	"encoding/binary"
	"fastrpc"
	"fmt"
	"megenericsmrproto"
	"io"
	"log"
	"net"
	"os"
	"rdtsc"
	"state"
	"time"
)

const CHAN_BUFFER_SIZE = 200000

type RPCPair struct {
	Obj  fastrpc.Serializable
	Chan chan fastrpc.Serializable
}

type Propose struct {
	*megenericsmrproto.Propose
	Reply int64
}

type PaxosPropose struct {
	*megenericsmrproto.Propose
	Reply int64
	ReplyForRepli int
}

type Beacon struct {
	Rid       int32
	Timestamp uint64
}

type Replica struct {
	N            int        // total number of replicas
	Id           int32      // the ID of the current replica
	PeerAddrList []string   // array with the IP:port address of every replica
	portnum      int
	Peers        []net.Conn // cache of connections to all other replicas
	PeerReaders  []*bufio.Reader
	PeerWriters  []*bufio.Writer
	Alive        []bool // connection status
	Listener     net.Listener

	State *state.State

	ProposeChan chan *Propose // channel for client proposals
	BeaconChan  chan *Beacon  // channel for beacons from peer replicas
	PaxosProposeChan chan *PaxosPropose //channel for replica Paxos

	Shutdown bool

	Thrifty bool // send only as many messages as strictly required?
	Exec    bool // execute commands?
	Dreply  bool // reply to client after command has been executed?
	Beacon  bool // send beacons to detect how fast are the other replicas?

	Durable     bool     // log to a stable store?
	StableStore *os.File // file support for the persistent log

	PreferredPeerOrder []int32 // replicas in the preferred order of communication

	rpcTable map[uint8]*RPCPair
	rpcCode  uint8

	Ewma []uint64
    TotalNumber []uint64

	OnClientConnect chan bool

	LeaderId int //change to Paxos concerned
	DoPaxosState int //o represent is ready to change the algorithm mode,1 represent is done to change the algorithm mode
	VersionId int //map to master version
	ClientConn []*bufio.Writer //store the client connection
	ClientNum int64 //the number of client that connect with this replica
}

func NewReplica(id int, peerAddrList []string, portnum int, thrifty bool, exec bool, dreply bool) *Replica {
	r := &Replica{
		len(peerAddrList),
		int32(id),
		peerAddrList,
		portnum,
		make([]net.Conn, len(peerAddrList)),
		make([]*bufio.Reader, len(peerAddrList)),
		make([]*bufio.Writer, len(peerAddrList)),
		make([]bool, len(peerAddrList)),
		nil,
		state.InitState(),
		make(chan *Propose, CHAN_BUFFER_SIZE),
		make(chan *Beacon, CHAN_BUFFER_SIZE),
		make(chan *PaxosPropose, CHAN_BUFFER_SIZE),
		false,
		thrifty,
		exec,
		dreply,
		false,
		false,
		nil,
		make([]int32, len(peerAddrList)),
		make(map[uint8]*RPCPair),
		megenericsmrproto.PAXOSPROPOSE + 1,
		make([]uint64, len(peerAddrList)),
        make([]uint64, len(peerAddrList)),
		make(chan bool, 100),
	    -1,
	    -1,
	    0,
	    make([]*bufio.Writer, CHAN_BUFFER_SIZE),
	    -1}

	var err error

	if r.StableStore, err = os.Create(fmt.Sprintf("stable-store-replica%d", r.Id)); err != nil {
		log.Fatal(err)
	}

	for i := 0; i < r.N; i++ {
		r.PreferredPeerOrder[i] = int32((int(r.Id) + 1 + i) % r.N)
		r.Ewma[i] = 0
        r.TotalNumber[i] = 0
	}

	return r
}

/* Client API */

func (r *Replica) Ping(args *megenericsmrproto.PingArgs, reply *megenericsmrproto.PingReply) error {
	return nil
}

func (r *Replica) BeTheLeader(args *megenericsmrproto.BeTheLeaderArgs, reply *megenericsmrproto.BeTheLeaderReply) error {
	return nil
}

/* ============= */

func ConnectToPeers(r *Replica) {
	var b [4]byte
	bs := b[:4]
	done := make(chan bool)

	go r.waitForPeerConnections(done)

	//connect to peers
	for i := 0; i < int(r.Id); i++ {
		for done := false; !done; {
			if conn, err := net.Dial("tcp", r.PeerAddrList[i]); err == nil {
				r.Peers[i] = conn
				done = true
			} else {
				time.Sleep(1e9)
			}
		}
		binary.LittleEndian.PutUint32(bs, uint32(r.Id))
		if _, err := r.Peers[i].Write(bs); err != nil {
			fmt.Println("Write id error:", err)
			continue
		}
		r.Alive[i] = true
		r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
		r.PeerWriters[i] = bufio.NewWriter(r.Peers[i])
	}
	<-done
	log.Printf("Replica id: %d. Done connecting to peers\n", r.Id)

	for rid, reader := range r.PeerReaders {
		if int32(rid) == r.Id {
			continue
		}
		go r.replicaListener(rid, reader)
	}
}

func ConnectToPeersNoListeners(r *Replica) {
	var b [4]byte
	bs := b[:4]
	done := make(chan bool)

	go r.waitForPeerConnections(done)

	//connect to peers
	for i := 0; i < int(r.Id); i++ {
		for done := false; !done; {
			if conn, err := net.Dial("tcp", r.PeerAddrList[i]); err == nil {
				r.Peers[i] = conn
				done = true
			} else {
				time.Sleep(1e9)
			}
		}
		binary.LittleEndian.PutUint32(bs, uint32(r.Id))
		if _, err := r.Peers[i].Write(bs); err != nil {
			fmt.Println("Write id error:", err)
			continue
		}
		r.Alive[i] = true
		r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
		r.PeerWriters[i] = bufio.NewWriter(r.Peers[i])
	}
	<-done
	log.Printf("Replica id: %d. Done connecting to peers\n", r.Id)
}

/* Peer (replica) connections dispatcher */
func (r *Replica) waitForPeerConnections(done chan bool) {
	var b [4]byte
	bs := b[:4]

	r.Listener, _ = net.Listen("tcp", fmt.Sprintf(":%d", r.portnum))
	for i := r.Id + 1; i < int32(r.N); i++ {
		conn, err := r.Listener.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
			continue
		}
		if _, err := io.ReadFull(conn, bs); err != nil {
			fmt.Println("Connection establish error:", err)
			continue
		}
		id := int32(binary.LittleEndian.Uint32(bs))
		r.Peers[id] = conn
		r.PeerReaders[id] = bufio.NewReader(conn)
		r.PeerWriters[id] = bufio.NewWriter(conn)
		r.Alive[id] = true
	}

	done <- true
}

/* Client connections dispatcher */
func WaitForClientConnections(r *Replica) {
	for !r.Shutdown {
		conn, err := r.Listener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		r.ClientNum++
		go r.clientListener(conn,r.ClientNum)

		r.OnClientConnect <- true
	}
}

func (r *Replica) replicaListener(rid int, reader *bufio.Reader) {
	var msgType uint8
	var err error = nil
	var gbeacon megenericsmrproto.Beacon
	var gbeaconReply megenericsmrproto.BeaconReply

	for err == nil && !r.Shutdown {

		if msgType, err = reader.ReadByte(); err != nil {
			break
		}

		switch uint8(msgType) {

		case megenericsmrproto.PAXOSPROPOSE:
			//log.Printf("Success receive the Replica Propose information\n")
			paxosprop := new(megenericsmrproto.RepliPropose)
			if err = paxosprop.Unmarshal(reader); err != nil {
				break
			}
			genepropose := new(megenericsmrproto.Propose)
			genepropose.CommandId = paxosprop.CommandId
			genepropose.Command = paxosprop.Command
			genepropose.Timestamp = paxosprop.Timestamp
			r.PaxosProposeChan <- &PaxosPropose{genepropose, paxosprop.ConnToCli, rid}
			//log.Printf("Success put the Replica Propose information to the PaxosProposeChan\n")
			break

		case megenericsmrproto.GENERIC_SMR_BEACON:
			if err = gbeacon.Unmarshal(reader); err != nil {
				break
			}
			beacon := &Beacon{int32(rid), gbeacon.Timestamp}
			r.BeaconChan <- beacon
			break

		case megenericsmrproto.GENERIC_SMR_BEACON_REPLY:
			if err = gbeaconReply.Unmarshal(reader); err != nil {
				break
			}
			//TODO: UPDATE STUFF
            r.TotalNumber[rid]++
			r.Ewma[rid] = r.Ewma[rid] + rdtsc.Cputicks() - gbeaconReply.Timestamp
			break

		default:
			if rpair, present := r.rpcTable[msgType]; present {
				obj := rpair.Obj.New()
				if err = obj.Unmarshal(reader); err != nil {
					break
				}
				rpair.Chan <- obj
			} else {
				log.Println("Error: received unknown message type")
			}
		}
	}
}

func (r *Replica) clientListener(conn net.Conn,i int64) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	var msgType byte //:= make([]byte, 1)
	var err error
	for !r.Shutdown && err == nil {

		if msgType, err = reader.ReadByte(); err != nil {
			break
		}

		switch uint8(msgType) {

		case megenericsmrproto.PROPOSE:
			prop := new(megenericsmrproto.Propose)
			if err = prop.Unmarshal(reader); err != nil {
				break
			}
			r.ClientConn[i] = writer
			r.ProposeChan <- &Propose{prop, i}
			break

		case megenericsmrproto.READ:
			read := new(megenericsmrproto.Read)
			if err = read.Unmarshal(reader); err != nil {
				break
			}
			//r.ReadChan <- read
			break

		case megenericsmrproto.PROPOSE_AND_READ:
			pr := new(megenericsmrproto.ProposeAndRead)
			if err = pr.Unmarshal(reader); err != nil {
				break
			}
			//r.ProposeAndReadChan <- pr
			break
		}
	}
	if err != nil && err != io.EOF {
		log.Println("Error when reading from client connection:", err)
	}
}

func RegisterRPC(r *Replica, msgObj fastrpc.Serializable, notify chan fastrpc.Serializable) uint8 {
	code := r.rpcCode
	r.rpcCode++
	r.rpcTable[code] = &RPCPair{msgObj, notify}
	return code
}

func SendMsg(r *Replica, peerId int32, code uint8, msg fastrpc.Serializable) {
	w := r.PeerWriters[peerId]
	w.WriteByte(code)
	msg.Marshal(w)
	w.Flush()
}

/*func SendRepliMsg(r *Replica, w *bufio.Writer, code uint8, msg fastrpc.Serializable) {
	w.WriteByte(code)
	msg.Marshal(w)
	w.Flush()
}*/

func SendMsgNoFlush(r *Replica, peerId int32, code uint8, msg fastrpc.Serializable) {
	w := r.PeerWriters[peerId]
	w.WriteByte(code)
	msg.Marshal(w)
}

func ReplyPropose(r *Replica, reply *megenericsmrproto.ProposeReply, w *bufio.Writer) {
	//r.clientMutex.Lock()
	//defer r.clientMutex.Unlock()
	//w.WriteByte(megenericsmrproto.PROPOSE_REPLY)
	reply.Marshal(w)
	w.Flush()
}

func ReplyProposeTS(r *Replica, reply *megenericsmrproto.ProposeReplyTS, w *bufio.Writer) {
	//r.clientMutex.Lock()
	//defer r.clientMutex.Unlock()
	//w.WriteByte(megenericsmrproto.PROPOSE_REPLY)
	reply.Marshal(w)
	w.Flush()
}

func ReplyRepliProposeTS(r *Replica, reply *megenericsmrproto.ProposeRepliReplyTS, w *bufio.Writer) {
	//r.clientMutex.Lock()
	//defer r.clientMutex.Unlock()
	//w.WriteByte(megenericsmrproto.PROPOSE_REPLY)
	reply.Marshal(w)
	w.Flush()
}

func SendBeacon(r *Replica, peerId int32) {
	w := r.PeerWriters[peerId]
	w.WriteByte(megenericsmrproto.GENERIC_SMR_BEACON)
	beacon := &megenericsmrproto.Beacon{rdtsc.Cputicks()}
	beacon.Marshal(w)
	w.Flush()
}

func ReplyBeacon(r *Replica, beacon *Beacon) {
	w := r.PeerWriters[beacon.Rid]
	w.WriteByte(megenericsmrproto.GENERIC_SMR_BEACON_REPLY)
	rb := &megenericsmrproto.BeaconReply{beacon.Timestamp}
	rb.Marshal(w)
	w.Flush()
}

// updates the preferred order in which to communicate with peers according to a preferred quorum
func UpdatePreferredPeerOrder(r *Replica, quorum []int32) {
	aux := make([]int32, r.N)
	i := 0
	for _, p := range quorum {
		if p == r.Id {
			continue
		}
		aux[i] = p
		i++
	}

	for _, p := range r.PreferredPeerOrder {
		found := false
		for j := 0; j < i; j++ {
			if aux[j] == p {
				found = true
				break
			}
		}
		if !found {
			aux[i] = p
			i++
		}
	}

	r.PreferredPeerOrder = aux
}
