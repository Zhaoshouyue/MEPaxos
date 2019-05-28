package main

import (
	"flag"
	"fmt"
	"megenericsmrproto"
	"log"
	"masterproto"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
	"errors"
	"math"
)

var portnum *int = flag.Int("port", 7087, "Port # to listen on. Defaults to 7087")
var numNodes *int = flag.Int("N", 5, "Number of replicas. Defaults to 5.")

//added by Luna
var paxos bool = false //the master judge if the current state is Paxos by "paxos"
var paxosState bool = true
var eLatency []float64 = make([]float64, *numNodes) //each replica's commands' total latency in EPaxos mode
var repliNumber []int = make([]int, *numNodes) // the number of requests that each replica has dealt with in EPaxos mode
var min_k []float64 = make([]float64, *numNodes) // store the min_k latency informtion of each Replica
var replicaToLeader [][]float64 = make([][]float64, *numNodes) //store the latency between replica
var isDeliver []bool = make([]bool, *numNodes) //Mark whether the replica deliver its information to master
var decideELatency float64 = 0 //the whole ELatency
var decidePLeader int = -1 //choose the leader of Paxos mode in EPaxos mode
var pLatency []float64 = make([]float64, *numNodes) //Estimate the Paxos Latency when each replica as a leader in EPaxos mode
var decidePLatency float64 = math.MaxFloat64 //the (samllest) Paxos latency
var myflag bool = true // use to judge if master receives all replica's information,if true, receives all,if false, not receive all
var eConflict []int = make([]int, *numNodes) // the max conflict of each Replica, which is predicted by leader

type doPaxos struct {
	leaderId    int //replica which will be as a leader of Paxos mode
	version     int //this is the version time to prepare to go to Paxos mode
	//state       int //0 represent prepare, 1 represent propose, 2 represent Paxos mode
	sentToRepli int //to record the process of transform algorithm,normally 0, 1 represent should send prepare_paxos message to other replica，2 represent has sent prepare_paxos messages to other replica
	sendReq     int //the number of prepare-paxos (the first phase of transform algorithm) that master has sent 
	repliReply  int //received ack
	maxnum      []int32 //the max instance number of each replica deal with in EPaxos mode
	pmaxnum     int32 //the max instance number of each replica deal with in Paxos mode
}

type Master struct {
	N        int
	nodeList []string
	addrList []string
	portList []int
	lock     *sync.Mutex
	nodes    []*rpc.Client
	leader   []bool
	alive    []bool
	isPaxos  doPaxos//add by Luna
}

func main() {
	flag.Parse()

	log.Printf("Master starting on port %d\n", *portnum)
	log.Printf("...waiting for %d replicas\n", *numNodes)

	master := &Master{*numNodes,
		make([]string, 0, *numNodes),
		make([]string, 0, *numNodes),
		make([]int, 0, *numNodes),
		new(sync.Mutex),
		make([]*rpc.Client, *numNodes),
		make([]bool, *numNodes),
		make([]bool, *numNodes),
	    doPaxos{-1, 0, 0, 0, 0, make([]int32, *numNodes), -1}}

	rpc.Register(master)//log in RPC service,the default name is the type name
	rpc.HandleHTTP()//set the transport protocol,here is HTTP
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum))//listen to the local network address
	if err != nil {
		log.Fatal("Master listen error:", err)//printf error and exit
	}

	//add by Luna
	for i := 0; i < *numNodes; i++ {
		eLatency[i] = 0
		pLatency[i] = 0
		isDeliver[i] = false
		repliNumber[i] = 0
		replicaToLeader[i] = make([]float64, *numNodes)
	}

	go master.run()

	http.Serve(l, nil)
}

func (master *Master) run() {
	for true {
		master.lock.Lock()
		if len(master.nodeList) == master.N {
			master.lock.Unlock()
			break
		}
		master.lock.Unlock()
		time.Sleep(100000000)//0.1 second
	}
	time.Sleep(2000000000)//2 seconds

	// connect to SMR servers
	for i := 0; i < master.N; i++ {
		var err error
		addr := fmt.Sprintf("%s:%d", master.addrList[i], master.portList[i]+1000)
		master.nodes[i], err = rpc.DialHTTP("tcp", addr)//connect to replicas and doing RPC via master.nodes[i]
		if err != nil {
			log.Fatalf("Error connecting to replica %d\n", i)
		}
		master.leader[i] = false
	}
	master.leader[0] = true//replica 0 is the leader
	
	for true {
		//added by Luna
		if !paxos {//EPaxos状态

			if !paxosState {
				myflag = true
				//receiveRepli = 0
				decideELatency = 0
				decidePLatency = math.MaxFloat64
				decidePLeader = -1
				master.isPaxos.sentToRepli = 0
				master.isPaxos.leaderId = -1
				master.isPaxos.version = 0
				master.isPaxos.sendReq = 0
				master.isPaxos.repliReply = 0
				master.isPaxos.pmaxnum = -1
				for i := 0; i < master.N; i++ {
					isDeliver[i] = false
					eLatency[i] = 0
					pLatency[i] = 0
					repliNumber[i] = 0
					min_k[i] = 0
					master.isPaxos.maxnum[i] = -1
				}
				paxosState = true
			}

			//judge if master receives all replicas' information
			for j := 0; j < master.N; j++ {
				if !isDeliver[j] {
					myflag = false
					break
				}
			}

			//if master receives all replicas' information and the transform algorithm is not start
			if myflag && master.isPaxos.sentToRepli == 0{
				//Calculate EPaxos Latency and estimate Paxos latency
				for i := 0; i < master.N; i++ {
					decideELatency = decideELatency + eLatency[i]
					pLatency[i] = (1 + float64(repliNumber[i])) * min_k[i]//latency in the leader
					//latency in other replicas except the leader
					for j := 0; j < master.N; j++ {
						if i!=j {
							pLatency[i] = pLatency[i] + float64(repliNumber[j]) * (min_k[i] + replicaToLeader[i][j])
						}
					}
					if pLatency[i] < decidePLatency {
						decidePLatency = pLatency[i]
						decidePLeader = i
					}
				}

				//if EPaxos latency is larger than Paxos latency,change the master state and prepare to start transform algorithm
				if decideELatency > decidePLatency {
					log.Printf("decideELatency > decidePLatency，leader is %d.\n",decidePLeader)
					master.isPaxos.leaderId = decidePLeader
					master.isPaxos.version++
					//master.isPaxos.state = 0 //prepare do paxos
					master.isPaxos.sentToRepli = 1
				}

				//reset some variables
				for k := 0; k<master.N; k++ {
					eLatency[k] = 0
					pLatency[k] = 0
					isDeliver[k] = false
					repliNumber[k] = 0
				}

				decideELatency = 0
				decidePLatency = math.MaxFloat64
				decidePLeader = -1
			}

			myflag = true
			//receiveRepli = 0

			//send prepare-paxos message to other replica, do the first phase of transform algorithm
			if master.isPaxos.sentToRepli == 1 {
				//send message to the other replicas
				for i := 0; i < master.N; i++ {
					//tell the replica to prepare Paxos
					req := new(megenericsmrproto.PaxosPrepare)
					req.MasterVersion = master.isPaxos.version
					reply := new(megenericsmrproto.PaxosPrepareReply)
					master.isPaxos.sendReq++
					err := master.callt(master.nodes[i], "Replica.PaxosPrepare", req, reply)
					if err != nil {
						log.Printf("Error connecting to replica %d to prepare Paxos\n", i)
					}else if reply.RepliVersion >= req.MasterVersion {
						//log.Printf("master.isPaxos.leaderId is %d, req.LeaderId is %d, master.isPaxos.version is %d, req.MasterVersion is %d, reply.RepliVersion is %d\n",master.isPaxos.leaderId, req.LeaderId, master.isPaxos.version, req.MasterVersion, reply.RepliVersion)
						master.isPaxos.version = reply.RepliVersion + 1
						log.Printf("Replica %d has higher or equal version than master\n", i)
						break
					}else {
						log.Printf("Sucess to tell replica %d to prepare paxos\n", i)
						master.isPaxos.maxnum[i] = reply.Maxnumber //calculate result MS
						master.isPaxos.repliReply++
					}
				}
				master.isPaxos.sentToRepli = 2
			}

			//if all message sent has received ack, master send ack message
			if master.isPaxos.sentToRepli == 2 {
				if master.isPaxos.sendReq == master.isPaxos.repliReply && master.isPaxos.sendReq==master.N {
					log.Printf("All replica has prepared\n")
					//set the leader
					master.leader[master.isPaxos.leaderId] = true
					for j:=0; j<master.N; j++ {
						if j != master.isPaxos.leaderId {
							master.leader[j] = false
						}
					}
					for i:=0; i<master.N; i++ {
						//if master.alive[i] {
							req := new(megenericsmrproto.PaxosPropose)
							req.LeaderId = master.isPaxos.leaderId
							req.MasterVersion = master.isPaxos.version
							req.Maxnumber = master.isPaxos.maxnum
							reply := new(megenericsmrproto.PaxosProposeReply)
							err := master.callt(master.nodes[i], "Replica.PaxosPropose", req, reply)
							if err != nil {
								//master.alive[i] = false
								log.Printf("Error connecting to replica %d to go to Paxos mode\n", i)
							} else {
								log.Printf("Success connecting to replica %d to go to Paxos mode\n", i)
							}
						//}
					}
					log.Printf("All replica has gone into the Paxos mode,the leader is replica %d\n", master.isPaxos.leaderId)
					paxos = true
					//master.isPaxos.state = 2
				} else {
					log.Printf("There is replica failed to prepare, cancel paxos\n")
					for j := 0; j < master.N; j++ {
						//if master.alive[j] {
							req := new(megenericsmrproto.PaxosCancel)
							req.MasterVersion = master.isPaxos.version
							req.LeaderId = master.isPaxos.leaderId
							reply := new(megenericsmrproto.PaxosCancelReply)
							err := master.nodes[j].Call("Replica.CancelPaxos", req, reply)
							if err != nil {
								//master.alive[j] = false
								log.Printf("Error connecting to replica %d to cancel Paxos\n", j)
							} else if reply.LeaderId != req.LeaderId {
								if reply.RepliVersion > master.isPaxos.version {
									master.isPaxos.version = reply.RepliVersion + 1
								}
								log.Printf("Replica %d has higher version than master\n", j)
							} else {
								log.Printf("Success connecting to replica %d to cancel Paxos\n", j)
							}
						//}
					}
					master.isPaxos.leaderId = -1
					//master.isPaxos.state = -1
				}
				for k:=0; k<master.N; k++ {
					master.isPaxos.maxnum[k] = -1
				}
				master.isPaxos.sentToRepli = 0
				master.isPaxos.sendReq = 0
				master.isPaxos.repliReply = 0
			}
		} else {
			//Initiate state
			if paxosState {
				myflag = true
				//receiveRepli = 0
				decideELatency = 0
				decidePLatency = 0
				//master.isPaxos.leaderId = -1
				master.isPaxos.sentToRepli = 0
				master.isPaxos.sendReq = 0
				master.isPaxos.repliReply = 0
				master.isPaxos.version = 0
				master.isPaxos.pmaxnum = -1
				for i := 0; i < master.N; i++ {
					repliNumber[i] = 0
					eConflict[i] = 0
					isDeliver[i] = false
					min_k[i] = 0
					master.isPaxos.maxnum[i] = -1
				}
				paxosState = false
			}

			for j := 0; j < master.N; j++ {
				if !isDeliver[j] {
					myflag = false
					break
				}/*else {
					receiveRepli = receiveRepli + 1
				}*/
			}

			if myflag && master.isPaxos.sentToRepli == 0{
				//Calculate PLatency,ELatency
				decidePLatency = decidePLatency + min_k[master.isPaxos.leaderId]
				//log.Printf("min_k[master.isPaxos.leaderId] is %v, decidePLatency is %v\n", min_k[master.isPaxos.leaderId], decidePLatency)
				for i := 0; i < master.N; i++ {
					//if master.alive[i] {
						decideELatency = decideELatency + float64(repliNumber[i] + eConflict[i]) * min_k[i]
						//log.Printf("repliNumber[i] is %v, eConflict[i] is %v, min_k[i] is %v, decideELatency is %v\n", repliNumber[i], eConflict[i], min_k[i], decideELatency)
						decidePLatency = decidePLatency + (replicaToLeader[master.isPaxos.leaderId][i] + min_k[master.isPaxos.leaderId]) * float64(repliNumber[i])
						//log.Printf("replicaToLeader[master.isPaxos.leaderId][i] is %v, min_k[master.isPaxos.leaderId] is %v, repliNumber[master.isPaxos.leaderId] is %v, decidePLatency is %v\n", replicaToLeader[master.isPaxos.leaderId][i], min_k[master.isPaxos.leaderId], repliNumber[master.isPaxos.leaderId], decidePLatency)
					//}
				}

				//log.Printf("decideELatency is %v, decidePLatency is %v\n", decideELatency, decidePLatency)

				if decideELatency < decidePLatency {
					log.Printf("decideELatency < decidePLatency\n")
					//master.isPaxos.leaderId = decidePLeader
					master.isPaxos.version++
					//master.isPaxos.state = 0 //prepare do paxos
					master.isPaxos.sentToRepli = 1
				}

				for k := 0; k<master.N; k++ {
					eConflict[k] = 0
					min_k[k] = -1
					isDeliver[k] = false
					repliNumber[k] = 0
				}

				decideELatency = 0
				decidePLatency = 0
			}

			myflag = true
			//receiveRepli = 0

			//send prepare-paxos message to other replica
			if master.isPaxos.sentToRepli == 1 {
				//send message to the other replicas
				for i := 0; i < master.N; i++ {
					//if master.alive[i] {
					req := new(megenericsmrproto.EPaxosPrepare)
					req.MasterVersion = master.isPaxos.version
					reply := new(megenericsmrproto.EPaxosPrepareReply)
					master.isPaxos.sendReq++
					err := master.callt(master.nodes[i], "Replica.EPaxosPrepare", req, reply)
					if err != nil {
						log.Printf("Error connecting to replica %d to prepare Paxos\n", i)
					}else if reply.RepliVersion >= req.MasterVersion {
						master.isPaxos.version = reply.RepliVersion + 1
						log.Printf("Replica %d has higher or equal version than master\n", i)
						break
					}else {
						log.Printf("Sucess to tell replica %d to prepare EPaxos\n", i)
						master.isPaxos.repliReply++
						if i == master.isPaxos.leaderId {
							master.isPaxos.pmaxnum = reply.Maxnumber
						}
					}
					//}
				}
				master.isPaxos.sentToRepli = 2
			}

			//if all message sent has received ack, master send ack message
			if master.isPaxos.sentToRepli == 2 {
				if master.isPaxos.sendReq == master.isPaxos.repliReply && master.isPaxos.sendReq==master.N {
					log.Printf("All replica has prepared\n")
					//master.isPaxos.state = 1
					for i:=0; i<master.N; i++ {
						//if master.alive[i] {
							req := new(megenericsmrproto.EPaxosPropose)
							req.Maxnumber = master.isPaxos.pmaxnum
							req.MasterVersion = master.isPaxos.version
							reply := new(megenericsmrproto.EPaxosProposeReply)
							err := master.callt(master.nodes[i], "Replica.EPaxosPropose", req, reply)
							if err != nil {
								//master.alive[i] = false
								log.Printf("Error connecting to replica %d to go to EPaxos mode\n", i)
							} else {
								log.Printf("Success connecting to replica %d to go to EPaxos mode\n", i)
							}
						//}
					}
					log.Printf("All replica has gone into the EPaxos mode\n")
					paxos = false
					//master.isPaxos.state = -1
				} else {
					log.Printf("There is replica failed to prepare, cancel Epaxos\n")
					for j := 0; j < master.N; j++ {
						//if master.alive[j] {
							req := new(megenericsmrproto.EPaxosCancel)
							req.MasterVersion = master.isPaxos.version
							reply := new(megenericsmrproto.EPaxosCancelReply)
							err := master.nodes[j].Call("Replica.CancelEPaxos", req, reply)
							if err != nil {
								//master.alive[j] = false
								log.Printf("Error connecting to replica %d to cancel EPaxos\n", j)
							} else if reply.RepliVersion > req.MasterVersion {
								master.isPaxos.version = reply.RepliVersion + 1
								log.Printf("Replica %d has higher version than master\n", j)
							} else {
								log.Printf("Success connecting to replica %d to cancel EPaxos\n", j)
							}
						//}
					}
					//master.isPaxos.state = -1
				}
				master.isPaxos.pmaxnum = -1
				master.isPaxos.sentToRepli = 0
				master.isPaxos.sendReq = 0
				master.isPaxos.repliReply = 0
			}
		}

		time.Sleep(1000 * 1000 * 1000)//1 second
		new_leader := false
		for i, node := range master.nodes {//detect the failure of replicas
			err := node.Call("Replica.Ping", new(megenericsmrproto.PingArgs), new(megenericsmrproto.PingReply))
			if err != nil {
				//log.Printf("Replica %d has failed to reply\n", i)
				master.alive[i] = false
				if master.leader[i] {
					// neet to choose a new leader
					new_leader = true
					master.leader[i] = false
				}
			} else {
				master.alive[i] = true
			}
		}
		if !new_leader {//no replica failure
			continue
		}
		for i, new_master := range master.nodes {//set new leader
			if master.alive[i] {
				err := new_master.Call("Replica.BeTheLeader", new(megenericsmrproto.BeTheLeaderArgs), new(megenericsmrproto.BeTheLeaderReply))
				if err == nil {
					master.leader[i] = true
					log.Printf("Replica %d is the new leader.", i)
					for j := 0; j < master.N; j++ {
						if master.alive[j] {
							args := new(masterproto.GetLeaderReply)
							args.LeaderId = int(i)
							err1 := master.nodes[j].Call("Replica.UpdateLeader", args, new(megenericsmrproto.BeTheLeaderArgs))
							if err1 != nil {
								log.Fatalf("Faoled tp inform replica %d the leader information\n", j)
							}
						}
					} 
					break
				}
			}
		}
	}
}

//servers call register to register its informatin(address and port number)
func (master *Master) Register(args *masterproto.RegisterArgs, reply *masterproto.RegisterReply) error {

	master.lock.Lock()
	defer master.lock.Unlock()

	nlen := len(master.nodeList)
	index := nlen

	addrPort := fmt.Sprintf("%s:%d", args.Addr, args.Port)

	for i, ap := range master.nodeList {
		if addrPort == ap {
			index = i
			break
		}
	}

	if index == nlen {
		master.nodeList = master.nodeList[0 : nlen+1]
		master.nodeList[nlen] = addrPort
		master.addrList = master.addrList[0 : nlen+1]
		master.addrList[nlen] = args.Addr
		master.portList = master.portList[0 : nlen+1]
		master.portList[nlen] = args.Port
		nlen++
	}

	if nlen == master.N {
		reply.Ready = true
		reply.ReplicaId = index
		reply.NodeList = master.nodeList
	} else {
		reply.Ready = false
	}

	return nil
}

//replicas call GetLeader to find the leader's index
func (master *Master) GetLeader(args *masterproto.GetLeaderArgs, reply *masterproto.GetLeaderReply) error {
	time.Sleep(4 * 1000 * 1000)
	for i, l := range master.leader {
		if l {
			*reply = masterproto.GetLeaderReply{i}
			break
		}
	}
	return nil
}

//obtain the information of replica list
func (master *Master) GetReplicaList(args *masterproto.GetReplicaListArgs, reply *masterproto.GetReplicaListReply) error {
	master.lock.Lock()
	defer master.lock.Unlock()

	if len(master.nodeList) == master.N {
		reply.ReplicaList = master.nodeList
		reply.Ready = true
	} else {
		reply.Ready = false
	}
	return nil
}

//added by Luna
//mepaxos call GetPaxosModeInfo to pass the information when mepaxos is in EPaxos mode
func (master *Master) GetPaxosModeInfo(args *masterproto.PaxosModeInfoArgs, reply *masterproto.PaxosModeInfoReply) error {
	if !isDeliver[args.Repli] {
		eLatency[args.Repli] = float64(args.TotalNo + args.ConfilctNo) * args.Min_k
		repliNumber[args.Repli] = args.TotalNo
		min_k[args.Repli] = args.Min_k
		replicaToLeader[args.Repli] = args.RepliToLeader
		isDeliver[args.Repli] = true
	}
	return nil
}

//mepaxos call GetEPaxosModeInfo to pass the information when mepaxos is in Paxos mode and the replica is not the leader
func (master *Master) GetEPaxosModeInfo(args *masterproto.EPaxosModeInfoArgs, reply *masterproto.EPaxosModeInfoReply) error {
	if !isDeliver[args.Repli] && paxos{
		repliNumber[args.Repli] = args.TotalNo
		min_k[args.Repli] = args.Min_k
		isDeliver[args.Repli] = true
	}
	return nil
}

//mepaxos call GetEPaxosModeInfo to pass the information when mepaxos is in Paxos mode and the replica is the leader
func (master *Master) GetEPaxosModeLeaderInfo(args *masterproto.EPaxosModeLeaderInfoArgs, reply *masterproto.EPaxosModeLeaderInfoReply) error {
	if !isDeliver[args.Repli] && paxos{
		repliNumber[args.Repli] = args.TotalNo
		eConflict = args.ConfilctNo
		min_k[args.Repli] = args.Min_k
		//replicaToLeader[args.Repli] = args.RepliToLeader
		for i:=0; i<master.N; i++ {
			replicaToLeader[args.Repli][i] = args.RepliToLeader[i]
			log.Printf("replicaToLeader[args.Repli][%d] is %v\n", i, replicaToLeader[args.Repli][i])
		}
		isDeliver[args.Repli] = true
	}
	return nil
}

//rewrite the call function whcih set the timeout is 1 seconds
func (master *Master) callt(client *rpc.Client, serviceMethod string, args interface{}, reply interface{}) error {
	select {
	case call := <-client.Go(serviceMethod, args, reply, make(chan *rpc.Call, 1)).Done:
		return call.Error
	case <-time.After(1*time.Second):
	}
	return errors.New("timeout")
}
