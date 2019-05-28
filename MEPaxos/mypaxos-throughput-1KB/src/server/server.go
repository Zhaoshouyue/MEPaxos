package main

import (
	"mepaxos"//modified by Luna
	"flag"
	"fmt"
	"log"
	"masterproto"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"time"
)

var portnum *int = flag.Int("port", 7070, "Port # to listen on. Defaults to 7070")
var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost.")
var masterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7087.")
var myAddr *string = flag.String("addr", "", "Server address (this machine). Defaults to localhost.")
var doMEPaxos *bool = flag.Bool("me", false, "Use MEPaxos as the replication protocol. Defaults to false.")//modified by Luna
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var thrifty = flag.Bool("thrifty", true, "Use only as many messages as strictly required for inter-replica communication.")
var exec = flag.Bool("exec", true, "Execute commands.")
var dreply = flag.Bool("dreply", false, "Reply to client only after command has been executed.")
var beacon = flag.Bool("beacon", true, "Send beacons to other replicas to compare their relative speeds.")
var durable = flag.Bool("durable", false, "Log to a stable store (i.e., a file in the current dir).")

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)

		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt)
		go catchKill(interrupt)
	}

	log.Printf("Server starting on port %d\n", *portnum)

	//modified by Luna
	masterAddrWithPort := fmt.Sprintf("%s:%d", *masterAddr, *masterPort)
	args := &masterproto.RegisterArgs{*myAddr, *portnum}
	var reply masterproto.RegisterReply
	var mcli *rpc.Client
	var err1 error
	for done := false; !done; {
		mcli, err1 = rpc.DialHTTP("tcp", masterAddrWithPort)
		if err1 == nil {
			err1 = mcli.Call("Master.Register", args, &reply)
			if err1 == nil && reply.Ready == true {
				done = true
				break
			}
		}
		time.Sleep(1e9)
	}
	replicaId := reply.ReplicaId
	nodeList := reply.NodeList

	if *doMEPaxos {
		log.Println("Starting Modified Egalitarian Paxos replica...")
		rep := mepaxos.NewReplica(replicaId, nodeList, *portnum, *thrifty, *exec, *dreply, *beacon, *durable, mcli)
		rpc.Register(rep)
	}

	rpc.HandleHTTP()
	//listen for RPC on a different port (8070 by default)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum+1000))
	if err != nil {
		log.Fatal("listen error:", err)
	}

	http.Serve(l, nil)
}

func catchKill(interrupt chan os.Signal) {
	<-interrupt
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}
	fmt.Println("Caught signal")
	os.Exit(0)
}
