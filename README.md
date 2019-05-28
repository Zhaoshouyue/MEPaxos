# MEPaxos
consensus algorithm MEPaxos

we develop MEPaxos on the basis of EPaxos and Multi-Paxos to solve the problem of client commands conflicting of EPaxos

the code is based on the work of EPaxos(https://github.com/efficient/epaxos)

# Three folders：
1.mypaxos-latency， which is the latency code
2.mypaxos-throughput-1KB， which is the throughput code when the client command is 1KB
3.mypaxos-throughput-16B， which is the throughput code when the client command is 16B

# You may need to run these command:
go install master
go install server
go install client

# You can run through:
bin/memaster &
bin/server & (3 or 5 times, depend on your server numbers)
bin/client
