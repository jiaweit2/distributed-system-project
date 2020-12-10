# CS425 Distributed System Projects

## Run

### Introducer
On VM 01, run `go run cmd/introducer/introducer.go`.

### Node
On all VMs(including VM 01), run `go run cmd/member/member.go`.
There are several optional flags:

-introducer: sepcify the address of introducer, defaults to VM 01

-interval: heartbeat interval in seconds

-mode: ALL for All-to-All mode, G for gossip mode

-loss: packet loss rate

-interface: specify network interface, defaults to 'eth0'

-replica: number of file replications on SDFS, defaults to 4

### Maple / Juice
VM 01 is set to be the fixed master for MapleJuice task and assumed to be alive at all time. Here is the example to run one of our task. After `member.go` on all nodes are run successfully, on any node, in our own custom cmd line, sequentially run <br>
`maple VotingMapper1/VotingMapper1 5 voteMapper1Out CondorcetVotes` <br>
`juice VotingReducer1/VotingReducer1 5 voteMapper1Out voteReducer1Out 0` <br>
`maple VotingMapper2/VotingMapper2 5 voteMapper2Out voteReducer1Out` <br>
`juice VotingReducer2/VotingReducer2 5 voteMapper2Out FinalOutput 1` <br>
The final output file, named `FinalOutput`, will be on a random node. You may do `get FinalOutput FinalOutput` at your current node and then you can view the file at `~/local`.