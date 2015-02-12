package raft

import (
	"log"
	"myrpc"
	"net"
	"net/rpc"
	"strconv"
)

type LSN uint64 //Log sequence number, unique for all time.

//-- Loentry interface and its implementation
type LogEntry interface {
	Lsn() LSN
	Data() []byte
	Committed() bool
}

//--implementation
type LogItem struct {
	lsn         LSN
	isCommitted bool
	data        []byte
}

func (l LogItem) Lsn() LSN {
	return l.lsn
}
func (l LogItem) Data() []byte {
	return l.data
}
func (l LogItem) Committed() bool {
	return l.isCommitted
}

//--

type ServerConfig struct {
	Id         int    // Id of server. Must be unique
	Hostname   string // name or ip of host
	ClientPort int    // port at which server listens to client messages.
	LogPort    int    // tcp port for inter-replica protocol messages.
}

type ClusterConfig struct {
	Path    string         // Directory for persistent log
	Servers []ServerConfig // All servers in this cluster
}

type SharedLog interface {
	// Each data item is wrapped in a LogEntry with a unique
	// lsn. The only error that will be returned is ErrRedirect,
	// to indicate the server id of the leader. Append initiates
	// a local disk write and a broadcast to the other replicas,
	// and returns without waiting for the result.
	Append(data []byte) (LogEntry, error)
}

// Raft implements the SharedLog interface.
type Raft struct {
	ClusterConfigObj   ClusterConfig
	Myconfig           ServerConfig
	LeaderConfig       ServerConfig
	CurrentLogEntryCnt LSN
	commitCh           chan *LogEntry
}

func (r Raft) isThisServerLeader() bool {
	return r.Myconfig.Id == r.LeaderConfig.Id
}

func makeTCP_RPC_call(hostname string, port int, cmd string, done chan bool) {
	portstr := hostname + ":" + strconv.Itoa(port)
	client, err := rpc.Dial("tcp", portstr)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	// Synchronous call
	args := &myrpc.Args{cmd} //later it is mmore appropriate to pass log entry in this
	var reply bool
	err = client.Call("RPCservice.AppendEntriesRPC", args, &reply)
	if err != nil {
		log.Fatal("arith error:", err)
	} else {
		done <- reply
	}
}

func drainchan(commch chan bool) {
	for {
		select {
		case <-commch:
		default:
			return
		}
	}
}

func (r *Raft) Append(data []byte) (LogEntry, error) {
	if !r.isThisServerLeader() {
		var e error = ErrRedirect(r.LeaderConfig.Id)
		return nil, e
	} else { //this server is leader
		//make connections with all
		serverCount := len(r.ClusterConfigObj.Servers)
		done_sync := make(chan bool, serverCount)
		for i := 0; i < serverCount; i++ {
			hostname := r.ClusterConfigObj.Servers[i].Hostname
			logport := r.ClusterConfigObj.Servers[i].LogPort
			makeTCP_RPC_call(hostname, logport, string(data), done_sync)
		}
		//wait for responses
		//majority check
		majorityCnt := serverCount/2 + 1
		for i := 0; i < majorityCnt; i++ {
			<-done_sync
		}
		drainchan(done_sync)
		logentry := LogItem{r.CurrentLogEntryCnt, true, data}
		r.CurrentLogEntryCnt += 1
		return logentry, nil
	}
	return nil, nil
}

type ErrRedirect int // See Log.Append. Implements Error interface

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return string(e)
}

func RegisterTCP_RPCService(hostname string, port int) {
	portstr := hostname + ":" + strconv.Itoa(port)
	rpcservice := new(myrpc.RPCservice)
	rpc.Register(rpcservice)

	tcpAddr, err := net.ResolveTCPAddr("tcp", portstr)
	checkErr(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkErr(err)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()
}
