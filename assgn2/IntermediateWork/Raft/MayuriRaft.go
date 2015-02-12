package Raft

import (
	"./myrpc"
 "fmt"
 "net/rpc"
 "strconv"
 "log"
  "net"
	"net/http"
	"os"
)

type LSN uint64 //Log sequence number, unique for all time.

//-- Logentry interface and its implementation
type LogEntry interface {
    Lsn() LSN
    Data() []byte
    Committed() bool
}
//--implementation
type LogItem struct{
	lsn LSN
	isCommitted bool
	data []byte
}
func (l LogItem) Lsn() LSN{
	return l.lsn
}
func (l LogItem) Data() []byte{
	return l.data
}
func (l LogItem) Committed() bool{
	return l.isCommitted
}
//--

type ServerConfig struct {
Id int // Id of server. Must be unique
Hostname string // name or ip of host
ClientPort int // port at which server listens to client messages.
LogPort int // tcp port for inter-replica protocol messages.
}

type ClusterConfig struct {
Path string // Directory for persistent log
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
	ClusterConfigObj ClusterConfig
	Myconfig ServerConfig
	LeaderConfig ServerConfig
	CurrentLogEntryCnt LSN
}   


func (r Raft) isThisServerLeader() bool {
	return r.Myconfig.Id == r.LeaderConfig.Id
}
func makeRPC_call(hostname string, port int, cmd string, done chan *rpc.Call){
	portstr := hostname + ":"+ strconv.Itoa(port)
	client, err := rpc.DialHTTP("tcp", portstr)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	// Synchronous call
	args := &myrpc.Args{cmd} //later it is mmore appropriate to pass log entry in this
	var reply bool
	client.Go("RPCservice.AppendEntriesRPC", args, &reply, done)
	
	fmt.Println("Reply recieved from appendEntries: ", reply)
}

func makeRPC_call_sync(hostname string, port int, cmd string, done chan bool){
	portstr := hostname + ":"+ strconv.Itoa(port)
	client, err := rpc.DialHTTP("tcp", portstr)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	// Synchronous call
	args := &myrpc.Args{cmd} //later it is mmore appropriate to pass log entry in this
	var reply bool
	//client.Go("RPCservice.AppendEntriesRPC", args, &reply, done)
	err = client.Call("RPCservice.AppendEntriesRPC", args, &reply)
	if err != nil {
		log.Fatal("arith error:", err)
	}else{
		fmt.Println("Reply recieved from appendEntries: ", reply)
		done <- true
	}
}

func makeTCP_RPC_call(hostname string, port int, cmd string, done chan bool){
	portstr := hostname + ":"+ strconv.Itoa(port)
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
	}else{
		fmt.Println("Reply recieved from appendEntries: ", reply)
		done <- reply
	}
}

func (r *Raft) Append (data []byte) (LogEntry,error){
        if (!r.isThisServerLeader()){
			fmt.Println(r.LeaderConfig.Id)
			var e error = ErrRedirect(r.LeaderConfig.Id)
            return nil, e
            //s.commitCh<-err--done write to channel when error
		}else {  //this server is leader
            //make connections with all 
            serverCount := len(r.ClusterConfigObj.Servers)
            //done := make(chan *rpc.Call, serverCount) 
            done_sync := make(chan bool, serverCount) 
            for i:=0; i<serverCount; i++ {
				hostname :=r.ClusterConfigObj.Servers[i].Hostname
				logport := r.ClusterConfigObj.Servers[i].LogPort
				//makeRPC_call(hostname, logport, string(data), done)
				//makeRPC_call_sync(hostname, logport, string(data), done_sync)
				makeTCP_RPC_call(hostname, logport, string(data), done_sync)
			}
            //wait for responses
            //majority check
            majorityCnt := serverCount/2 + 1
            for i:=0; i< majorityCnt; i++{
				//<-done
				<-done_sync
			}
			//wht about clearing the channel as we are reading only majority of them not all ?
            logentry := LogItem{r.CurrentLogEntryCnt , true, data }
            r.CurrentLogEntryCnt += 1
            return logentry, nil
		}
}    


type ErrRedirect int // See Log.Append. Implements Error interface

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
//return "Redirect to server " + strconv.Itoa(10)
//return "Redirect to server " + strconv.Itoa(e)
return string(e);
}

func RegisterRPCService(hostname string, port int, rpcservice *myrpc.RPCservice){
	portstr := hostname + ":"+ strconv.Itoa(port)
	 //rpcservice  = new(myrpc.RPCservice)
	//rpc.Register(rpcservice)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", portstr)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}

func RegisterTCP_RPCService(hostname string, port int){
	portstr := hostname + ":"+ strconv.Itoa(port)
	rpcservice  := new(myrpc.RPCservice)
	rpc.Register(rpcservice)

	tcpAddr, err := net.ResolveTCPAddr("tcp", portstr)
	checkError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	/* This works:
	rpc.Accept(listener)
	*/
	/* and so does this:
	 */
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		rpc.ServeConn(conn)
	}

}

func checkError(err error) {
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		os.Exit(1)
	}
}
