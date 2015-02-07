package Raft


//import "./rpc"
//import "log"
//import "fmt"
import( "net/rpc"
       // "strings"
        //"strconv"
        //"log"
       )



type LSN uint64 //Log sequence number, unique for all time.
type ErrRedirect int // See Log.Append. Implements Error interface.

//-- Loentry interface and its implementation
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
Append(data []byte) (LogEntry, error)
}


// Raft implements the SharedLog interface.
type Raft struct {
	clusterConfigObj *ClusterConfig
	myconfig ServerConfig
	leaderConfig ServerConfig
	currentLogEntryCnt LSN

}

func (r Raft) isThisServerLeader() bool {
	return r.myconfig.Id == r.leaderConfig.Id
}
func makeRPC_call(hostname string, port int, cmd string, done chan *rpc.Call){
	/*
	portstr := hostname + ":"+ strconv.Itoa(port)
	client, err := rpc.DialHTTP("tcp", portstr)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	// Synchronous call
	args := &rpc.Args{cmd} //later it is mmore appropriate to pass log entry in this
	var reply bool
	client.Go("RPCservice.AppendEntriesRPC", args, &reply, done)
	
	//fmt.Println("Reply recieved from appendEntries: ", reply)
	*/
}

func (r *Raft) Append (data []byte) (LogEntry,ErrRedirect){
/*
        if (!r.isThisServerLeader()){
            return nil, ErrRedirect{r.leaderConfig.Id}
            //s.commitCh<-err--done write to channel when error
		}else {  //this server is leader
            //make connections with all 
            serverCount := len(r.clusterConfigObj.Servers)
            done := make(chan *rpc.Call, serverCount) 
            for i:=0; i<serverCount; i++ {
				hostname :=r.clusterConfigObj.Servers[i].Hostname
				logport := r.clusterConfigObj.Servers[i].LogPort
				makeRPC_call(hostname, logport, string(data), done)
			}
            //wait for responses
            //majority check
            majorityCnt = serverCount/2 + 1
            for i:=0; i< majorityCnt; i++{
				<-done
			}
			//wht about clearing the channel as we are reading only majrity of them not all ?
            logentry := LogItem{r.currentLogEntryCnt , true, data }
            r.currentLogEntryCnt += 1
            return logentry, nil
		}
		*/
		//for testing server
		var logE LogEntry
		return logE,0
}    


// ErrRedirect as an Error object
/*
func (e ErrRedirect) Error() string {
//return "Redirect to server " + strconv.Itoa(10)
//return "Redirect to server " + strconv.Itoa(e)
return strconv.Itoa(e);
}*/



