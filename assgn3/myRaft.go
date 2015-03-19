package raft

import (
	"log"
	///"myrpc"
	//"net"
	//"net/rpc"
	"strconv"
	"strings"
)

type LSN uint64 //Log sequence number, unique for all time.

//-- Log entry interface and its implementation
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
	//====for assgn3	========
	clientCh chan ClientAppendResponse
	eventCh   chan interface{}
	voteCount int
	
	//Persisitent state on all servers
	currentTerm int	
	votedFor  int
	myLog LogDB	
	
	//Volatile state on all servers	
	myMetaData LogMetadata
}

func (r Raft) isThisServerLeader() bool {
	return r.Myconfig.Id == r.LeaderConfig.Id
}

func (r *Raft) Append(data []byte) (LogEntry, error) {
	if (debug){		//TO CHECK
		//do nothing
	} else {
		//send to server's eventCh using method send()
		obj = ClientAppendResponse{data}
		send(r.Myconfig.Id,obj)		
		response := <-r.clientCh					//wait for response from server
		return response.logEntry, response.ret_error
	}
}

type ErrRedirect int

//==========================Addition for assgn3============
const majority int=3	
const (ElectionTimeout = iota
	HeartbeatTimeout = iota
	)
//type logDB map[int]LogVal //map is not ordered!! Change this to array or linked list--PENDING
type LogDB []LogVal //log is array of type LogVal
type LogVal struct {
	term int
	cmd  string				//should it be logEntry type?, shouldnot be as isCommited and lsn are useless to store in log
	//they are needed only for kvStore processing i.e. logEntry must be passed to commitCh
}
type LogMetadata struct{
	lastLogIndex int//corresponding term is term in LogVal, can also be calculated as len(LogDB)-1
	//prev is 1 before the last index
	prevLogIndex int // value is lastLogIndex-1, Change this later
	prevLogTerm int
	//majorityAcksRcvd bool this is same as lastApplied?
	commitIndex int
	lastApplied int
}
type AppendEntriesReq struct {	
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []LogEntry  //or LogItem?, 
	//LogEntry interface was given by sir, so that the type returned in Append is LogEntry type i.e. its implementator's type
	leaderCommitIndex int
}
type AppendEntriesResponse struct{
	term     int
	success      bool
}
type ClientAppendReq struct {
	data         []byte
}
type ClientAppendResponse struct{
	logEntry LogItem
	ret_error    error
}
type RequestVote struct {
	term            int
	candidateId     int
	lastLogIndex    int
	lastLogTerm     int
	}
type RequestVoteResponse struct{
	term        int
	voteGranted bool
}

func send(serverId int ,msg interface{}){	//type of msg? it can be Append,AppendEntries or RequestVote or the responses
	//send to each servers event channel--How to get every1's channel?
	//maintain a shared/global map which has serverid->raftObj mapping so every server can get others raftObj
	raftObj := server_raft_map[serverId]	
	raftObj.eventCh <- msg
}

func (r *Raft) receive()interface{}{
	//waits on eventCh
	request := <-r.eventCh
	return request
}

func (r *Raft) sendToAll(msg interface{}){
    for k := range mymap {
		if (r.Myconfig.Id != k) 					//send to all except self
			send(k,msg)
    } 
}


//Keeps looping and performing follower functions till it timesout and changes to candidate
func (r *Raft) follower() {
	//start heartbeat timer,timeout func wil place HeartbeatTimeout on channel    --PENDING
	for {
		//req := <-r.eventCh
		req := r.receive()
		switch req.(type) {
		case AppendEntriesReq:
			fmt.Println("in appendEntries")
			request := req.(AppendEntriesReq)		//explicit typecasting
			r.serviceAppendEntriesReq(request)	
		case RequestVote:
			fmt.Println("in Requestvote")
			request := req.(RequestVote)
			r.serviceRequestVote(request)
		case ClientAppendReq:		//follower can't handle clients and redirects to leader, sends upto commitCh as well as clientCh
			fmt.Println("in client append")
			request := req.(ClientAppendReq)		//explicit typecasting
			response := ClientAppendResponse{}
			//What is lsn and what value to give?--PENDING
			response.logEntry.data = request.data
			response.logEntry.isCommitted = false
			r.commitCh <- response.logEntry
			var e error = ErrRedirect(r.LeaderConfig.Id)
			response.ret_error = e
			r.clientCh <- response			//respond to client giving the leader Id
			//Where to give to client? Same channel? NO, as follower is itself reading that same channel
			//new channel for sending data to client,Test method will read it, so test method will do 
			//logentry,error= r.Append(data) and will check the redirect error			
		case HeartbeatTimeout:
			//turn off timer? as now election timer will start in candidate() mode
			return candidate
		}
	}
}

//conducts election, returns only when state is changed else keeps looping on outer loop(i.e. restarting elections)
func (r *Raft) candidate() {
	//This loop is for election process which keeps on going until a leader is elected
	for {							//Election process starts!
		r.currentTerm = r.currentTerm + 1 						//increment current term	
		//--start election timer for election-time out time, so when responses stop coming it must restart the election	
		r.votedFor,r.voteCount = r.Myconfig.Id,1		//vote for self								
		reqVoteObj := r.prepRequestVote()		//prepare request vote obj
		r.sendToAll(reqVoteObj)					//send requests for vote to all servers	
		responseCount := 0
		//this loop for reading responses from all servers
		for {									//keep receiving responses
			req := r.receive() 
			switch req.(type) {
			case RequestVoteResponse: 								//got the vote response				
				response := req.(RequestVoteResponse)				//explicit typecasting so that fields of struct can be used
				responseCount = responseCount + 1					//variable to keep track of the no.of responses
				if response.voteGranted {
					r.voteCount = r.voteCount + 1
				}
				serverCount := len(r.ClusterConfigObj.Servers)
				if responseCount == serverCount-1 {					//got all the responses or not?
					if r.voteCount >= r.majority {
						//r.updateLeaderInfo() --update leader details in raft object of all server?
						return leader				//becomes the leader
					}
				}
			case AppendEntriesReq: //received an AE request instead of votes, i.e. some other leader has been elected
				request := req.(AppendEntriesReq)
				if request.term > r.currentTerm ||(request.term == r.currentTerm && request.prevLogIndex >= r.lastLogIndex){
					return follower									//become follower as valid leader is present
				} else {
					break 				//keep fighting for the leadership and restart the election or DO NOTHING?? --PENDING
					//say 2 votes r received then AEReq is received which is not from a valid leader 
					//so should it continue waiting for 1 more vote or restart?
				}
			case ElectionTimeout:
				//turn off the timer	---Timeout PENDING
				break;		//come out of inner loop i.e. restart the election process
			//default: if something else comes, then ideally it should ignore that and again wait for correct type of response on channel
			//it does this, in the present code structure
			}
		}
	}
}


//Keeps sending heartbeats until state changes to follower
func (r *Raft) leader() {
	/*start heartbeat-sending timer, after timeout send heartbeats to all servers--using r.sendToAll(heartbeat) and keep checking
	if it is still the leader before sending heartbeat i.e. in timeOut func if(leader) then send HB!
	or 
	fire a go routine which loops forever and sends heartbeats after a fix time--WHat if this leader is demoted? 
	Then terminate the func */
	for {
		req := r.receive() 			//wait for req,extract the msg received on self eventCh
		switch req.(type) {
		case ClientAppendReq:
			request := req.(ClientAppendReq)
			cmd := string(request.data)
			//When to advance current term?
			r.AppendToLog(r.currentTerm,cmd) 						//append to self log as string not byte array	
			data := request.data
			return r.sendAppendEntriesRPC(data)
		case AppendEntriesReq:
			request := req.(AppendEntriesReq)
			// in case some other leader is also in function, it must fall back or remain leader
			msg := request.entries
			if msg.term > r.currentTerm ||(msg.term == r.currentTerm && msg.prevLogIndex >= r.lastLogIndex){
				return follower		//sender server is the latest leader, become follower
			}
		}
	}

}


func (r *Raft) prepAppendEntriesReq(data []byte)AppendEntriesReq{
	//populate fields of AppendEntries RPC
	term := r.currentTerm
	leaderId := r.LeaderConfig.Id
	index := r.myMetaData.lastLogIndex + 1
	entries[index] := data
	prevLogIndex := index -1
	prevLogTerm := r.myMetaData.prevLogTerm
	//leaderCommitIndex :=  --PENDING
	appendEntriesObj := AppendEntriesReq{term,prevLogIndex,prevLogTerm,entries,leaderCommitIndex}
	return appendEntriesObj
	
}

func (r *Raft) prepRequestVote()RequestVote{
	lastLogIndex := r.myMetaData.lastLogIndex
	lastLogTerm := r.myLog[lastLogIndex].term
	reqVoteObj := RequestVote{r.currentTerm,r.Myconfig.Id,lastLogIndex,lastLogTerm}
	return reqVoteObj
}

//Sends appendRPC requests to all servers and operates according to the responses received i.e. whether to advance the commitIndex or not
//sent by leader and received by followers or candidates
func (r *Raft) sendAppendEntriesRPC(data []byte){
	appEntriesObj := r.prepAppendEntriesReq(data) 	//prepare AppendEntries object,not showing with r.prepAppendEntries--WORKs
	r.sendToAll(appEntriesObj) 					//send AppendEntries to all use send(Server_i,appEntriesObj) 4 times	
	responseCount := 0
	ack := 0
	for {
	resp := r.receive()								//wait for acks on eventCh
	//what if some other msg except ClientAppendResponse comes on channel? so another switch-case	
		switch resp.(type){
			case AppendEntriesResponse:
				response := resp.(AppendEntriesResponse)
				responseCount += 1
				if response.success{
						ack += 1
				}else{
					//decrement the nextIndex and resend AppendEntries--Log replication
				}
				serverCount := len(r.ClusterConfigObj.Servers)				
				if responseCount == serverCount-1 {				//got all the responses or not?					
					if ack == majority {
						r.myMetaData.commitIndex += 1
					}
				/*
					////if majority acks received, commit and respond to client			//TO BE MODIFIED after writing Append func
						appendCount := appendCount + 1					
						
						return leader								//remain leader
					} else {
						//else resend AppendEntries--PENDING
					}
					*/
				}				
			case AppendEntriesReq:
			case RequestVote: 
		}
	}	
}

//Appends to self log
//adds new entry, modifies last and prev indexes, term
//how to modify the term fields? n who does
func (r *Raft) AppendToLog(term int,cmd string) {
	r.myMetaData.prevLogIndex = r.myMetaData.lastLogIndex
	index := r.myMetaData.lastLogIndex + 1				//location for new entry
	logVal := LogVal{term, cmd}							//make object for log's value field
	r.myLog[index] = logVal								//append to log	
	
	//modify metadata after appending	 
	r.myMetaData.lastLogIndex = index					
	r.myMetaData.prevLogTerm = r.currentTerm	
	r.myMetaData.prevLogIndex = r.myMetaData.lastLogIndex //or index -1
	r.currentTerm = term	
}

//Follower receives AEReq and appends to log checking the request objects fields
//If no data, this is heartbeat and timer is reset.
//also updates leader info after reseting timer or appending to log
func (r *Raft) serviceAppendEntriesReq(request AppendEntriesReq) {
	//If leader ,no need of checking as only leader will send these?	--CONFIRM
	//replicates entry wise , one by one
	if request.term >= r.currentTerm && request.prevLogTerm == r.currentTerm && request.prevLogIndex == r.myMetaData.lastLogIndex {
		if len(request.entries)==0{ 			//if no log entries present in request
			//this is heartbeat!
			//--reset the timer	--Will timer be part of raft? or pass the timer explicitly to this method
		} else {					
			r.AppendToLog(request.term,request.entries)				//append to log				
		}
		//extract leader info from request and update self's leader info

	} else {		//This is not a valid leader so send NACK
		//ignore the request i.e. send success false to the send using send()
		appEntriesResponse := AppendEntriesResponse()
		appEntriesResponse.success = false
		appEntriesResponse.term = r.currentTerm
		send(request.leaderId,appEntriesResponse)	//since only leader can send AppendEntriesReq		
	}
}

func (r *Raft) serviceRequestVote(request RequestVote){	
	response := RequestVoteResponse{} 	//prep response object,for responding back to requester
	//if haven't voted in this term then only vote!
	if r.votedFor==-1 && (request.term > r.currentTerm ||(request.term == r.currentTerm && request.prevLogIndex >= r.lastLogIndex)){
		response.voteGranted = true
		r.votedFor = request.candidateId	
	}else{
		response.voteGranted = false
	}
	response.term = r.currentTerm				//to return self's term too
	candidateId := request.candidateId
	send(candidateId,response)				//send to sender using send(sender,response)
}