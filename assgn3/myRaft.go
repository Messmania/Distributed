package raft

import (
	//"log"
	"math"
	"math/rand"
	//"strings"
	"fmt"
	"time"
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
	Id int // Id of server. Must be unique
	//Hostname   string // name or ip of host
	//ClientPort int    // port at which server listens to client messages.
	//LogPort    int    // tcp port for inter-replica protocol messages.
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
	clientCh  chan ClientAppendResponse
	eventCh   chan interface{}
	voteCount int

	//Persisitent state on all servers
	currentTerm int
	votedFor    int
	myLog       LogDB

	//Volatile state on all servers
	myMetaData LogMetadata
}

func (r Raft) isThisServerLeader() bool {
	return r.Myconfig.Id == r.LeaderConfig.Id
}

func (r *Raft) Append(data []byte) (LogEntry, error) {
	//if (.debug) { //TO CHECK
	//do nothing
	//} else {
	//send to server's eventCh using method send()
	logItem := LogItem{0, false, data}        //TO BE MODIFIED
	obj := ClientAppendResponse{logItem, nil} ///TO BE MODIFIED
	send(r.Myconfig.Id, obj)
	response := <-r.clientCh //wait for response from server
	return response.logEntry, response.ret_error
	//}
}

type ErrRedirect int

//==========================Addition for assgn3============
//For converting default time unit of ns to secs
var secs time.Duration = time.Duration(math.Pow10(9))

const majority int = 3
const (
	ElectionTimeout      = iota
	HeartbeatTimeout     = iota
	AppendEntriesTimeOut = iota
)

//type logDB map[int]LogVal //map is not ordered!! Change this to array or linked list--DONE
type LogDB []LogVal //log is array of type LogVal
type LogVal struct {
	term int
	cmd  []byte //should it be logEntry type?, should not be as isCommited and lsn are useless to store in log
	//they are needed only for kvStore processing i.e. logEntry must be passed to commitCh
}
type LogMetadata struct {
	lastLogIndex int //last entry in log,corresponding term is term in LogVal, can also be calculated as len(LogDB)-1
	prevLogIndex int // value is lastLogIndex-1 always so not needed as such,---Change this later
	prevLogTerm  int
	//majorityAcksRcvd bool this is same as lastApplied?
	commitIndex int
	//lastApplied int--Not needed

	//Volatile state for leader
	//nextIndex must be maintained for every follower separately,
	// so there must be a mapping from follower to server's nextIndex for it
	// this dis nextIndex[server_id]=nextIndex
	nextIndexMap map[int]int //used during log repair,this is the entry which must be sent for replication
	matchIndex   int
}
type AppendEntriesReq struct {
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	//entries      []LogEntry  //or LogItem?
	//entries      []LogVal  //should it be LogEntry?, shouldn't be as it doesn't have a term, or a wrapper of term and LogEntry?
	entries []byte //leader appends one by one so why need array of logEntry as given in paper
	//LogEntry interface was given by sir, so that the type returned in Append is LogEntry type i.e. its implementator's type
	leaderCommitIndex  int
	leaderLastLogIndex int
}
type AppendEntriesResponse struct {
	term       int
	success    bool
	followerId int
}
type ClientAppendReq struct {
	data []byte
}
type ClientAppendResponse struct {
	logEntry  LogEntry
	ret_error error
}
type RequestVote struct {
	term         int
	candidateId  int
	lastLogIndex int
	lastLogTerm  int
}
type RequestVoteResponse struct {
	term        int
	voteGranted bool
}

func send(serverId int, msg interface{}) { //type of msg? it can be Append,AppendEntries or RequestVote or the responses
	//send to each servers event channel--How to get every1's channel?
	//maintain a shared/global map which has serverid->raftObj mapping so every server can get others raftObj and its eventCh
	raftObj := server_raft_map[serverId]
	raftObj.eventCh <- msg
}

func (r *Raft) receive() interface{} {
	//waits on eventCh
	request := <-r.eventCh
	return request
}

func (r *Raft) sendToAll(msg interface{}) {
	for k := range server_raft_map {
		if r.Myconfig.Id != k { //send to all except self
			send(k, msg)
		}
	}
}

//Keeps looping and performing follower functions till it timesout and changes to candidate
func (r *Raft) follower() int {
	fmt.Println("In follower()")
	//start heartbeat timer,timeout func wil place HeartbeatTimeout on channel
	waitTime := 10
	HeartBeatTimer := r.StartTimer(HeartbeatTimeout, waitTime)
	for {
		req := r.receive()
		fmt.Println("after r.receive()")
		switch req.(type) {
		case AppendEntriesReq:
			fmt.Println("in appendEntries")
			request := req.(AppendEntriesReq) //explicit typecasting
			r.serviceAppendEntriesReq(request, HeartBeatTimer, waitTime)
		case RequestVote:
			fmt.Println("in Requestvote")
			request := req.(RequestVote)
			waitTime_secs := secs * time.Duration(waitTime)
			HeartBeatTimer.Reset(waitTime_secs)
			r.serviceRequestVote(request)
		case ClientAppendReq: //follower can't handle clients and redirects to leader, sends upto commitCh as well as clientCh
			fmt.Println("in client append")
			request := req.(ClientAppendReq) //explicit typecasting
			response := ClientAppendResponse{}
			//What is lsn and what value to give?--PENDING
			logItem := LogItem{LSN(rand.Intn(1000)), false, request.data}
			response.logEntry = logItem
			r.commitCh <- &response.logEntry
			var e error = ErrRedirect(r.LeaderConfig.Id)
			response.ret_error = e
			//Ideally a client must pass its channel while calling Append else
			//how does a client know where to wait for response, shouldn't be in raft object
			r.clientCh <- response //respond to client giving the leader Id--to be changed
			//Where to give to client? Same channel? NO, as follower is itself reading that same channel
			//new channel for sending data to client,Test method will read it, so test method will do
			//logentry,error= r.Append(data) and will check the redirect error
		case int:
			fmt.Println("In follower timeout")
			//turn off timer? as now election timer will start in candidate() mode
			HeartBeatTimer.Stop() //not giving intellisense--CHECK LATER
			return candidate
		}
	}
}

//conducts election, returns only when state is changed else keeps looping on outer loop(i.e. restarting elections)
func (r *Raft) candidate() int {
	fmt.Println("In candidate()")
	//This loop is for election process which keeps on going until a leader is elected
	for { //Election process starts!
		r.currentTerm = r.currentTerm + 1 //increment current term
		//--start election timer for election-time out time, so when responses stop coming it must restart the election
		waitTime := 12
		ElectionTimer := r.StartTimer(ElectionTimeout, waitTime)
		r.votedFor, r.voteCount = r.Myconfig.Id, 1 //vote for self
		fmt.Println("before calling prepRV")
		reqVoteObj := r.prepRequestVote() //prepare request vote obj
		fmt.Println("after calling prepRV")
		r.sendToAll(reqVoteObj) //send requests for vote to all servers
		responseCount := 0
		//this loop for reading responses from all servers
		for { //keep receiving responses
			req := r.receive()
			switch req.(type) {
			case RequestVoteResponse: //got the vote response
				response := req.(RequestVoteResponse) //explicit typecasting so that fields of struct can be used
				responseCount = responseCount + 1     //variable to keep track of the no.of responses
				if response.voteGranted {
					r.voteCount = r.voteCount + 1
				}
				serverCount := len(r.ClusterConfigObj.Servers)
				if responseCount == serverCount-1 { //got all the responses or not?--RRMOVE THIS--NOT NEEDED!
					if r.voteCount >= majority {
						//r.updateLeaderInfo() --update leader details in raft object of all server?
						return leader //becomes the leader
					}
				}
			case AppendEntriesReq: //received an AE request instead of votes, i.e. some other leader has been elected
				request := req.(AppendEntriesReq)
				if request.term > r.currentTerm || (request.term == r.currentTerm && request.prevLogIndex >= r.myMetaData.lastLogIndex) {
					return follower //become follower as valid leader is present
				} else {
					break //keep fighting for the leadership and restart the election or DO NOTHING?? --PENDING
					//say 2 votes r received then AEReq is received which is not from a valid leader
					//so should it continue waiting for 1 more vote or restart?
				}
			case int:
				//turn off the timer	---Timeout PENDING
				ElectionTimer.Stop()
				break //come out of inner loop i.e. restart the election process
				//default: if something else comes, then ideally it should ignore that and again wait for correct type of response on channel
				//it does this, in the present code structure
			}
		}
	}
}

//Keeps sending heartbeats until state changes to follower
func (r *Raft) leader() int {
	fmt.Println("In leader(), I am: ", r.Myconfig.Id)
	/*start heartbeat-sending timer, after timeout send heartbeats to all servers--using r.sendToAll(heartbeat) and keep checking
	if it is still the leader before sending heartbeat i.e. in timeOut func if(leader) then send HB!
	or
	fire a go routine which loops forever and sends heartbeats after a fix time--What if this leader is demoted?
	Then terminate the func */
	waitTime := 5                                              //duration between two heartbeats
	HeartbeatTimer := r.StartTimer(HeartbeatTimeout, waitTime) //starts the timer and places timeout object on the channel
	var AppendEntriesTimer *time.Timer
	ack := 0
	responseCount := 0
	for {
		req := r.receive() //wait for client append req,extract the msg received on self eventCh
		switch req.(type) {
		case ClientAppendReq:
			HeartbeatTimer.Stop() // request for append will be sent now and will server as Heartbeat too
			request := req.(ClientAppendReq)
			data := request.data
			//No check for semantics of cmd before appending to log?
			r.AppendToLog(r.currentTerm, data) //append to self log as byte array
			//reset the heartbeat timer, now this sendRPC will maintain the authority of the leader
			r.sendAppendEntriesRPC()
			waitTime := 6                                                     //random value as max time for receiving AE_responses
			AppendEntriesTimer = r.StartTimer(AppendEntriesTimeOut, waitTime) //Can be written in HeartBeatTimer too
		case AppendEntriesResponse:
			response := req.(AppendEntriesResponse)
			serverCount := len(r.ClusterConfigObj.Servers)
			if response.success {
				ack += 1
			} else {
				if response.term > r.currentTerm { //this means another server is more up to date than itself
					r.currentTerm = response.term
					return follower
				} else { //Log is stale and needs to be repaired!
					//PUT THIS IN A logrepair func
					id := response.followerId
					failedIndex := r.myMetaData.nextIndexMap[id]
					nextIndex := failedIndex - 1 //decrementing follower's nextIndex
					appEntriesRetry := r.prepAppendEntriesReq(nextIndex)
					send(id, appEntriesRetry)
				}
			}
			if responseCount >= majority { //convert this nested ifs to && if condition
				if ack == majority { //are all positive acks? if not wait for reponses
					if response.term == r.currentTerm { //safety property for commiting entries from older terms
						r.myMetaData.commitIndex = r.myMetaData.lastLogIndex //commitIndex advances,Entries committed!
						//respond to client once entry is safely committed!
						// This response ideally comes from kvStore, so here we must send it to commitCh with committed as True
						reply := ClientAppendResponse{}
						data := r.myLog[r.myMetaData.lastLogIndex].cmd //last entry of leader's log
						logItem := LogItem{LSN(rand.Intn(1000)), true, data}
						reply.logEntry = logItem
						r.commitCh <- &reply.logEntry
					}
				}
				if responseCount == serverCount-1 && ack == serverCount { //entry is replicated on all servers! else keep waiting for acks!
					AppendEntriesTimer.Stop()
				}
			}

		case AppendEntriesReq: // in case some other leader is also in function, it must fall back or remain leader
			request := req.(AppendEntriesReq)
			if request.term > r.currentTerm {
				r.currentTerm = request.term //update self term and step down
				return follower              //sender server is the latest leader, become follower
			} else {
				reply := AppendEntriesResponse{r.currentTerm, false, r.Myconfig.Id}
				send(request.leaderId, reply)
			}

		case int: //Time out-time to send Heartbeats!
			timeout := req.(int)
			r.sendAppendEntriesRPC() //send Heartbeats
			waitTime_secs := secs * time.Duration(waitTime)
			if timeout == HeartbeatTimeout {
				HeartbeatTimer.Reset(waitTime_secs)
			} else if timeout == AppendEntriesTimeOut {
				AppendEntriesTimer.Reset(waitTime_secs) //start the timer again, does this call the TimeOut method again??-YES
			}
		}
	}
}

//Sends appendRPC requests to all servers and operates according to the responses received i.e. whether to advance the commitIndex or not
//sent by leader, r is leader
func (r *Raft) sendAppendEntriesRPC() {
	nextIndex := r.setNextIndex_All()
	appEntriesObj := r.prepAppendEntriesReq(nextIndex) //prepare AppendEntries object
	r.sendToAll(appEntriesObj)                         //send AppendEntries to all the followers
}

//Appends to self log
//adds new entry, modifies last and prev indexes, term
//how to modify the term fields? n who does--candidate state advances terms
func (r *Raft) AppendToLog(term int, cmd []byte) {
	//r.myMetaData.prevLogIndex = r.myMetaData.lastLogIndex
	index := r.myMetaData.lastLogIndex + 1 //location for new entry
	logVal := LogVal{term, cmd}            //make object for log's value field
	r.myLog[index] = logVal                //append to log

	//modify metadata after appending
	r.myMetaData.lastLogIndex = index
	r.myMetaData.prevLogIndex = index - 1
	r.myMetaData.prevLogTerm = r.myLog[r.myMetaData.prevLogIndex].term
	r.currentTerm = term
}

//Follower receives AEReq and appends to log checking the request objects fields
//also updates leader info after reseting timer or appending to log
func (r *Raft) serviceAppendEntriesReq(request AppendEntriesReq, HeartBeatTimer *time.Timer, waitTime int) {
	//replicates entry wise , one by one
	waitTime_secs := secs * time.Duration(waitTime)
	appEntriesResponse := AppendEntriesResponse{}
	appEntriesResponse.followerId = r.Myconfig.Id
	appEntriesResponse.success = false //false by default
	if request.term >= r.currentTerm { //valid leader
		r.LeaderConfig.Id = request.leaderId //update leader info
		HeartBeatTimer.Reset(waitTime_secs)  //reset the timer
		myLastIndexTerm := r.myLog[r.myMetaData.lastLogIndex].term
		if !(request.leaderLastLogIndex == r.myMetaData.lastLogIndex && request.term == myLastIndexTerm) { //this is not a heartbeat but append request
			if request.prevLogTerm == r.currentTerm && request.prevLogIndex == r.myMetaData.lastLogIndex { //log is consistent till now
				r.AppendToLog(request.term, request.entries) //append to log, also modifies  r.currentTerm
				appEntriesResponse.success = true
			}
		}
	}
	appEntriesResponse.term = r.currentTerm
	send(request.leaderId, appEntriesResponse)
}

//r is follower who received the request
//Services the received request for vote
func (r *Raft) serviceRequestVote(request RequestVote) {
	response := RequestVoteResponse{} //prep response object,for responding back to requester
	candidateId := request.candidateId
	//if haven't voted in this term then only vote!
	//Check if r.currentTerm is lastLogTerm or not in any scenario.. comparison must be with lastLogTerm of self log
	if r.votedFor == -1 && (request.lastLogTerm > r.currentTerm || (request.lastLogTerm == r.currentTerm && request.lastLogIndex >= r.myMetaData.lastLogIndex)) {
		response.voteGranted = true
		r.votedFor = candidateId
	} else {
		response.voteGranted = false
	}
	response.term = r.currentTerm //to return self's term too
	send(candidateId, response)   //send to sender using send(sender,response)
}

//preparing object for replicating log value at nextIndex
func (r *Raft) prepAppendEntriesReq(nextIndex int) AppendEntriesReq {
	//populate fields of AppendEntries RPC
	//r is leader object so it has log details
	term := r.myLog[nextIndex].term //term of the entry to be replicated
	leaderId := r.LeaderConfig.Id
	var entries []byte
	if len(r.myLog) != 0 {
		entries = r.myLog[nextIndex].cmd //entry to be replicated
	} else {
		entries = nil
	}
	prevLogIndex := nextIndex - 1             //should be changed to nextIndex-1
	prevLogTerm := r.myLog[prevLogIndex].term //should be changed to term corresponding to nextIndex-1 entry
	leaderCommitIndex := r.myMetaData.commitIndex
	leaderLastLogIndex := r.myMetaData.lastLogIndex
	appendEntriesObj := AppendEntriesReq{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommitIndex, leaderLastLogIndex}
	return appendEntriesObj

}

//prepares the object for sending to RequestVoteRPC, requesting the vote
func (r *Raft) prepRequestVote() RequestVote {
	lastLogIndex := r.myMetaData.lastLogIndex
	fmt.Println("here")
	//if this is request when log is empty
	var lastLogTerm int
	if len(r.myLog) == 0 {
		fmt.Println("In if of prepRV()")
		lastLogTerm = -1 //Just for now--Modify later
	} else {
		fmt.Println("In else of prepRV()")
		lastLogTerm = r.myLog[lastLogIndex].term
	}
	fmt.Println("here2")
	reqVoteObj := RequestVote{r.currentTerm, r.Myconfig.Id, lastLogIndex, lastLogTerm}
	return reqVoteObj
}

//Starts the timer with appropriate random number secs
func (r *Raft) StartTimer(timeoutObj int, waitTime int) (timerObj *time.Timer) {
	//waitTime := rand.Intn(10)
	//for testing
	//waitTime := 5
	expInSec := secs * time.Duration(waitTime) //gives in seconds
	fmt.Printf("Expiry time of %v is:%v", timeoutObj, expInSec)
	timerObj = time.AfterFunc(expInSec, func() {
		r.TimeOut(timeoutObj)
	})
	return
}

//Places timeOutObject on the eventCh of the caller
func (r *Raft) TimeOut(timeoutObj int) {
	r.eventCh <- timeoutObj
}

func (r *Raft) setNextIndex_All() int {
	nextIndex := r.myMetaData.lastLogIndex //given as lastLogIndex+1 in paper..don't know why,seems wrong.
	for k := range server_raft_map {
		if r.Myconfig.Id != k {
			r.myMetaData.nextIndexMap[k] = nextIndex
		}
	}
	return nextIndex
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return string(e)
}
