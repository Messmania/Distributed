package raft

import (
	"encoding/json"
	//"fmt"
	"math"
	//"math/rand"
	"os"
	"strconv"
	"sync"
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
	Append(data []byte) (LogEntry, error)
}

// Raft implements the SharedLog interface.
type Raft struct {
	ClusterConfigObj   ClusterConfig
	Myconfig           ServerConfig
	LeaderConfig       ServerConfig
	CurrentLogEntryCnt LSN
	commitCh           chan *LogEntry

	//====for assgn3==============================
	//clientCh  chan ClientAppendResponse
	eventCh chan interface{}

	//Persisitent state on all servers--ON DISK
	currentTerm int
	votedFor    int
	myLog       []LogVal

	//Volatile state on all servers
	myMetaData LogMetadata

	//used by leader only
	f_specific map[int]*followerDetails //map of serverId-details

	//Path strings to files on disk
	path_CV  string
	path_Log string
}

//used by leader to track details specific to a follower
type followerDetails struct {
	vote bool
	//include next index here--later
	//nextIndex int
}

func (r *Raft) Append(data []byte) (LogEntry, error) {
	obj := ClientAppendReq{data}
	send(r.Myconfig.Id, obj) //ADD CRASH CONDITION--PENDING
	//fmt.Println("I am", r.Myconfig.Id, "In Append,data sent to channel of", r.Myconfig.Id)
	response := <-r.commitCh
	//fmt.Println("Response received on commit channel", response)
	return *response, nil
}

type ErrRedirect int

//==========================Addition for assgn3============
//temp for testing
//const layout = "3:04:5 pm (MST)"

//global vars for simulating crashing of servers
var crash bool
var server_to_crash int
var globMutex = &sync.RWMutex{}

//var LSNMutex = &sync.RWMutex{}

//For converting default time unit of ns to millisecs
var secs time.Duration = time.Millisecond

const majority int = 3
const noOfServers int = 5
const (
	ElectionTimeout      = iota
	HeartbeatTimeout     = iota
	AppendEntriesTimeOut = iota
	RetryTimeOut         = iota
)

//type logDB map[int]LogVal //map is not ordered!! Change this to array or linked list--DONE(changed to array)
//type LogDB []LogVal //log is array of type LogVal
type LogVal struct {
	Term int
	Cmd  []byte //should it be logEntry type?, should not be as isCommited and lsn are useless to store in log
	//they are needed only for kvStore processing i.e. logEntry must be passed to commitCh

	//This is added for commiting entries from prev term--So when there are entries from prev term in leaders log but have not been replicated on mjority
	//then there must be a flag which is checked before advancing the commitIndex, if false, commit(replicate on maj) them first!
	//used only by leader
	Acks int
}
type LogMetadata struct {
	lastLogIndex int //last entry in log,corresponding term is term in LogVal, can also be calculated as len(LogDB)-1
	prevLogIndex int // value is lastLogIndex-1 always so not needed as such,---Change this later
	prevLogTerm  int
	commitIndex  int

	//Volatile state for leader
	//nextIndex must be maintained for every follower separately,
	// so there must be a mapping from follower to server's nextIndex for it

	// this dis nextIndex[server_id]=nextIndex--===========Shift this to follower object later==PENDING
	nextIndexMap map[int]int //used during log repair,this is the entry which must be sent for replication
	//matchIndex   int--NOT NEEDED AS OF NOW (Since no batch processing)
}
type AppendEntriesReq struct {
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []byte //leader appends one by one so why need array of logEntry as given in paper
	//LogEntry interface was given by sir, so that the type returned in Append is LogEntry type i.e. its implementator's type
	leaderCommitIndex  int
	leaderLastLogIndex int
}
type AppendEntriesResponse struct {
	term         int
	success      bool
	followerId   int
	isHeartBeat  bool
	lastLogIndex int //added for parallel appends!-4 April-03:25am
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
	id          int //added so that leader can keep track of votes from a server rather than voteCount
}

func send(serverId int, msg interface{}) { //type of msg? it can be Append,AppendEntries or RequestVote or the responses
	//send to each servers event channel--How to get every1's channel?
	//maintain a shared/global map which has serverid->raftObj mapping so every server can get others raftObj and its eventCh
	raftObj := server_raft_map[serverId]
	//fmt.Println("Before writing to channel of:", serverId)
	raftObj.eventCh <- msg
	//fmt.Println("After writing to channel of:", serverId)
}

func (r *Raft) receive() interface{} {
	//waits on eventCh
	request := <-r.eventCh
	switch request.(type) { //if data on channel is RetryTimeout, then it should revert to follower, hence return the timeout object
	case int:
		request = request.(int)
	}
	if !(getServerToCrash() == r.Myconfig.Id && getCrash()) || request == RetryTimeOut {
		return request
	} else {
		return nil
	}
}

func (r *Raft) sendToAll_AppendReq(msg []interface{}) {
	//fmt.Println("Server-Raft map:", server_raft_map)
	if !(r.Myconfig.Id == getServerToCrash() && getCrash()) {
		//fmt.Println("I am", r.Myconfig.Id, "able to send")
		for k := range server_raft_map {
			//fmt.Println("Id from map is:", k, r.Myconfig.Id)
			if r.Myconfig.Id != k { //send to all except self
				//fmt.Println("Sending HB to ", k)
				go send(k, msg[k]) //removed keyword go
				//fmt.Println("After sending RPCS")

			}
		}
	}
}

//This is different method because it sends same object to all followers,
//In above method arg is an array so if that is used, RV obj will have to be replicated unnecessarily
func (r *Raft) sendToAll(msg interface{}) {
	//fmt.Println("Server-Raft map:", server_raft_map)
	for k := range server_raft_map {
		//fmt.Println("Id from map is:", k, r.Myconfig.Id)
		if r.Myconfig.Id != k { //send to all except self
			go send(k, msg) //removed go
			//fmt.Println("After sending RV")
		}
	}

}

//Keeps looping and performing follower functions till it timesout and changes to candidate
func (r *Raft) follower(timeout int) int {
	//myId := r.Myconfig.Id
	//fmt.Println("In follower()", myId)
	//start heartbeat timer,timeout func wil place HeartbeatTimeout on channel
	waitTime := timeout //use random number after func is tested--PENDING
	HeartBeatTimer := r.StartTimer(HeartbeatTimeout, waitTime)

	for {
		req := r.receive()
		switch req.(type) {
		case AppendEntriesReq:
			request := req.(AppendEntriesReq) //explicit typecasting
			r.serviceAppendEntriesReq(request, HeartBeatTimer, waitTime)
		case RequestVote:
			waitTime_secs := secs * time.Duration(waitTime)
			request := req.(RequestVote)
			//fmt.Println("Requestvote came to", myId, "from", request.candidateId)
			HeartBeatTimer.Reset(waitTime_secs)
			//fmt.Println("Timer reset to:", waitTime_secs)
			r.serviceRequestVote(request)
		case ClientAppendReq: //follower can't handle clients and redirects to leader, sends upto commitCh as well as clientCh
			//fmt.Println("in client append")
			request := req.(ClientAppendReq) //explicit typecasting
			response := ClientAppendResponse{}
			logItem := LogItem{r.CurrentLogEntryCnt, false, request.data} //lsn is count started from 0
			r.CurrentLogEntryCnt += 1
			response.logEntry = logItem
			r.commitCh <- &response.logEntry
			//var e error = ErrRedirect(r.LeaderConfig.Id)
			//response.ret_error = e
			//r.clientCh <- response //respond to client giving the leader Id--Should only be given leader id right?--CHECK
		case int:
			//fmt.Println("In follower timeout", r.Myconfig.Id, time.Now().Format(layout))
			HeartBeatTimer.Stop() //turn off timer as now election timer will start in candidate() mode
			return candidate
		}
	}
}

//conducts election, returns only when state is changed else keeps looping on outer loop(i.e. restarting elections)
func (r *Raft) candidate() int {
	//myId := r.Myconfig.Id
	//fmt.Println("Election started!I am", myId)

	//reset the votes else it will reflect the votes received in last term
	r.resetVotes()

	//--start election timer for election-time out time, so when responses stop coming it must restart the election

	waitTime := 10
	//fmt.Println("ELection timeout is", waitTime)
	ElectionTimer := r.StartTimer(ElectionTimeout, waitTime)
	//This loop is for election process which keeps on going until a leader is elected
	for {
		r.currentTerm = r.currentTerm + 1 //increment current term
		//fmt.Println("I am candidate", r.Myconfig.Id, "and current term is now:", r.currentTerm)

		r.votedFor = r.Myconfig.Id //vote for self
		r.WriteCVToDisk()          //write Current term and votedFor to disk
		r.f_specific[r.Myconfig.Id].vote = true

		//fmt.Println("before calling prepRV")
		reqVoteObj := r.prepRequestVote() //prepare request vote obj
		//fmt.Println("after calling prepRV")
		r.sendToAll(reqVoteObj) //send requests for vote to all servers
		//this loop for reading responses from all servers
		for {
			req := r.receive()
			switch req.(type) {
			case RequestVoteResponse: //got the vote response
				response := req.(RequestVoteResponse) //explicit typecasting so that fields of struct can be used
				//fmt.Println("Got the vote", response.voteGranted)
				if response.voteGranted {
					//					temp := r.f_specific[response.id] //NOT ABLE TO DO THIS--WHY??--WORK THIS WAY
					//					temp.vote = true

					r.f_specific[response.id].vote = true
					//r.voteCount = r.voteCount + 1
				}
				voteCount := r.countVotes()
				//fmt.Println("I am:", r.Myconfig.Id, "Votecount is", voteCount)
				if voteCount >= majority {
					//fmt.Println("Votecount is majority, I am new leader", r.Myconfig.Id)
					ElectionTimer.Stop()
					r.LeaderConfig.Id = r.Myconfig.Id //update leader details
					return leader                     //become the leader
				}

			case AppendEntriesReq: //received an AE request instead of votes, i.e. some other leader has been elected
				request := req.(AppendEntriesReq)
				//Can be clubbed with serviceAppendEntriesReq with few additions!--SEE LATER

				//fmt.Println("I am ", r.Myconfig.Id, "candidate,got AE_Req from", request.leaderId, "terms my,leader are", r.currentTerm, request.term)
				waitTime_secs := secs * time.Duration(waitTime)
				appEntriesResponse := AppendEntriesResponse{}
				appEntriesResponse.followerId = r.Myconfig.Id
				appEntriesResponse.success = false //false by default, in case of heartbeat or invalid leader
				if request.term >= r.currentTerm { //valid leader
					r.LeaderConfig.Id = request.leaderId //update leader info
					ElectionTimer.Reset(waitTime_secs)   //reset the timer
					var myLastIndexTerm int
					if len(r.myLog) == 0 {
						myLastIndexTerm = -1

					} else {
						myLastIndexTerm = r.myLog[r.myMetaData.lastLogIndex].Term
					}
					if request.leaderLastLogIndex == r.myMetaData.lastLogIndex && request.term == myLastIndexTerm { //this is heartbeat from a valid leader
						appEntriesResponse.success = true
					}
					send(request.leaderId, appEntriesResponse)
					return follower
				} else {
					//check if log is same
					//fmt.Println("In candidate, AE_Req-else")
					send(request.leaderId, appEntriesResponse)
				}
			case int:
				waitTime_secs := secs * time.Duration(waitTime)
				ElectionTimer.Reset(waitTime_secs)
				break //come out of inner loop i.e. restart the election process
				//default: if something else comes, then ideally it should ignore that and again wait for correct type of response on channel
				//it does this, in the present code structure
			}
		}
	}
}

//Keeps sending heartbeats until state changes to follower
func (r *Raft) leader() int {
	//fmt.Println("In leader(), I am: ", r.Myconfig.Id)

	r.sendAppendEntriesRPC() //send Heartbeats
	//waitTime := 4            //duration between two heartbeats
	waitTime := 1
	waitTime_secs := secs * time.Duration(waitTime)
	//fmt.Println("Heartbeat time out is", waitTime)

	waitTimeAE := 5                                            //max time to wait for AE_Response
	HeartbeatTimer := r.StartTimer(HeartbeatTimeout, waitTime) //starts the timer and places timeout object on the channel
	//var AppendEntriesTimer *time.Timer
	waitStepDown := 7
	RetryTimer := r.StartTimer(RetryTimeOut, waitStepDown)
	//fmt.Println("I am", r.Myconfig.Id, "timer created", AppendEntriesTimer)
	responseCount := 0
	for {

		req := r.receive() //wait for client append req,extract the msg received on self eventCh
		switch req.(type) {
		case ClientAppendReq:
			//reset the heartbeat timer, now this sendRPC will maintain the authority of the leader
			HeartbeatTimer.Reset(waitTime_secs)
			request := req.(ClientAppendReq)
			data := request.data
			//fmt.Println("Received CA request,cmd is: ", string(data))
			//No check for semantics of cmd before appending to log?
			r.AppendToLog_Leader(data) //append to self log as byte array
			r.sendAppendEntriesRPC()
			responseCount = 0 //for RetryTimer
			//AppendEntriesTimer = r.StartTimer(AppendEntriesTimeOut, waitTimeAE) //Can be written in HeartBeatTimer too
			//fmt.Println("I am", r.Myconfig.Id, "Timer assigned a value", AppendEntriesTimer)
		case AppendEntriesResponse:
			response := req.(AppendEntriesResponse)
			//fmt.Println("got AE_Response! from : ", response.followerId, response)
			responseCount += 1
			if responseCount >= majority {
				waitTime_retry := secs * time.Duration(waitStepDown)
				RetryTimer.Reset(waitTime_retry)
			}
			//when isHeartBeat is true then success is also true according to the code in serviceAEReq so case wont be there when isHB is true and success is false
			// isHB true means it is a succeeded heartbeat hence no work to do if it is AE req then only proceed else do nothing and continue
			//So when follower's log is stale or he is more latest, it would set isHB false
			if !response.isHeartBeat {
				retVal := r.serviceAppendEntriesResp(response, HeartbeatTimer, waitTimeAE, waitTime)
				if retVal == follower {
					return follower
				}

			}

		case AppendEntriesReq: // in case some other leader is also in function, it must fall back or remain leader
			request := req.(AppendEntriesReq)
			if request.term > r.currentTerm {
				//fmt.Println("In leader,AE_Req case, I am ", r.Myconfig.Id, "becoming follower now, because request.term, r.currentTerm", request.term, r.currentTerm)
				r.currentTerm = request.term //update self term and step down
				r.votedFor = -1              //since term has increased so votedFor must be reset to reflect for this term
				r.WriteCVToDisk()
				return follower //sender server is the latest leader, become follower
			} else {
				//reject the request sending false
				reply := AppendEntriesResponse{r.currentTerm, false, r.Myconfig.Id, false, r.myMetaData.lastLogIndex}
				send(request.leaderId, reply)
			}

		case int: //Time out-time to send Heartbeats!
			timeout := req.(int)
			if timeout == RetryTimeOut {
				RetryTimer.Stop()
				return follower
			}
			//fmt.Println("Timeout of", r.Myconfig.Id, "is of type:", timeout)

			//waitTime_secs := secs * time.Duration(waitTime)
			if timeout == HeartbeatTimeout {
				//fmt.Println("Leader:Reseting HB timer")
				HeartbeatTimer.Reset(waitTime_secs)
				responseCount = 0 //since new heartbeat is now being sent
				//it depends on nextIndex which is correctly read in prepAE_Req method,
				//since it was AE other than HB(last entry), it would have already modified the nextIndex map
				r.sendAppendEntriesRPC() //This either sends Heartbeats or retries the failed AE due to which the timeout happened,
				//HeartbeatTimer.Reset(secs * time.Duration(8)) //for checking leader change, setting timer of f4 to 8s--DOESN'T work..-_CHECK
			}

		}
	}
}

func (r *Raft) serviceAppendEntriesResp(response AppendEntriesResponse, HeartbeatTimer *time.Timer, waitTimeAE int, waitTime int) int {
	//fmt.Println("In serviceAE_Response", r.myId(), "received from", response.followerId)
	//fmt.Println("Next index of", f_id, "is", nextIndex, len(r.myLog))
	f_id := response.followerId
	//nextIndex := r.myMetaData.nextIndexMap[f_id]--WRONG
	nextIndex := response.lastLogIndex
	//fmt.Println("Next index of", f_id, "is", nextIndex, len(r.myLog))
	ack := &r.myLog[nextIndex].Acks
	if response.success { //log of followers are consistent and no new leader has come up
		(*ack) += 1                                           //increase the ack count since follower responded true
		follower_nextIndex := r.myMetaData.nextIndexMap[f_id] //read again since, another Append at leader might have modified it
		if follower_nextIndex < r.myMetaData.lastLogIndex {
			//this means this ack came for log repair request, so nextIndex must be advanced till it becomes equal to leader's last index
			r.myMetaData.nextIndexMap[f_id] += 1
			//fmt.Println("Modified nextIndex map", r.myMetaData.nextIndexMap)
		}

	} else { //retry if follower rejected the rpc
		//false is sent it means follower is either more latest or its log is stale!
		//fmt.Println("Follower ", response.followerId, "response.term,r.currentTerm:", response.term, r.currentTerm)
		if response.term > r.currentTerm { //this means another server is more up to date than itself
			r.currentTerm = response.term //update self term with latest leader's term
			r.votedFor = -1               //since term has increased so votedFor must be reset to reflect for this term
			r.WriteCVToDisk()
			//fmt.Println("I am ", r.Myconfig.Id, "becoming follower now\n")
			return follower
		} else { //Log is stale and needs to be repaired!
			//fmt.Println("About to send AE_rpc!!")
			HeartbeatTimer.Reset(time.Duration(waitTime) * secs) //reset HB timer
			r.LogRepair(response)
		}
	}
	//if responseCount >= majority { //convert this nested ifs to && if condition
	//fmt.Println("Responses received from majority")
	if *ack == majority { //are all positive acks? if not wait for responses--WHY NOT >= ?? CHECK
		//fmt.Println("Acks received from majority")
		r.advanceCommitIndex(response.term)
	}
	return -1
}
func (r *Raft) advanceCommitIndex(responseTerm int) {
	//advance commitIndex only when entry from current term has been replicated
	if responseTerm == r.currentTerm { //safety property for commiting entries from older terms
		prevCommitIndex := r.myMetaData.commitIndex
		newCommitIndex := r.myMetaData.lastLogIndex
		//fmt.Println(r.myId(), "prev and new CI are:", prevCommitIndex, newCommitIndex)

		//When commit index advances by more than 1 entry, it should commit(also give for execution to KVStore) all the prev entries too
		//i.e. check if they are already committed(replicated on majority by checking acks
		for i := prevCommitIndex + 1; i <= newCommitIndex; i++ { //it will always be same in the implementation, i.e. loop will run once only
			//fmt.Println(r.myId(), "loop var", i, newCommitIndex)
			//fmt.Println("acks for", i, r.myLog[i].acks)
			if r.myLog[i].Acks >= majority {
				//advance CI
				r.myMetaData.commitIndex += 1

				//fmt.Println(r.myId(), "CI is:", r.myMetaData.commitIndex)
				reply := ClientAppendResponse{}
				data := r.myLog[i].Cmd                               //last entry of leader's log
				logItem := LogItem{r.CurrentLogEntryCnt, true, data} //lsn is count started from 0
				r.CurrentLogEntryCnt += 1
				reply.logEntry = logItem
				// This response ideally comes from kvStore, so here we must send it to commitCh with committed as True
				r.commitCh <- &reply.logEntry
			}
		}

	}
}

//Sends appendRPC requests to all servers and operates according to the responses received i.e. whether to advance the commitIndex or not
//sent by leader, r is leader
func (r *Raft) sendAppendEntriesRPC() {
	appEntriesObj := r.prepAppendEntriesReq() //prepare AppendEntries object

	appEntriesObjSlice := make([]interface{}, len(appEntriesObj))
	//fmt.Println("Prep AE_RPC is:", appEntriesObj)
	//Copy to new slice created--This is the method to send a []interface to []TypeX
	for i, d := range appEntriesObj {
		appEntriesObjSlice[i] = d
	}
	r.sendToAll_AppendReq(appEntriesObjSlice) //send AppendEntries to all the followers
}

//Appends to self log
//adds new entry, modifies last and prev indexes, term
//how to modify the term fields? n who does--candidate state advances terms
func (r *Raft) AppendToLog_Leader(cmd []byte) {
	term := r.currentTerm
	logVal := LogVal{term, cmd, 0} //make object for log's value field with acks set to 0
	//fmt.Println("Before putting in log,", logVal)
	r.myLog = append(r.myLog, logVal)
	//fmt.Println("I am:", r.Myconfig.Id, "Added cmd to my log")

	//modify metadata after appending
	//fmt.Println("Metadata before appending,lastLogIndex,prevLogIndex,prevLogTerm", r.myMetaData.lastLogIndex, r.myMetaData.prevLogIndex, r.myMetaData.prevLogTerm)
	lastLogIndex := r.myMetaData.lastLogIndex + 1
	r.myMetaData.prevLogIndex = r.myMetaData.lastLogIndex
	r.myMetaData.lastLogIndex = lastLogIndex
	//fmt.Println(r.myId(), "Length of my log is", len(r.myLog))
	if len(r.myLog) == 1 {
		r.myMetaData.prevLogTerm = r.myMetaData.prevLogTerm + 1 //as for empty log prevLogTerm is -2

	} else if len(r.myLog) > 1 { //explicit check, else would have sufficed too, just to eliminate len=0 possibility
		r.myMetaData.prevLogTerm = r.myLog[r.myMetaData.prevLogIndex].Term
	}
	//r.currentTerm = term
	//fmt.Println("I am leader, Appended to log, last index, its term is", r.myMetaData.lastLogIndex, r.myLog[lastLogIndex].term)
	//fmt.Println("Metadata after appending,lastLogIndex,prevLogIndex,prevLogTerm", r.myMetaData.lastLogIndex, r.myMetaData.prevLogIndex, r.myMetaData.prevLogTerm)
	r.setNextIndex_All() //Added-28 march for LogRepair
	//Write to disk
	//fmt.Println(r.myId(), "In append_leader, appended to log", string(cmd))
	r.WriteLogToDisk()

}

//This is called by a follower since it should be able to overwrite for log repairs
func (r *Raft) AppendToLog_Follower(request AppendEntriesReq) {
	term := request.term
	cmd := request.entries
	index := request.prevLogIndex + 1
	logVal := LogVal{term, cmd, 0} //make object for log's value field

	if len(r.myLog) == index {
		r.myLog = append(r.myLog, logVal) //when trying to add a new entry
	} else {
		r.myLog[index] = logVal //overwriting in case of log repair
		//fmt.Println("Overwiriting!!")
	}
	//fmt.Println(r.myId(), "Append to log", string(cmd))
	//modify metadata after appending
	//r.myMetaData.lastLogIndex = r.myMetaData.lastLogIndex + 1
	//r.myMetaData.prevLogIndex = r.myMetaData.lastLogIndex
	//	if len(r.myLog) == 1 {
	//		r.myMetaData.prevLogTerm = r.myMetaData.prevLogTerm + 1
	//	} else if len(r.myLog) > 1 {
	//		r.myMetaData.prevLogTerm = r.myLog[r.myMetaData.prevLogIndex].Term
	//	}

	//Changed on 4th april, above is wrong in case of overwriting of log
	r.myMetaData.lastLogIndex = index
	r.myMetaData.prevLogIndex = index - 1
	if index == 0 {
		r.myMetaData.prevLogTerm = r.myMetaData.prevLogTerm + 1 //or simple -1
	} else if index >= 1 {
		r.myMetaData.prevLogTerm = r.myLog[index-1].Term
	}

	//Update commit index
	leaderCI := float64(request.leaderCommitIndex)
	myLI := float64(r.myMetaData.lastLogIndex)
	if request.leaderCommitIndex > r.myMetaData.commitIndex {
		if myLI == -1 { //REDUNDANT since Append to log will make sure it is never -1,also must not copy higher CI if self LI is -1
			r.myMetaData.commitIndex = int(leaderCI)
		} else {
			r.myMetaData.commitIndex = int(math.Min(leaderCI, myLI))
		}
	}
	//fmt.Println(r.myId(), "My CI is:", r.myMetaData.commitIndex)
	r.WriteLogToDisk()
}

//Modifies the next index in the map and returns
func (r *Raft) LogRepair(response AppendEntriesResponse) {
	id := response.followerId
	//fmt.Println("In log repair for ", id)
	failedIndex := r.myMetaData.nextIndexMap[id]
	var nextIndex int
	//fmt.Println("Failed index is:", failedIndex)
	if failedIndex != 0 {
		nextIndex = failedIndex - 1 //decrementing follower's nextIndex

	} else { //if nextIndex is 0 means, follower doesn't have first entry (of leader's log),so decrementing should not be done, so retry the same entry again!
		nextIndex = failedIndex
	}
	//Added--3:38-23 march
	r.myMetaData.nextIndexMap[id] = nextIndex
	//fmt.Println("I am", response.followerId, "My Old and new NI are", failedIndex, nextIndex)
	return
}

//Follower receives AEReq and appends to log checking the request objects fields
//also updates leader info after reseting timer or appending to log
func (r *Raft) serviceAppendEntriesReq(request AppendEntriesReq, HeartBeatTimer *time.Timer, waitTime int) {
	//replicates entry wise , one by one
	waitTime_secs := secs * time.Duration(waitTime)

	//fmt.Println("Hearbeat came to", r.Myconfig.Id, "my and request terms are:", r.currentTerm, request.term)
	appEntriesResponse := AppendEntriesResponse{} //make object for responding to leader
	appEntriesResponse.followerId = r.Myconfig.Id
	appEntriesResponse.success = false     //false by default
	appEntriesResponse.isHeartBeat = false //by default
	var myLastIndexTerm, myLastIndex int
	myLastIndex = r.myMetaData.lastLogIndex
	//fmt.Println("I am", r.Myconfig.Id, "checking if valid leader:", request.leaderId)
	if request.term >= r.currentTerm { //valid leader
		//fmt.Println("I am", r.Myconfig.Id, "this is valid leader:", request.leaderId)
		r.LeaderConfig.Id = request.leaderId //update leader info
		r.currentTerm = request.term         //update self term
		HeartBeatTimer.Reset(waitTime_secs)  //reset the timer if this is HB or AE req from valid leader
		if len(r.myLog) == 0 {               //if log is empty
			myLastIndexTerm = -1
		} else {
			myLastIndexTerm = r.myLog[myLastIndex].Term
		}
		//This is a HB,here log is empty on both sides so term must not be checked (as leader has incremented its term due to elections)
		if request.entries == nil && myLastIndex == request.leaderLastLogIndex {
			//case when first condition is true and 2nd fails wont come,since AE comes from a leader with
			//empty log(hence entries nil) whereas follower has values(2nd condition mismatch)
			appEntriesResponse.success = true
			appEntriesResponse.isHeartBeat = true
			//fmt.Println("I am:", r.Myconfig.Id, "Log empty, HB received!(In serviceAppendReq)")
		} else { //log has data so-- for hearbeat, check the index and term of last entry
			if request.leaderLastLogIndex == myLastIndex && request.term == myLastIndexTerm {
				//this is heartbeat as last entry is already present in self log
				appEntriesResponse.success = true
				appEntriesResponse.isHeartBeat = true
				r.myMetaData.commitIndex = request.leaderCommitIndex //update the CI for last entry that leader got majority acks for!
				//fmt.Println("I am", r.Myconfig.Id, "this is valid leader:", request.leaderId, "got HB", r.myMetaData.commitIndex)
			} else { //this is not a heartbeat but append request
				//fmt.Println("I am:", r.Myconfig.Id, "This is append request \n r.currentTerm,mylastTerm,req.prevLogTerm,mylastIndex,req.prevLogIndex", r.currentTerm, myLastIndexTerm, request.prevLogTerm, myLastIndex, request.prevLogIndex)
				//fmt.Println("I am:", r.Myconfig.Id, "This is append request", string(request.entries))
				//term and index of self last entry and request's previous entries must be checked
				//but what if r.current term has increased to more than the term of last log entry due to repeated elections but no CA req during that time
				//so extract the last term from self log--previously it was being compared to r.currentTerm--FAILING NOW--FIXED
				if request.prevLogTerm == myLastIndexTerm && request.prevLogIndex == myLastIndex { //log is consistent till now
					//fmt.Println("Log is consistent till now! Going to append new entry")
					r.AppendToLog_Follower(request) //append to log
					//fmt.Println(r.myId(), "Appended to log,sending true for", string(request.entries))
					r.currentTerm = request.term
					appEntriesResponse.success = true
					appEntriesResponse.isHeartBeat = false
				}
			}
		}
	}

	appEntriesResponse.term = r.currentTerm
	appEntriesResponse.lastLogIndex = r.myMetaData.lastLogIndex
	//fmt.Println("Response sent by", r.Myconfig.Id, "is :", appEntriesResponse.success, "to", request.leaderId)

	//fmt.Printf("Follower %v sent the AE_ack to %v \n", r.Myconfig.Id, request.leaderId)
	//Where is it sending to leader's channel??--Added
	send(request.leaderId, appEntriesResponse)
}

//r is follower who received the request
//Services the received request for vote
func (r *Raft) serviceRequestVote(request RequestVote) {
	//fmt.Println("In service RV method of ", r.Myconfig.Id)
	response := RequestVoteResponse{} //prep response object,for responding back to requester
	candidateId := request.candidateId
	response.id = r.Myconfig.Id
	//fmt.Println("Follower", r.Myconfig.Id, "log as complete?", r.logAsGoodAsMine(request))
	if r.isDeservingCandidate(request) {
		response.voteGranted = true
		r.votedFor = candidateId
		r.currentTerm = request.term

		//Writing current term and voteFor to disk
		r.WriteCVToDisk()

	} else {
		response.voteGranted = false
	}
	response.term = r.currentTerm //to return self's term too

	//fmt.Println("Follower", r.Myconfig.Id, "voting", response.voteGranted) //"because votefor is", r.votedFor, "my and request terms are:", r.currentTerm, request.term)
	//fmt.Println("Follower", r.Myconfig.Id, "Current term,request.term is", r.currentTerm, request.term, "Self lastLogIndex is", r.myMetaData.lastLogIndex, "VotedFor,request.lastLogTerm", r.votedFor, request.lastLogTerm)
	//fmt.Println("VotedFor,request.lastLogTerm", r.votedFor, request.lastLogTerm)

	//fmt.Printf("In serviceRV of %v, obj prep is %v \n", r.Myconfig.Id, response)
	send(candidateId, response) //send to sender using send(sender,response)
}

func (r *Raft) WriteCVToDisk() {

	fh_CV, err1 := os.OpenFile(r.path_CV, os.O_RDWR|os.O_APPEND, 0600)
	if err1 != nil {
		panic(err1)
	}
	//	fh_CV.Write([]byte(strconv.Itoa(r.currentTerm)))
	//	fh_CV.Write([]byte(strconv.Itoa(r.votedFor)))

	//Using gob--Dots are coming??--Convert to JSON(which is readable) for testing purposes
	cv_encoder := json.NewEncoder(fh_CV)
	cv_encoder.Encode(r.currentTerm)
	cv_encoder.Encode(r.votedFor)
	fh_CV.Close()
}

func (r *Raft) WriteLogToDisk() {

	fh_Log, err1 := os.OpenFile(r.path_Log, os.O_RDWR|os.O_APPEND, 0600)
	if err1 != nil {
		panic(err1)
	}
	//Using json--NOT WORKING--WORKs
	log_encoder := json.NewEncoder(fh_Log)
	//	index := r.myMetaData.lastLogIndex
	//	index_str := strconv.Itoa(index)
	//	data := map[string]LogVal{index_str: r.myLog[index]}
	//	log_encoder.Encode(data)
	log_m, _ := json.Marshal(r.myLog)
	log_encoder.Encode(string(log_m))
	fh_Log.Close()

}
func (r *Raft) isDeservingCandidate(request RequestVote) bool {
	//if candidate term is greater don't check votedFor as it will be the last term value and since term is greater it means it hasn't voted in this term
	//If term is equal check votedFor
	//in both cases ,log completion is checked mandatorily
	return ((request.term > r.currentTerm && r.logAsGoodAsMine(request)) || (request.term == r.currentTerm && r.logAsGoodAsMine(request) && (r.votedFor == -1 || r.votedFor == request.candidateId)))
}

func (r *Raft) logAsGoodAsMine(request RequestVote) bool {
	//fmt.Println("Follower", r.Myconfig.Id, "Granting vote as", (request.lastLogTerm > r.currentTerm || (request.lastLogTerm == r.currentTerm && request.lastLogIndex >= r.myMetaData.lastLogIndex)))
	return (request.lastLogTerm > r.currentTerm || (request.lastLogTerm == r.currentTerm && request.lastLogIndex >= r.myMetaData.lastLogIndex))

}

func (r *Raft) resetVotes() {
	for i := 0; i < noOfServers; i++ {
		r.f_specific[i].vote = false
	}
}

//preparing object for replicating log value at nextIndex, one for each follower depending on nextIndex read from nextIndexMap
func (r *Raft) prepAppendEntriesReq() (appendEntriesReqArray [noOfServers]AppendEntriesReq) {
	for i := 0; i < noOfServers; i++ {
		if i != r.Myconfig.Id {
			nextIndex := r.myMetaData.nextIndexMap[i] //read the nextIndex to be sent from map
			leaderId := r.LeaderConfig.Id
			var entries []byte
			var term, prevLogIndex, prevLogTerm int

			//if len(r.myLog) != 0 { //removed since, in case of decrementing nextIndexes for log repair, log length is never zero but nextIndex becomes -1
			if nextIndex >= 0 { //this is AE request with last entry sent (this will be considered as HB when log of follower is consistent)
				//fmt.Println("Next index is", nextIndex, "for server", i)
				term = r.myLog[nextIndex].Term
				entries = r.myLog[nextIndex].Cmd //entry to be replicated
				prevLogIndex = nextIndex - 1     //should be changed to nextIndex-1
				if nextIndex == 0 {
					prevLogTerm = -1 //since indexing will be log[-1] so it must be set explicitly
				} else {
					prevLogTerm = r.myLog[prevLogIndex].Term //this is the way to get new prevLogTerm to be sent
				}

			} else { //so this is prepReq for heartbeat for empty as nextIndex is -1
				//when log is empty indexing to log shouldn't be done hence copy old values
				term = r.currentTerm
				entries = nil
				prevLogIndex = r.myMetaData.prevLogIndex
				prevLogTerm = r.myMetaData.prevLogTerm
			}

			leaderCommitIndex := r.myMetaData.commitIndex
			leaderLastLogIndex := r.myMetaData.lastLogIndex
			appendEntriesObj := AppendEntriesReq{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommitIndex, leaderLastLogIndex}
			appendEntriesReqArray[i] = appendEntriesObj
		}

	}
	return appendEntriesReqArray

}

//prepares the object for sending to RequestVoteRPC, requesting the vote
func (r *Raft) prepRequestVote() RequestVote {
	lastLogIndex := r.myMetaData.lastLogIndex
	//if this is the request when log is empty
	var lastLogTerm int
	if len(r.myLog) == 0 {
		//fmt.Println("In if of prepRV()")
		//lastLogTerm = -1 //Just for now--Modify later
		lastLogTerm = r.currentTerm
	} else {
		//fmt.Println("In else of prepRV()")
		lastLogTerm = r.myLog[lastLogIndex].Term
	}
	//fmt.Println("here2")
	reqVoteObj := RequestVote{r.currentTerm, r.Myconfig.Id, lastLogIndex, lastLogTerm}
	return reqVoteObj
}

//Starts the timer with appropriate random number secs, also timeout object is needed for distinguishing between different timeouts
func (r *Raft) StartTimer(timeoutObj int, waitTime int) (timerObj *time.Timer) {
	//expInSec := secs * time.Duration(waitTime) //gives in seconds
	expInSec := time.Duration(waitTime) * time.Millisecond
	timerObj = time.AfterFunc(expInSec, func() {
		r.TimeOut(timeoutObj)
	})
	return
}

//Places timeOutObject on the eventCh of the caller
func (r *Raft) TimeOut(timeoutObj int) {
	r.eventCh <- timeoutObj
}

func (r *Raft) setNextIndex_All() {
	nextIndex := r.myMetaData.lastLogIndex //given as lastLogIndex+1 in paper..don't know why,seems wrong.
	//fmt.Println("In setNextIndex_All,nextIndex is", nextIndex, r.myId())
	for k := range server_raft_map {
		if r.Myconfig.Id != k {
			r.myMetaData.nextIndexMap[k] = nextIndex
		}
	}
	//	fmt.Println("In setNextIndex_All,map prep is:", r.myMetaData.nextIndexMap)
	return
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return string(e)
}

//checks the number of true votes in follower_specific member
func (r *Raft) countVotes() (voteCount int) {
	for i := 0; i < noOfServers; i++ {
		if r.f_specific[i].vote {
			voteCount += 1
		}
	}

	return
}

//Calls append func and writes the value returned by it in myChan
func (r *Raft) Client(myChan chan LogEntry, data string) {
	//fmt.Println("Client:", data, r.myId())
	logItem, err := r.Append([]byte(data))
	if err != nil {
		//redirect to leader???--ASK!
	} else {
		myChan <- logItem
	}
}

//For testing
func (r *Raft) myId() string {
	return "I am " + strconv.Itoa(r.Myconfig.Id)
}

func getCrash() bool {
	globMutex.RLock()
	crash_var := crash
	globMutex.RUnlock()
	return crash_var
}

func setCrash(c bool) {
	globMutex.Lock()
	crash = c
	globMutex.Unlock()
}

func getServerToCrash() int {
	globMutex.RLock()
	s_var := server_to_crash
	globMutex.RUnlock()
	return s_var
}

func setServerToCrash(s int) {
	globMutex.Lock()
	server_to_crash = s
	globMutex.Unlock()
}
