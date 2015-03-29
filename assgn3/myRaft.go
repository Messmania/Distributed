package raft

import (
	//"log"
	"math"
	//"math/rand"
	//"strings"
	"fmt"
	//"io/ioutil"
	"os"
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
	//clientCh  chan ClientAppendResponse
	eventCh   chan interface{}
	voteCount int

	//Persisitent state on all servers
	currentTerm int
	votedFor    int
	//myLog       LogDB
	myLog []LogVal
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

	obj := ClientAppendReq{data}
	send(r.Myconfig.Id, obj)
	//fmt.Println("In Append,data sent to channel of", r.Myconfig.Id)
	response := <-r.commitCh
	//fmt.Println("Response received on commit channel", response)
	//return response.logEntry, response.ret_error
	return *response, nil
	//}
}

type ErrRedirect int

//==========================Addition for assgn3============
//temp for testing
//const layout = "3:04:5 pm (MST)"

//For converting default time unit of ns to secs
var secs time.Duration = time.Duration(math.Pow10(9))

const majority int = 3
const noOfServers int = 5
const (
	ElectionTimeout      = iota
	HeartbeatTimeout     = iota
	AppendEntriesTimeOut = iota
)

//type logDB map[int]LogVal //map is not ordered!! Change this to array or linked list--DONE(changed to array)
//type LogDB []LogVal //log is array of type LogVal
type LogVal struct {
	term int
	cmd  []byte //should it be logEntry type?, should not be as isCommited and lsn are useless to store in log
	//they are needed only for kvStore processing i.e. logEntry must be passed to commitCh

	//This is added for commiting entries from prev term--So when there are entries from prev term in leaders log but have not been replicated on mjority
	//then there must be a flag which is checked before advancing the commitIndex, if false, commit(replicate on maj) them first!
	//used only by leader
	acks int
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
	term        int
	success     bool
	followerId  int
	isHeartBeat bool
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
	//fmt.Println("Before writing to channel of:", serverId)
	raftObj.eventCh <- msg
	//fmt.Println("After writing to channel of:", serverId)
}

func (r *Raft) receive() interface{} {
	//waits on eventCh
	request := <-r.eventCh
	return request
}

func (r *Raft) sendToAll_AppendReq(msg []interface{}) {
	//fmt.Println("Server-Raft map:", server_raft_map)

	for k := range server_raft_map {
		//fmt.Println("Id from map is:", k, r.Myconfig.Id)
		if r.Myconfig.Id != k { //send to all except self
			//fmt.Println("Sending HB to ", k)
			go send(k, msg[k])
			//fmt.Println("After sending RPCS")

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

			go send(k, msg)
			//fmt.Println("After sending RV")
		}
	}

}

//Keeps looping and performing follower functions till it timesout and changes to candidate
func (r *Raft) follower(timeout int) int {
	//myId := r.Myconfig.Id
	///fmt.Println("In follower()", myId)
	//start heartbeat timer,timeout func wil place HeartbeatTimeout on channel
	waitTime := timeout //use random number after func is tested--PENDING
	HeartBeatTimer := r.StartTimer(HeartbeatTimeout, waitTime)
	for {
		req := r.receive()
		switch req.(type) {
		case AppendEntriesReq:
			//fmt.Printf("Follower %v got heartbeat \n", myId)
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
			//fmt.Println("In follower timeout", r.Myconfig.Id)
			HeartBeatTimer.Stop() //turn off timer as now election timer will start in candidate() mode
			return candidate
		}
	}
}

//conducts election, returns only when state is changed else keeps looping on outer loop(i.e. restarting elections)
func (r *Raft) candidate() int {
	//myId := r.Myconfig.Id
	//fmt.Println("In candidate()", myId)
	//--start election timer for election-time out time, so when responses stop coming it must restart the election
	waitTime := 12
	ElectionTimer := r.StartTimer(ElectionTimeout, waitTime)
	//This loop is for election process which keeps on going until a leader is elected
	for {
		r.currentTerm = r.currentTerm + 1 //increment current term
		//fmt.Println("I am canditate", r.Myconfig.Id, "and current term is:", r.currentTerm)
		r.votedFor, r.voteCount = r.Myconfig.Id, 1 //vote for self
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
					r.voteCount = r.voteCount + 1
				}
				if r.voteCount >= majority {
					//fmt.Println("Votecount is majority, I am new leader", r.Myconfig.Id)
					ElectionTimer.Stop()
					r.LeaderConfig.Id = r.Myconfig.Id //update leader details
					return leader                     //become the leader
				}

			case AppendEntriesReq: //received an AE request instead of votes, i.e. some other leader has been elected
				request := req.(AppendEntriesReq)
				//Can be clubbed with serviceAppendEntriesReq with few additions!--SEE LATER
				waitTime_secs := secs * time.Duration(waitTime)
				appEntriesResponse := AppendEntriesResponse{}
				appEntriesResponse.followerId = r.Myconfig.Id
				appEntriesResponse.success = false //false by default, in case of heartbeat or invalid leader
				if request.term >= r.currentTerm { //valid leader
					r.LeaderConfig.Id = request.leaderId //update leader info
					ElectionTimer.Reset(waitTime_secs)   //reset the timer
					myLastIndexTerm := r.myLog[r.myMetaData.lastLogIndex].term
					if request.leaderLastLogIndex == r.myMetaData.lastLogIndex && request.term == myLastIndexTerm { //this is heartbeat from a valid leader
						appEntriesResponse.success = true
					}
					send(request.leaderId, appEntriesResponse)
					return follower
				} else {
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
	fmt.Println("In leader(), I am: ", r.Myconfig.Id)
	/*start heartbeat-sending timer, after timeout send heartbeats to all servers--using r.sendToAll(heartbeat) and keep checking
	if it is still the leader before sending heartbeat i.e. in timeOut func if(leader) then send HB!
	or
	fire a go routine which loops forever and sends heartbeats after a fix time--What if this leader is demoted?
	//send heartbeat immediately , when elected leader
	Then terminate the func */
	//r.setNextIndex_All()
	r.sendAppendEntriesRPC()                                   //send Heartbeats
	waitTime := 4                                              //duration between two heartbeats
	waitTimeAE := 2                                            //max time to wait for AE_Response
	HeartbeatTimer := r.StartTimer(HeartbeatTimeout, waitTime) //starts the timer and places timeout object on the channel
	var AppendEntriesTimer *time.Timer
	//ack := 0
	responseCount := 0
	for {
		req := r.receive() //wait for client append req,extract the msg received on self eventCh
		switch req.(type) {
		case ClientAppendReq:
			HeartbeatTimer.Stop() // request for append will be sent now and will server as Heartbeat too
			request := req.(ClientAppendReq)
			data := request.data
			fmt.Println("Received CA request,cmd is: ", string(data))
			//No check for semantics of cmd before appending to log?
			r.AppendToLog_Leader(r.currentTerm, data) //append to self log as byte array
			fmt.Println("I am:", r.Myconfig.Id, "Appended to log! lastindex is", r.myMetaData.lastLogIndex)
			r.setNextIndex_All() //Added-28 march for LogRepair
			//reset the heartbeat timer, now this sendRPC will maintain the authority of the leader

			//ack = 0                                                             //reset the prev ack values
			r.sendAppendEntriesRPC()                                            //random value as max time for receiving AE_responses
			AppendEntriesTimer = r.StartTimer(AppendEntriesTimeOut, waitTimeAE) //Can be written in HeartBeatTimer too
		case AppendEntriesResponse:
			response := req.(AppendEntriesResponse)
			fmt.Println("got AE_Response! from : ", response.followerId, response)
			//r.serviceAppendEntriesResponse(AppendEntriesTimer)
			serverCount := len(r.ClusterConfigObj.Servers)
			responseCount += 1

			//Set fields to be used further
			//when isHeartBeat is true then success is also true according to the code in serviceAEReq so case wont be there when isHB is true and success is false
			// isHB true means it is a succeeded heartbeat hence no work to do
			//If it is AE req then only proceed else do nothing and continue
			//So when follower's log is stale or he is more latest, it would set isHB false
			if !response.isHeartBeat {
				f_id := response.followerId
				nextIndex := r.myMetaData.nextIndexMap[f_id]
				fmt.Println("Next index of", f_id, "is", nextIndex)

				ack := &r.myLog[nextIndex].acks //fails for the first HB response because NI is -1,

				if response.success { //log of followers are consistent and no new leader has come up
					//ack += 1
					//fmt.Println("Got ack!!", ack)
					//This follower's log is consistent and regular HBs to be sent to it.

					//If this response is from logRepair, then update nextIndexMap also , this ack is not counted in total ack--PENDING
					//Get the entry which was sent to follower using nextIndexMap and increment its ack count when follower sent true
					//Do not increase ack count if response is for heartbeat, else same follower will keep sending true and increasing the ack count
					//while others log is being repaired
					//if !response.isHeartBeat { //--REDUNDANT NOW
					*ack += 1 //increase the ack count since follower responded true
					follower_nextIndex := r.myMetaData.nextIndexMap[f_id]
					if follower_nextIndex < r.myMetaData.lastLogIndex {
						//this means this ack came for log repair request, so nextIndex must be advanced till it becomes equal to leader's last index
						r.myMetaData.nextIndexMap[f_id] += 1
					}
					//}

				} else { //retry if follower rejected the rpc
					//false is sent it means follower is either more latest or its log is stale!
					//fmt.Println("response.term,r.currentTerm:", response.term, r.currentTerm)
					if response.term > r.currentTerm { //this means another server is more up to date than itself
						r.currentTerm = response.term
						return follower
					} else { //Log is stale and needs to be repaired!

						//This follower needs a log repair..while doing this, what if HB timer of other followers times out??
						//PUT THIS IN A logrepair func--DONE

						//fmt.Println("About to send AE_rpc!!")
						//This means new AE_RPC is being sent for log repair by decrementing the nextIndex to be sent for replication
						//so timer must be reset
						AppendEntriesTimer.Reset(time.Duration(waitTimeAE) * secs)
						r.LogRepair(response)

					}
				}
				//if responseCount >= majority { //convert this nested ifs to && if condition
				fmt.Println("Responses received from majority")
				if *ack == majority { //are all positive acks? if not wait for responses
					fmt.Println("Acks received from majority")
					//advance commitIndex only when entry from current term has been replicated
					if response.term == r.currentTerm { //safety property for commiting entries from older terms
						prevCommitIndex := r.myMetaData.commitIndex
						currentCommitIndex := r.myMetaData.lastLogIndex
						r.myMetaData.commitIndex = currentCommitIndex //commitIndex advances,Entries committed!
						//Write to file!--Testing--WORKS! but when to create the file?
						data := r.myLog[currentCommitIndex].cmd
						fh, err := os.OpenFile("TestWriteToFile", os.O_RDWR|os.O_APPEND, 0600)
						fmt.Println("Error in opening file is", err)
						if err == nil {
							fh.Write(data)
							fmt.Println("Wrote data")
							fh.Close()
						} else {
							//log the error
						}

						//Testing ends
						//When commit index advances by more than 1 entry, it commits all the prev entries too
						for i := prevCommitIndex + 1; i <= currentCommitIndex; i++ {
							//fmt.Println("In for loop of commit")
							//CHECK IF ALL ENTRIES HAVE RECEIVED Majority responses! If not replicate them first--PENDING
							//if !r.myLog[i].majorityReceived {
							//these have not been committed yet! Replicate them first
							//call prep AE, send to all
							//only when this if condition is false it should go to statements after this else control diverts from here only

							//}
							fmt.Println("About to commit")
							reply := ClientAppendResponse{}
							data := r.myLog[i].cmd                               //last entry of leader's log
							logItem := LogItem{r.CurrentLogEntryCnt, true, data} //lsn is count started from 0
							r.CurrentLogEntryCnt += 1
							reply.logEntry = logItem
							// This response ideally comes from kvStore, so here we must send it to commitCh with committed as True
							r.commitCh <- &reply.logEntry
						}

					}
				}
				//But what if followers crash? Then it keeps waiting and retries after time out
				//if server crashes this process or func ends or doesn't execute this statement
				//this means server must try to replicate entries on all even after it is committed--given in paper
				//majority response is needed to advance commit index
				//CHECK THIS
				if *ack == serverCount { //entry is replicated on all servers! else keep waiting for acks!
					AppendEntriesTimer.Stop()
				}
			}

		case AppendEntriesReq: // in case some other leader is also in function, it must fall back or remain leader
			request := req.(AppendEntriesReq)
			if request.term > r.currentTerm {
				r.currentTerm = request.term //update self term and step down
				return follower              //sender server is the latest leader, become follower
			} else {
				//reject the request sending false
				reply := AppendEntriesResponse{r.currentTerm, false, r.Myconfig.Id, false}
				send(request.leaderId, reply)
			}

		case int: //Time out-time to send Heartbeats!
			fmt.Println("\n\nTime to send HBs!")
			timeout := req.(int)
			//fmt.Println("Timeout is of type:", timeout, HeartbeatTimeout)
			r.sendAppendEntriesRPC() //send Heartbeats
			waitTime_secs := secs * time.Duration(waitTime)
			if timeout == HeartbeatTimeout {
				//fmt.Println("Leader:Reseting HB timer")
				HeartbeatTimer.Reset(waitTime_secs)
				//HeartbeatTimer.Reset(secs * time.Duration(8)) //for checking leader change, setting timer of f4 to 8s--DOESN'T work..-_CHECK
			} else if timeout == AppendEntriesTimeOut {
				AppendEntriesTimer.Reset(waitTime_secs) //start the timer again, does this call the TimeOut method again??-YES
			}
		}
	}
}

//Sends appendRPC requests to all servers and operates according to the responses received i.e. whether to advance the commitIndex or not
//sent by leader, r is leader
func (r *Raft) sendAppendEntriesRPC() {
	//r.setNextIndex_All()
	appEntriesObj := r.prepAppendEntriesReq() //prepare AppendEntries object

	appEntriesObjSlice := make([]interface{}, len(appEntriesObj))
	fmt.Println("Prep AE_RPC is:", appEntriesObj)
	//Copy to new slice created
	for i, d := range appEntriesObj {
		appEntriesObjSlice[i] = d
	}
	r.sendToAll_AppendReq(appEntriesObjSlice) //send AppendEntries to all the followers
}

//Appends to self log
//adds new entry, modifies last and prev indexes, term
//how to modify the term fields? n who does--candidate state advances terms
func (r *Raft) AppendToLog_Leader(term int, cmd []byte) {

	logVal := LogVal{term, cmd, 0} //make object for log's value field with acks set to 0
	//fmt.Println("Before putting in log,", logVal)
	r.myLog = append(r.myLog, logVal)
	//r.myLog[index] = logVal //append to log, when array is dynamic, i.e. slice, indexing can't be used like normal array
	//fmt.Println("I am:", r.Myconfig.Id, "Added cmd to my log")

	//modify metadata after appending
	lastLogIndex := r.myMetaData.lastLogIndex + 1
	r.myMetaData.lastLogIndex = lastLogIndex
	r.myMetaData.prevLogIndex = r.myMetaData.lastLogIndex
	if len(r.myLog) == 1 {
		r.myMetaData.prevLogTerm = r.myMetaData.prevLogTerm + 1
	} else if len(r.myLog) > 1 {
		r.myMetaData.prevLogTerm = r.myLog[r.myMetaData.prevLogIndex].term
	}
	r.currentTerm = term
}

//This is called by a follower since it should be able to overwrite for log repairs
//Check how to do overwriting in slices--PENDING
func (r *Raft) AppendToLog_Follower(term int, cmd []byte) {
	//r.myMetaData.prevLogIndex = r.myMetaData.lastLogIndex

	logVal := LogVal{term, cmd, 0} //make object for log's value field
	//fmt.Println("Before putting in log,", logVal)

	//How to overwrite in slices???---PENDING
	r.myLog = append(r.myLog, logVal)
	//r.myLog[index] = logVal //append to log, when array is dynamic, i.e. slice, indexing can't be used like normal array
	//fmt.Println("I am:", r.Myconfig.Id, "Added cmd to my log")

	//modify metadata after appending
	r.myMetaData.lastLogIndex = r.myMetaData.lastLogIndex + 1
	r.myMetaData.prevLogIndex = r.myMetaData.lastLogIndex
	if len(r.myLog) == 1 {
		r.myMetaData.prevLogTerm = r.myMetaData.prevLogTerm + 1
	} else if len(r.myLog) > 1 {
		r.myMetaData.prevLogTerm = r.myLog[r.myMetaData.prevLogIndex].term
	}
	r.currentTerm = term

}

func (r *Raft) LogRepair(response AppendEntriesResponse) {
	id := response.followerId
	//fmt.Println("Follower:", id)
	failedIndex := r.myMetaData.nextIndexMap[id]
	var nextIndex int
	//fmt.Println("Failed index is:", failedIndex)
	if nextIndex != 0 {
		nextIndex = failedIndex - 1 //decrementing follower's nextIndex

	} else { //if nextIndex is 0 means, follower doesn't have first entry (of leader's log), so retry the same entry again!
		nextIndex = failedIndex
	}
	//Added--3:38-23 march
	fmt.Println("About to modify next index map")
	r.myMetaData.nextIndexMap[id] = nextIndex
	return
}

//Follower receives AEReq and appends to log checking the request objects fields
//also updates leader info after reseting timer or appending to log
func (r *Raft) serviceAppendEntriesReq(request AppendEntriesReq, HeartBeatTimer *time.Timer, waitTime int) {
	//replicates entry wise , one by one
	waitTime_secs := secs * time.Duration(waitTime)

	//===========for testing--reducing timeout of f4 to see if he becomes leader in next term
	//	if r.Myconfig.Id == 4 {
	//		waitTime_secs = 1
	//	}
	//===Testing ends==========
	//fmt.Println("Hearbeat came to", r.Myconfig.Id, "my and request terms are:", r.currentTerm, request.term)
	appEntriesResponse := AppendEntriesResponse{} //make object for responding to leader
	appEntriesResponse.followerId = r.Myconfig.Id
	appEntriesResponse.success = false     //false by default
	appEntriesResponse.isHeartBeat = false //by default
	var myLastIndexTerm, myLastIndex int
	myLastIndex = r.myMetaData.lastLogIndex
	if request.term >= r.currentTerm { //valid leader
		r.LeaderConfig.Id = request.leaderId //update leader info
		r.currentTerm = request.term         //update self term
		HeartBeatTimer.Reset(waitTime_secs)  //reset the timer if this is HB or AE req from valid leader
		if len(r.myLog) == 0 {               //if log is empty
			myLastIndexTerm = -1
		} else {
			myLastIndexTerm = r.myLog[r.myMetaData.lastLogIndex].term
		}

		if request.entries == nil && myLastIndex == request.leaderLastLogIndex { //This is a HB,here log is empty on both sides so term must not be checked (as leader has incremented its term)
			appEntriesResponse.success = true
			appEntriesResponse.isHeartBeat = true
			//fmt.Println("I am:", r.Myconfig.Id, "Log empty, HB received!(In serviceAppendReq)")
		} else { //log has data so-- for hearbeat, check the index and term of last entry
			if request.leaderLastLogIndex == r.myMetaData.lastLogIndex && request.term == myLastIndexTerm { //this is heartbeat as last entry is already present in self log
				appEntriesResponse.success = true
				appEntriesResponse.isHeartBeat = true
			} else {
				//this is not a heartbeat but append request
				fmt.Println("I am:", r.Myconfig.Id, "This is append request")
				if request.prevLogTerm == r.currentTerm && request.prevLogIndex == r.myMetaData.lastLogIndex { //log is consistent till now
					r.AppendToLog_Follower(request.term, request.entries) //append to log, also modifies  r.currentTerm
					appEntriesResponse.success = true
					appEntriesResponse.isHeartBeat = false
				}
			}
		}
	}
	appEntriesResponse.term = r.currentTerm
	fmt.Println("Response sent by", r.Myconfig.Id, "is :", appEntriesResponse.success, "conditions:", request.prevLogTerm, r.currentTerm)
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

	if request.term >= r.currentTerm && r.logAsGoodAsMine(request) && (r.votedFor == -1 || r.votedFor == candidateId) {
		response.voteGranted = true
		r.votedFor = candidateId
		r.currentTerm = request.term
		//write votedFor to disk
	} else {
		response.voteGranted = false
	}
	response.term = r.currentTerm //to return self's term too

	/*===NEW====
	//After discussion --
	//Always compare log length before voting for a candidate
	// if term is greater , it means hasn't voted in this term, so check its log before voting true
	if request.term > r.currentTerm {
		//check the log if
		complete vote for it
		if request.lastLogTerm > r.currentTerm || (request.lastLogTerm == r.currentTerm && request.lastLogIndex >= r.myMetaData.lastLogIndex) { //log is complete hence vote for this
			response.voteGranted = true
			r.votedFor = candidateId
			r.currentTerm = request.term
			//write votedfor to disk
		} else { // this means candidate's log is shorter or older than self log
			response.voteGranted = false
		}

	} else if r.currentTerm == request.term { //same terms so check if already voted in this term, if not then check the log length and then only vote
		if r.votedFor == -1 && (request.lastLogTerm > r.currentTerm || (request.lastLogTerm == r.currentTerm && request.lastLogIndex >= r.myMetaData.lastLogIndex)) {
			response.voteGranted = true
			r.votedFor = candidateId
			//write votedfor to disk
		} else {
			response.voteGranted = false
		}

	} else { // this means self term is greater than candidate's term
		response.voteGranted = false
	}
	response.term = r.currentTerm


	*/
	/*====OLD===
	if r.votedFor == -1 && (request.lastLogTerm > r.currentTerm || (request.lastLogTerm == r.currentTerm && request.lastLogIndex >= r.myMetaData.lastLogIndex)) {
		response.voteGranted = true
		r.votedFor = candidateId
	} else {
		response.voteGranted = false
	}
	//	}
	*/

	//fmt.Println("Follower", r.Myconfig.Id, "voting", response.voteGranted) //"because votefor is", r.votedFor, "my and request terms are:", r.currentTerm, request.term)
	//fmt.Println("Follower", r.Myconfig.Id, "Current term is", r.currentTerm, "Self lastLogIndex is", r.myMetaData.lastLogIndex)
	//fmt.Println("VotedFor,request.lastLogTerm", r.votedFor, request.lastLogTerm)

	//fmt.Printf("In serviceRV of %v, obj prep is %v \n", r.Myconfig.Id, response)
	send(candidateId, response) //send to sender using send(sender,response)
}

func (r *Raft) logAsGoodAsMine(request RequestVote) bool {
	//fmt.Println("Follower", r.Myconfig.Id, "Granting vote as", (request.lastLogTerm > r.currentTerm || (request.lastLogTerm == r.currentTerm && request.lastLogIndex >= r.myMetaData.lastLogIndex)))
	return (request.lastLogTerm > r.currentTerm || (request.lastLogTerm == r.currentTerm && request.lastLogIndex >= r.myMetaData.lastLogIndex))

}

//preparing object for replicating log value at nextIndex, one for each follower depending on nextIndex read from nextIndexMap
func (r *Raft) prepAppendEntriesReq() (appendEntriesReqArray [noOfServers]AppendEntriesReq) {
	//func (r *Raft) prepAppendEntriesReq() map[int]AppendEntriesReq {
	//populate fields of AppendEntries RPC
	//fmt.Println("In prepAE, looping now")
	//r is leader object so it has log details
	//fmt.Println("NextIndex is:", nextIndex)
	//Right now, index of array is same as id of server i.e. 0-5 so this would work
	//if say server ids start from 5 6 7 8 9.. or 0 2 3 7 then return object must be a map of ServerId-Obj

	for i := 0; i < noOfServers; i++ {
		//fmt.Println("loop i is:", i, r.Myconfig.Id)
		if i != r.Myconfig.Id {
			//fmt.Println("Inside if")
			nextIndex := r.myMetaData.nextIndexMap[i]
			leaderId := r.LeaderConfig.Id
			var entries []byte
			var term, prevLogIndex, prevLogTerm int
			if len(r.myLog) != 0 {
				//fmt.Println("I am :", r.Myconfig.Id, "next index", nextIndex, r.myMetaData.nextIndexMap)
				//fmt.Println("My log till now is:", r.myLog)
				term = r.myLog[nextIndex].term
				entries = r.myLog[nextIndex].cmd //entry to be replicated
				prevLogIndex = nextIndex - 1     //should be changed to nextIndex-1
				if len(r.myLog) == 1 {
					prevLogTerm = r.myMetaData.prevLogTerm + 1 //should be changed to term corresponding to nextIndex-1 entry
				} else {
					prevLogTerm = r.myLog[prevLogIndex].term
				}
			} else {
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
	//fmt.Println("here")
	//if this is the request when log is empty
	var lastLogTerm int
	if len(r.myLog) == 0 {
		//fmt.Println("In if of prepRV()")
		//lastLogTerm = -1 //Just for now--Modify later
		lastLogTerm = r.currentTerm
	} else {
		//fmt.Println("In else of prepRV()")
		lastLogTerm = r.myLog[lastLogIndex].term
	}
	//fmt.Println("here2")
	reqVoteObj := RequestVote{r.currentTerm, r.Myconfig.Id, lastLogIndex, lastLogTerm}
	return reqVoteObj
}

//Starts the timer with appropriate random number secs
func (r *Raft) StartTimer(timeoutObj int, waitTime int) (timerObj *time.Timer) {
	//waitTime := rand.Intn(10)
	//for testing
	//waitTime := 5
	expInSec := secs * time.Duration(waitTime) //gives in seconds
	//fmt.Printf("Expiry time of %v is:%v \n", timeoutObj, expInSec)
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
	fmt.Println("In setNextIndex_All,nextIndex is", nextIndex)
	for k := range server_raft_map {
		if r.Myconfig.Id != k {
			r.myMetaData.nextIndexMap[k] = nextIndex
		}
	}
	fmt.Println("In setNextIndex_All,map prep is:", r.myMetaData.nextIndexMap)
	return
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return string(e)
}
