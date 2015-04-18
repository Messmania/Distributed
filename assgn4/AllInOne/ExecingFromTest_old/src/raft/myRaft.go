package raft

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
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
	Id         int    // Id of server. Must be unique
	Hostname   string // name or ip of host
	ClientPort int    // port at which server listens to client messages.
	LogPort    int    // tcp port for inter-replica protocol messages.
}

type ClusterConfig struct {
	Servers []ServerConfig // All servers in this cluster
}

type SharedLog interface {
	Append(Data []byte) (LogEntry, error)
}

// Raft implements the SharedLog interface.
type Raft struct {
	ClusterConfigObj   ClusterConfig
	Myconfig           ServerConfig
	LeaderConfig       ServerConfig
	CurrentLogEntryCnt LSN
	CommitCh           chan *LogEntry

	//====for assgn3==============================
	//clientCh  chan ClientAppendResponse
	EventCh chan interface{}

	//Persisitent state on all servers--ON DISK
	CurrentTerm int
	VotedFor    int
	MyLog       []LogVal

	//Volatile state on all servers
	MyMetaData LogMetaData

	//used by leader only
	f_specific map[int]*followerDetails //map of serverId-details

	//Path strings to files on disk
	Path_CV  string
	Path_Log string
}

//used by leader to track details specific to a follower
type followerDetails struct {
	Vote bool
	conn net.Conn
	//include next index here--later
	//nextIndex int
}

func (r *Raft) Append(Data []byte) (LogEntry, error) {
	obj := ClientAppendReq{Data}
	//fmt.Println("Raft obj is", r)
	r.send(r.Myconfig.Id, obj) //ADD CRASH CONDITION for append--PENDING
	//	fmt.Println(r.myId(), "In Append,Data sent to channel", string(Data))
	response := <-r.CommitCh
	//fmt.Println("Response received on commit channel", (*response).Committed())

	if !(*response).Committed() { //if committed is false then it means this server is not the leader--
		// what if server is the leader but entry is not committed, will it be false then or it doesn't write to chann? mp it doesn't write to CommitCh--CHECK
		var e error = ErrRedirect(r.LeaderConfig.Id)
		return nil, e
	} else {
		return *response, nil
	}
}

type ErrRedirect int

//==========Assign -4 ========
var hostname string = "localhost"

//==========================Addition for assgn3============
//temp for testing
const layout = "3:04:5 pm (MST)"

//global vars for simulating crashing of servers
var crash bool
var server_to_crash int

//var globMutex = &sync.RWMutex{}

//var LSNMutex = &sync.RWMutex{}

//For converting default time unit of ns to millisecs
var msecs time.Duration = time.Millisecond * 100 ///increased to x10, with conn only MilliSecond might be too small, hence failing--changed to x100

//var msecs time.Duration = time.Second //for testing

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
	Cmd  []byte
	Acks int
}
type LogMetaData struct {
	LastLogIndex int //last entry in log,corresponding Term is Term in LogVal, can also be calculated as len(LogDB)-1
	PrevLogIndex int // value is LastLogIndex-1 always so not needed as such,---Change this later
	PrevLogTerm  int
	CommitIndex  int
	NextIndexMap map[int]int
}
type AppendEntriesReq struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []byte //leader appends one by one so why need array of LogEntry as given in paper
	//LogEntry interface was given by sir, so that the type returned in Append is LogEntry type i.e. its implementator's type
	LeaderCommitIndex  int
	LeaderLastLogIndex int
}
type AppendEntriesResponse struct {
	Term         int
	Success      bool
	FollowerId   int
	IsHeartBeat  bool
	LastLogIndex int //added for parallel appends!-4 April-03:25am
}
type ClientAppendReq struct {
	Data []byte
}
type ClientAppendResponse struct {
	LogEntry LogEntry
	RetError error
}
type RequestVote struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}
type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
	Id          int //added so that leader can keep track of Votes from a server rather than VoteCount
}

func (r *Raft) makeConnection(port int, msg interface{}) {
	//service := hostname + ":" + strconv.Itoa(port) --Set hostname as global or pass
	//CHECK IF CONNECTION IS ALREADY MADE!
	//fmt.Println("In make connection", r.myId(), "for port", port)
	service := hostname + ":" + strconv.Itoa(port)
	tcpAddr, err := net.ResolveTCPAddr("tcp", service)
	checkErr("Error in makeConnection,ResolveTCP", err)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	//fmt.Println(r.myId(), "Dialied,conn is:", conn)
	if err != nil {
		//fmt.Println("Err in net.Resolve in makeConn is", err)
		//checkErr(err)
		return
	} else {
		//Encode(conn, msg)
		//EncodeString(conn)--works
		EncodeInterface(conn, msg)
		//Sender(conn, msg)
	}
}

//Encodes the msg into gob
func EncodeInterface(conn net.Conn, msg interface{}) {
	//	fmt.Println("in sender, conn is:", conn)
	enc_net := gob.NewEncoder(conn)
	//	fmt.Println("Object encoded is:", obj_enc)
	err_enc := enc_net.Encode(&msg)
	if err_enc != nil {
		//		fmt.Println("Err in encoding", err_enc)
	}
}

//changed to r.send to access port of serverId
func (r *Raft) send(serverId int, msg interface{}) {
	//==A-4=====
	//Dial the connection to serverId's port-get the port using self raft's clustObj
	//place the msg on the connection made

	//fmt.Println("In send", r.myId())

	if !(r.Myconfig.Id == getServerToCrash() && getCrash()) {
		port := r.ClusterConfigObj.Servers[serverId].LogPort //Confirm if Si config is in index i of clusterObj only!--PENDING
		//fmt.Println("Connection being made to port:", port, r.myId())
		//conn := r.makeConnection(port)
		if r.Myconfig.LogPort == port { //when msg is for self, no need to make connection! without this also it works, since server connects to self port
			r.EventCh <- msg
		} else {
			r.makeConnection(port, msg)
		}
	} else {
		//fmt.Println("I am crashed,did not send!", r.myId())
	}
}

func (r *Raft) receive() interface{} {

	//waits on EventCh
	request := <-r.EventCh
	//	fmt.Printf("In receiver(),read from EventCh,type: %T  %v\n", request, r.myId())

	switch request.(type) { //if Data on channel is RetryTimeout, then it should revert to follower, hence return the timeout object
	case int:
		request = request.(int)
	}
	if !(getServerToCrash() == r.Myconfig.Id && getCrash()) || request == RetryTimeOut {
		//		fmt.Printf("returning channel value %v ,value is %T,%v \n", r.myId(), request, request)
		return request
	} else {
		//fmt.Println("In receive(), Crashed!, returning nil", r.myId())
		return nil
	}

	//return 0 //dummy
	return request
}

func (r *Raft) sendToAll_AppendReq(msg []interface{}) {
	if !(r.Myconfig.Id == getServerToCrash() && getCrash()) {
		//fmt.Println("I am", r.Myconfig.Id, "able to send")
		for k := range r.ClusterConfigObj.Servers {
			//fmt.Println("Id from map is:", k, r.Myconfig.Id)
			if r.Myconfig.Id != k { //send to all except self
				//fmt.Println("Sending HB to ", k)
				go r.send(k, msg[k])
				//r.send(k, msg[k]) //for removing multiple open ports-FOR NOW--DOESNOT WORK- but test cases pass everytime, with go it gets stuck many a times--CHECK
				//fmt.Println("After sending RPCS")

			}
		}
	}
}

func (r *Raft) sendToAll(msg interface{}) {
	//==Assgn4
	//make connections with all and dump msg after encoding with gob to all conns

	for k, _ := range r.ClusterConfigObj.Servers {
		//fmt.Println("Id from map is:", k, r.myId())
		if r.Myconfig.Id != k { //send to all except self
			go r.send(k, msg)
			//r.send(k, msg) //for removing multiple open ports-FOR NOW
			//fmt.Println("After sending RV")
		}
	}

}

//Keeps looping and performing follower functions till it timesout and changes to candidate
func (r *Raft) follower(timeout int) int {
	//myId := r.Myconfig.Id
	//fmt.Println("In follower()", myId)
	//start heartbeat timer,timeout func wil place HeartbeatTimeout on channel
	waitTime := timeout
	HeartBeatTimer := r.StartTimer(HeartbeatTimeout, waitTime)

	for {
		//		fmt.Println("In follower,looping", r.myId())
		req := r.receive()
		//		fmt.Println("in follower(), receive returned", r.myId())
		switch req.(type) {
		case AppendEntriesReq:
			request := req.(AppendEntriesReq) //explicit typecasting
			//			fmt.Println("AE_Req came", r.myId(), request)
			r.serviceAppendEntriesReq(request, HeartBeatTimer, waitTime)
		case RequestVote:
			//			fmt.Println("RequestVote came", r.myId())
			waitTime_msecs := msecs * time.Duration(waitTime)
			request := req.(RequestVote)
			HeartBeatTimer.Reset(waitTime_msecs)
			//fmt.Println("Timer reset to:", waitTime_msecs)
			r.serviceRequestVote(request)
		case ClientAppendReq: //follower can't handle clients and redirects to leader, sends upto CommitCh as well as clientCh
			//			fmt.Println("GOT CA Req in Follower!", r.myId())
			request := req.(ClientAppendReq) //explicit typecasting
			response := ClientAppendResponse{}
			logItem := LogItem{r.CurrentLogEntryCnt, false, request.Data} //lsn is count started from 0
			r.CurrentLogEntryCnt += 1
			response.LogEntry = logItem
			r.CommitCh <- &response.LogEntry
			//			fmt.Println("Put in commit channel", r.myId())
			//var e error = ErrRedirect(r.LeaderConfig.Id)
			//response.RetError = e
			//r.clientCh <- response //respond to client giving the leader Id--Should only be given leader Id right?--CHECK
		case int:
			//			fmt.Println("In follower timeout", r.myId())
			HeartBeatTimer.Stop()
			return candidate
		}
	}
}

//conducts election, returns only when state is changed else keeps looping on outer loop(i.e. restarting elections)
func (r *Raft) candidate() int {
	//myId := r.Myconfig.Id
	//	fmt.Println("Election started!", r.myId())
	waitTime := 10
	//fmt.Println("ELection timeout is", waitTime)
	ElectionTimer := r.StartTimer(ElectionTimeout, waitTime)
	//This loop is for election process which keeps on going until a leader is elected
	for {
		//reset the Votes else it will reflect the Votes received in last Term
		r.resetVotes()
		//fmt.Println("Votes reset!")
		//fmt.Println("Vote map after resetting", r.f_specific[0], r.f_specific[1], r.f_specific[2], r.f_specific[3], r.f_specific[4])
		r.CurrentTerm = r.CurrentTerm + 1 //increment current Term
		//		fmt.Println("I am candidate", r.Myconfig.Id, "and current Term is now:", r.CurrentTerm)
		r.VotedFor = r.Myconfig.Id //Vote for self
		r.WriteCVToDisk()          //write Current Term and VotedFor to disk
		r.f_specific[r.Myconfig.Id].Vote = true
		//fmt.Println("Vote map after voting for self", r.f_specific[0], r.f_specific[1], r.f_specific[2], r.f_specific[3], r.f_specific[4])
		reqVoteObj := r.prepRequestVote() //prepare request Vote obj
		//		fmt.Println("after calling prepRV", r.myId(), "prepObj is:", reqVoteObj)
		r.sendToAll(reqVoteObj) //send requests for Vote to all servers
		//		fmt.Println("Sent to all", r.myId())
		//this loop for reading responses from all servers
		for {
			req := r.receive()
			//			fmt.Println("receive func returned value:", req, r.myId())
			switch req.(type) {
			case RequestVoteResponse: //got the Vote response
				response := req.(RequestVoteResponse) //explicit typecasting so that fields of struct can be used
				//fmt.Println("Got the Vote", response.VoteGranted, "from", response.Id)
				if response.VoteGranted {
					r.f_specific[response.Id].Vote = true
					//r.VoteCount = r.VoteCount + 1
				}
				VoteCount := r.countVotes()
				//				fmt.Println("I am:", r.Myconfig.Id, "Votecount is", VoteCount)
				if VoteCount >= majority {
					//					fmt.Println("Votecount is majority, I am new leader", r.Myconfig.Id)
					ElectionTimer.Stop()
					r.LeaderConfig.Id = r.Myconfig.Id //update leader details
					return leader                     //become the leader
				}

			case AppendEntriesReq: //received an AE request instead of Votes, i.e. some other leader has been elected
				request := req.(AppendEntriesReq)
				//Can be clubbed with serviceAppendEntriesReq with few additions!--SEE LATER

				//fmt.Println("I am ", r.Myconfig.Id, "candidate,got AE_Req from", request.LeaderId, "Terms my,leader are", r.CurrentTerm, request.Term)
				waitTime_msecs := msecs * time.Duration(waitTime)
				appEntriesResponse := AppendEntriesResponse{}
				appEntriesResponse.FollowerId = r.Myconfig.Id
				appEntriesResponse.Success = false //false by default, in case of heartbeat or invalid leader
				if request.Term >= r.CurrentTerm { //valid leader
					r.LeaderConfig.Id = request.LeaderId //update leader info
					ElectionTimer.Reset(waitTime_msecs)  //reset the timer
					var myLastIndexTerm int
					if len(r.MyLog) == 0 {
						myLastIndexTerm = -1

					} else {
						myLastIndexTerm = r.MyLog[r.MyMetaData.LastLogIndex].Term
					}
					if request.LeaderLastLogIndex == r.MyMetaData.LastLogIndex && request.Term == myLastIndexTerm { //this is heartbeat from a valid leader
						appEntriesResponse.Success = true
					}
					r.send(request.LeaderId, appEntriesResponse)
					return follower
				} else {
					//fmt.Println("In candidate, AE_Req-else")
					r.send(request.LeaderId, appEntriesResponse)
				}
			case int:
				waitTime_msecs := msecs * time.Duration(waitTime)
				ElectionTimer.Reset(waitTime_msecs)
				break
			}
		}
	}
}

//Keeps sending heartbeats until state changes to follower
func (r *Raft) leader() int {
	fmt.Println("================In leader()======================", r.myId())

	r.sendAppendEntriesRPC() //send Heartbeats
	waitTime := 1            //duration between two heartbeats
	waitTime_msecs := msecs * time.Duration(waitTime)
	//fmt.Println("Heartbeat time out is", waitTime)

	waitTimeAE := 5                                            //max time to wait for AE_Response
	HeartbeatTimer := r.StartTimer(HeartbeatTimeout, waitTime) //starts the timer and places timeout object on the channel
	//var AppendEntriesTimer *time.Timer
	waitStepDown := 7
	RetryTimer := r.StartTimer(RetryTimeOut, waitStepDown)
	//fmt.Println("I am", r.Myconfig.Id, "timer created", AppendEntriesTimer)
	responseCount := 0
	r.setNextIndex_All() //so that new leader sets it map
	for {

		req := r.receive() //wait for client append req,extract the msg received on self EventCh
		switch req.(type) {
		case ClientAppendReq:
			//reset the heartbeat timer, now this sendRPC will maintain the authority of the leader
			HeartbeatTimer.Reset(waitTime_msecs)
			request := req.(ClientAppendReq)
			Data := request.Data
			//			fmt.Println("Received CA request,cmd is: ", string(Data), r.myId())
			//No check for semantics of cmd before appending to log?
			r.AppendToLog_Leader(Data) //append to self log as byte array
			//			fmt.Println("I am", r.Myconfig.Id, "done appending to log", string(Data), "len is:", len(r.MyLog))
			r.sendAppendEntriesRPC()
			responseCount = 0 //for RetryTimer
			//AppendEntriesTimer = r.StartTimer(AppendEntriesTimeOut, waitTimeAE) //Can be written in HeartBeatTimer too

		case AppendEntriesResponse:
			response := req.(AppendEntriesResponse)
			//fmt.Println("got AE_Response! from : ", response.FollowerId, response)
			responseCount += 1
			if responseCount >= majority {
				waitTime_retry := msecs * time.Duration(waitStepDown)
				RetryTimer.Reset(waitTime_retry)
			}
			//when IsHeartBeat is true then Success is also true according to the code in serviceAEReq so case wont be there when isHB is true and Success is false
			// isHB true means it is a succeeded heartbeat hence no work to do if it is AE req then only proceed else do nothing and continue
			//So when follower's log is stale or he is more latest, it would set isHB false
			if !response.IsHeartBeat {
				retVal := r.serviceAppendEntriesResp(response, HeartbeatTimer, waitTimeAE, waitTime)
				if retVal == follower {
					return follower
				}

			}

		case AppendEntriesReq: // in case some other leader is also in function, it must fall back or remain leader
			request := req.(AppendEntriesReq)
			//			fmt.Println("In leader, received AE_Req")
			if request.Term > r.CurrentTerm {
				//fmt.Println("In leader,AE_Req case, I am ", r.Myconfig.Id, "becoming follower now, because request.Term, r.CurrentTerm", request.Term, r.CurrentTerm)
				r.CurrentTerm = request.Term //update self Term and step down
				r.VotedFor = -1              //since Term has increased so VotedFor must be reset to reflect for this Term
				r.WriteCVToDisk()
				return follower //sender server is the latest leader, become follower
			} else {
				//reject the request sending false
				reply := AppendEntriesResponse{r.CurrentTerm, false, r.Myconfig.Id, false, r.MyMetaData.LastLogIndex}
				r.send(request.LeaderId, reply)
			}

		case int: //Time out-time to send Heartbeats!

			timeout := req.(int)
			if timeout == RetryTimeOut {
				RetryTimer.Stop()
				//				fmt.Println(r.myId(), "Becoming follower, in case int")
				return follower
			}
			//fmt.Println("Timeout of", r.Myconfig.Id, "is of type:", timeout)
			//waitTime_msecs := msecs * time.Duration(waitTime)
			if timeout == HeartbeatTimeout {
				//				fmt.Println("Time to send HBs!", r.myId())
				//fmt.Println("Leader:Reseting HB timer")
				HeartbeatTimer.Reset(waitTime_msecs)
				responseCount = 0 //since new heartbeat is now being sent
				//it depends on nextIndex which is correctly read in prepAE_Req method,
				//since it was AE other than HB(last entry), it would have already modified the nextIndex map
				r.sendAppendEntriesRPC() //This either sends Heartbeats or retries the failed AE due to which the timeout happened,
				//HeartbeatTimer.Reset(msecs * time.Duration(8)) //for checking leader change, setting timer of f4 to 8s--DOESN'T work..-_CHECK
			}
		}
	}
}

func (r *Raft) serviceAppendEntriesResp(response AppendEntriesResponse, HeartbeatTimer *time.Timer, waitTimeAE int, waitTime int) int {
	//fmt.Println("In serviceAE_Response", r.myId(), "received from", response.FollowerId)
	//fmt.Println("Next index of", f_Id, "is", nextIndex, len(r.MyLog))
	f_Id := response.FollowerId
	//nextIndex := r.MyMetaData.NextIndexMap[f_Id]--WRONG
	lastIndex := response.LastLogIndex
	//	fmt.Println("last index of follower", f_Id, "is", lastIndex, "length of my log is", len(r.MyLog), ":", r.myId())
	ack := &r.MyLog[lastIndex].Acks
	if response.Success { //log of followers are consistent and no new leader has come up
		(*ack) += 1                                           //increase the ack count since follower responded true
		follower_nextIndex := r.MyMetaData.NextIndexMap[f_Id] //read again since, another Append at leader might have modified it
		if follower_nextIndex < r.MyMetaData.LastLogIndex {
			//this means this ack came for log repair request, so nextIndex must be advanced till it becomes equal to leader's last index
			r.MyMetaData.NextIndexMap[f_Id] += 1
			//			fmt.Println("In serviceAEREsp============================Modified nextIndex map", r.MyMetaData.NextIndexMap, "conditions:", follower_nextIndex, r.MyMetaData.LastLogIndex)
		}

	} else { //retry if follower rejected the rpc
		//false is sent it means follower is either more latest or its log is stale!
		//fmt.Println("Follower ", response.FollowerId, "response.Term,r.CurrentTerm:", response.Term, r.CurrentTerm)
		if response.Term > r.CurrentTerm { //this means another server is more up to date than itself
			r.CurrentTerm = response.Term //update self Term with latest leader's Term
			r.VotedFor = -1               //since Term has increased so VotedFor must be reset to reflect for this Term
			r.WriteCVToDisk()
			//fmt.Println("I am ", r.Myconfig.Id, "becoming follower now\n")
			return follower
		} else { //Log is stale and needs to be repaired!
			//fmt.Println("About to send AE_rpc!!")
			HeartbeatTimer.Reset(time.Duration(waitTime) * msecs) //reset HB timer
			r.LogRepair(response)
		}
	}
	//if responseCount >= majority { //convert this nested ifs to && if condition
	//fmt.Println("Responses received from majority")
	if *ack == majority { //are all positive acks? if not wait for responses--WHY NOT >= ?? CHECK- Because it will advance commitIndex for acks= 3,4,5
		//which is unecessary
		//		fmt.Println("Acks received from majority for NI:", lastIndex, *ack)
		r.advanceCommitIndex(response.Term)
	}
	return -1
}
func (r *Raft) advanceCommitIndex(responseTerm int) {
	//advance CommitIndex only when entry from current Term has been replicated
	if responseTerm == r.CurrentTerm { //safety property for commiting Entries from older Terms
		prevCommitIndex := r.MyMetaData.CommitIndex
		newCommitIndex := r.MyMetaData.LastLogIndex
		//fmt.Println(r.myId(), "prev and new CI are:", prevCommitIndex, newCommitIndex)

		//When commit index advances by more than 1 entry, it should commit(also give for execution to KVStore) all the prev Entries too
		//i.e. check if they are already committed(replicated on majority by checking acks
		for i := prevCommitIndex + 1; i <= newCommitIndex; i++ { //it will always be same in the implementation, i.e. loop will run once only
			//fmt.Println(r.myId(), "loop var", i, newCommitIndex)
			//fmt.Println("acks for", i, r.MyLog[i].acks)
			if r.MyLog[i].Acks >= majority {
				//advance CI
				r.MyMetaData.CommitIndex += 1

				//fmt.Println(r.myId(), "CI is:", r.MyMetaData.CommitIndex)
				reply := ClientAppendResponse{}
				Data := r.MyLog[i].Cmd                               //last entry of leader's log
				logItem := LogItem{r.CurrentLogEntryCnt, true, Data} //lsn is count started from 0
				r.CurrentLogEntryCnt += 1
				reply.LogEntry = logItem
				// This response ideally comes from kvStore, so here we must send it to CommitCh with committed as True
				r.CommitCh <- &reply.LogEntry
			}
		}

	}
}

//Sends appendRPC requests to all servers and operates according to the responses received i.e. whether to advance the CommitIndex or not
//sent by leader, r is leader
func (r *Raft) sendAppendEntriesRPC() {
	//	fmt.Println("In sendAERPC, calling prepAERPC")
	appEntriesObj := r.prepAppendEntriesReq() //prepare AppendEntries object
	//	fmt.Println("prep AE_Obj is :", appEntriesObj)
	appEntriesObjSlice := make([]interface{}, len(appEntriesObj))

	//Copy to new slice created--This is the method to send a []interface to []TypeX
	for i, d := range appEntriesObj {
		appEntriesObjSlice[i] = d
	}
	//fmt.Println("In sendAE_RPC, obj is", appEntriesObjSlice)
	r.sendToAll_AppendReq(appEntriesObjSlice) //send AppendEntries to all the followers
}

//Appends to self log
//adds new entry, modifies last and prev indexes, Term
//how to modify the Term fields? n who does--candidate state advances Terms
func (r *Raft) AppendToLog_Leader(cmd []byte) {
	Term := r.CurrentTerm
	logVal := LogVal{Term, cmd, 0} //make object for log's value field with acks set to 0
	//fmt.Println("Before putting in log,", logVal)
	r.MyLog = append(r.MyLog, logVal)
	//fmt.Println("I am:", r.Myconfig.Id, "Added cmd to my log")

	//modify metaData after appending
	//fmt.Println("MetaData before appending,LastLogIndex,PrevLogIndex,PrevLogTerm", r.MyMetaData.LastLogIndex, r.MyMetaData.PrevLogIndex, r.MyMetaData.PrevLogTerm)
	LastLogIndex := r.MyMetaData.LastLogIndex + 1
	r.MyMetaData.PrevLogIndex = r.MyMetaData.LastLogIndex
	r.MyMetaData.LastLogIndex = LastLogIndex
	//	fmt.Println(r.myId(), "Length of my log is", len(r.MyLog))
	if len(r.MyLog) == 1 {
		r.MyMetaData.PrevLogTerm = r.MyMetaData.PrevLogTerm + 1 //as for empty log PrevLogTerm is -2

	} else if len(r.MyLog) > 1 { //explicit check, else would have sufficed too, just to eliminate len=0 possibility
		r.MyMetaData.PrevLogTerm = r.MyLog[r.MyMetaData.PrevLogIndex].Term
	}
	//r.CurrentTerm = Term
	//fmt.Println("I am leader, Appended to log, last index, its Term is", r.MyMetaData.LastLogIndex, r.MyLog[LastLogIndex].Term)
	//fmt.Println("MetaData after appending,LastLogIndex,PrevLogIndex,PrevLogTerm", r.MyMetaData.LastLogIndex, r.MyMetaData.PrevLogIndex, r.MyMetaData.PrevLogTerm)
	r.setNextIndex_All() //Added-28 march for LogRepair
	//Write to disk
	//fmt.Println(r.myId(), "In append_leader, appended to log", string(cmd))
	r.WriteLogToDisk()

}

//This is called by a follower since it should be able to overwrite for log repairs
func (r *Raft) AppendToLog_Follower(request AppendEntriesReq) {
	//	fmt.Println(r.myId(), "In append of follower", string(request.Entries))
	Term := request.Term
	cmd := request.Entries
	index := request.PrevLogIndex + 1
	logVal := LogVal{Term, cmd, 0} //make object for log's value field

	if len(r.MyLog) == index {
		r.MyLog = append(r.MyLog, logVal) //when trying to add a new entry
	} else {
		r.MyLog[index] = logVal //overwriting in case of log repair
		//fmt.Println("Overwiriting!!")
	}

	//Changed on 4th april, above is wrong in case of overwriting of log
	r.MyMetaData.LastLogIndex = index
	r.MyMetaData.PrevLogIndex = index - 1
	if index == 0 {
		r.MyMetaData.PrevLogTerm = r.MyMetaData.PrevLogTerm + 1 //or simple -1
	} else if index >= 1 {
		r.MyMetaData.PrevLogTerm = r.MyLog[index-1].Term
	}

	//Update commit index
	leaderCI := float64(request.LeaderCommitIndex)
	myLI := float64(r.MyMetaData.LastLogIndex)
	if request.LeaderCommitIndex > r.MyMetaData.CommitIndex {
		r.MyMetaData.CommitIndex = int(math.Min(leaderCI, myLI))
	}
	//fmt.Println(r.myId(), "My CI is:", r.MyMetaData.CommitIndex)
	r.WriteLogToDisk()
}

//Modifies the next index in the map and returns
func (r *Raft) LogRepair(response AppendEntriesResponse) {
	Id := response.FollowerId
	//	fmt.Println("In log repair for ", Id)
	failedIndex := r.MyMetaData.NextIndexMap[Id]
	var nextIndex int
	//fmt.Println("Failed index is:", failedIndex)
	if failedIndex != 0 {
		nextIndex = failedIndex - 1 //decrementing follower's nextIndex
		//nextIndex = response.LastLogIndex + 1 //changed on 12 march--failing for some cases --CHECK, doesn't work with for loop in handleClient

	} else { //if nextIndex is 0 means, follower doesn't have first entry (of leader's log),so decrementing should not be done, so retry the same entry again!
		nextIndex = failedIndex
	}
	//Added--3:38-23 march
	r.MyMetaData.NextIndexMap[Id] = nextIndex
	//fmt.Println("In log repair==============Modified NI MAP==============", r.MyMetaData.NextIndexMap)
	//	fmt.Println("I am", response.FollowerId, "My Old and new NI are", failedIndex, nextIndex)
	return
}

//Follower receives AEReq and appends to log checking the request objects fields
//also updates leader info after reseting timer or appending to log
func (r *Raft) serviceAppendEntriesReq(request AppendEntriesReq, HeartBeatTimer *time.Timer, waitTime int) {
	//replicates entry wise , one by one
	waitTime_msecs := msecs * time.Duration(waitTime)

	//fmt.Println("In serviceAEREq ", r.myId(), "entries:", request.Entries)
	appEntriesResponse := AppendEntriesResponse{} //make object for responding to leader
	appEntriesResponse.FollowerId = r.Myconfig.Id
	appEntriesResponse.Success = false     //false by default
	appEntriesResponse.IsHeartBeat = false //by default
	var myLastIndexTerm, myLastIndex int
	myLastIndex = r.MyMetaData.LastLogIndex
	//fmt.Println("I am", r.Myconfig.Id, "checking if valid leader:", request.LeaderId)
	if request.Term >= r.CurrentTerm { //valid leader
		//fmt.Println("I am", r.Myconfig.Id, "this is valid leader:", request.LeaderId)
		//r.LeaderConfig.Id = request.LeaderId //update leader info
		leaderId := request.LeaderId
		r.UpdateLeaderInfo(leaderId)         //update leader info
		r.CurrentTerm = request.Term         //update self Term
		HeartBeatTimer.Reset(waitTime_msecs) //reset the timer if this is HB or AE req from valid leader
		if len(r.MyLog) == 0 {               //if log is empty
			myLastIndexTerm = -1
		} else {
			myLastIndexTerm = r.MyLog[myLastIndex].Term
		}
		//This is a HB,here log is empty on both sides so Term must not be checked (as leader has incremented its Term due to elections)
		if request.Entries == nil && myLastIndex == request.LeaderLastLogIndex {
			//case when first condition is true and 2nd fails wont come,since AE comes from a leader with
			//empty log(hence Entries nil) whereas follower has values(2nd condition mismatch)
			appEntriesResponse.Success = true
			appEntriesResponse.IsHeartBeat = true
			//fmt.Println("I am:", r.Myconfig.Id, "Log empty, HB received!(In serviceAppendReq)")
		} else { //log has Data so-- for hearbeat, check the index and Term of last entry
			if request.LeaderLastLogIndex == myLastIndex && request.Term == myLastIndexTerm {
				//this is heartbeat as last entry is already present in self log
				appEntriesResponse.Success = true
				appEntriesResponse.IsHeartBeat = true
				r.MyMetaData.CommitIndex = request.LeaderCommitIndex //update the CI for last entry that leader got majority acks for!
				//fmt.Println("I am", r.Myconfig.Id, "this is valid leader:", request.LeaderId, "got HB", r.MyMetaData.CommitIndex)
			} else { //this is not a heartbeat but append request
				//fmt.Println("I am:", r.Myconfig.Id, "This is append request \n r.CurrentTerm,mylastTerm,req.PrevLogTerm,mylastIndex,req.PrevLogIndex", r.CurrentTerm, myLastIndexTerm, request.PrevLogTerm, myLastIndex, request.PrevLogIndex)
				//fmt.Println("I am:", r.Myconfig.Id, "This is append request", string(request.Entries))
				//Term and index of self last entry and request's previous Entries must be checked
				//but what if r.current Term has increased to more than the Term of last log entry due to repeated elections but no CA req during that time
				//so extract the last Term from self log--previously it was being compared to r.CurrentTerm--FAILING NOW--FIXED
				if request.PrevLogTerm == myLastIndexTerm && request.PrevLogIndex == myLastIndex { //log is consistent till now
					//					fmt.Println(r.myId(), "Log is consistent till now! Going to append new entry:", request)
					r.AppendToLog_Follower(request) //append to log
					//					fmt.Println(r.myId(), "Appended to log,sending true for", request)
					r.CurrentTerm = request.Term
					appEntriesResponse.Success = true
					appEntriesResponse.IsHeartBeat = false
				}
			}
		}
	}

	appEntriesResponse.Term = r.CurrentTerm
	appEntriesResponse.LastLogIndex = r.MyMetaData.LastLogIndex
	//	fmt.Println("Response sent by", r.Myconfig.Id, "is :", appEntriesResponse, "to", request.LeaderId)

	//fmt.Printf("Follower %v sent the AE_ack to %v \n", r.Myconfig.Id, request.LeaderId)
	//Where is it sending to leader's channel??--Added
	r.send(request.LeaderId, appEntriesResponse)
}

//r is follower who received the request
//Services the received request for Vote
func (r *Raft) serviceRequestVote(request RequestVote) {
	//fmt.Println("In service RV method of ", r.Myconfig.Id)
	response := RequestVoteResponse{} //prep response object,for responding back to requester
	candidateId := request.CandidateId
	response.Id = r.Myconfig.Id
	//fmt.Println("Follower", r.Myconfig.Id, "log as complete?", r.logAsGoodAsMine(request))
	if r.isDeservingCandidate(request) {
		response.VoteGranted = true
		r.VotedFor = candidateId
		r.CurrentTerm = request.Term

		//Writing current Term and VoteFor to disk
		r.WriteCVToDisk()

	} else {
		response.VoteGranted = false
	}
	response.Term = r.CurrentTerm //to return self's Term too

	//fmt.Println("Follower", r.Myconfig.Id, "voting", response.VoteGranted) //"because Votefor is", r.VotedFor, "my and request Terms are:", r.CurrentTerm, request.Term)
	//fmt.Println("Follower", r.Myconfig.Id, "Current Term,request.Term is", r.CurrentTerm, request.Term, "Self LastLogIndex is", r.MyMetaData.LastLogIndex, "VotedFor,request.lastLogTerm", r.VotedFor, request.lastLogTerm)
	//fmt.Println("VotedFor,request.lastLogTerm", r.VotedFor, request.lastLogTerm)

	//fmt.Printf("In serviceRV of %v, obj prep is %v \n", r.Myconfig.Id, response)
	r.send(candidateId, response) //send to sender using send(sender,response)
}

func (r *Raft) WriteCVToDisk() {

	fh_CV, err1 := os.OpenFile(r.Path_CV, os.O_RDWR|os.O_APPEND, 0600)
	if err1 != nil {
		checkErr("Error in WriteCVToDisk", err1)
		panic(err1)
	}
	cv_encoder := json.NewEncoder(fh_CV)
	cv_encoder.Encode(r.CurrentTerm)
	cv_encoder.Encode(r.VotedFor)
	fh_CV.Close()
}

func (r *Raft) WriteLogToDisk() {

	fh_Log, err1 := os.OpenFile(r.Path_Log, os.O_RDWR|os.O_APPEND, 0600)
	if err1 != nil {
		panic(err1)
	}

	log_encoder := json.NewEncoder(fh_Log)
	log_m, _ := json.Marshal(r.MyLog)
	log_encoder.Encode(string(log_m))
	fh_Log.Close()

}
func (r *Raft) isDeservingCandidate(request RequestVote) bool {
	//if candidate Term is greater don't check VotedFor as it will be the last Term value and since Term is greater it means it hasn't Voted in this Term
	//If Term is equal check VotedFor
	//in both cases ,log completion is checked mandatorily
	return ((request.Term > r.CurrentTerm && r.logAsGoodAsMine(request)) || (request.Term == r.CurrentTerm && r.logAsGoodAsMine(request) && (r.VotedFor == -1 || r.VotedFor == request.CandidateId)))
}

func (r *Raft) logAsGoodAsMine(request RequestVote) bool {
	//fmt.Println("Follower", r.Myconfig.Id, "Granting Vote as", (request.lastLogTerm > r.CurrentTerm || (request.lastLogTerm == r.CurrentTerm && request.LastLogIndex >= r.MyMetaData.LastLogIndex)))
	return (request.LastLogTerm > r.CurrentTerm || (request.LastLogTerm == r.CurrentTerm && request.LastLogIndex >= r.MyMetaData.LastLogIndex))

}

func (r *Raft) resetVotes() {
	for i := 0; i < noOfServers; i++ {
		r.f_specific[i].Vote = false
	}
}

func (r *Raft) UpdateLeaderInfo(leaderId int) {
	r.LeaderConfig.ClientPort = r.ClusterConfigObj.Servers[leaderId].ClientPort
	r.LeaderConfig.Hostname = r.ClusterConfigObj.Servers[leaderId].Hostname
	r.LeaderConfig.Id = leaderId
	r.LeaderConfig.LogPort = r.ClusterConfigObj.Servers[leaderId].LogPort

}

//preparing object for replicating log value at nextIndex, one for each follower depending on nextIndex read from NextIndexMap
func (r *Raft) prepAppendEntriesReq() (appendEntriesReqArray [noOfServers]AppendEntriesReq) {
	//	fmt.Println("In prepAERPC")
	//var flag bool = false //for testing
	for i := 0; i < noOfServers; i++ {
		if i != r.Myconfig.Id {
			LeaderId := r.LeaderConfig.Id
			var Entries []byte
			var Term, PrevLogIndex, PrevLogTerm int
			nextIndex := r.MyMetaData.NextIndexMap[i] //read the nextIndex to be sent from map
			//			fmt.Println("In prepAEReq, nextIndex is", nextIndex, "for server", i)
			//if len(r.MyLog) != 0 { //removed since, in case of decrementing nextIndexes for log repair, log length is never zero but nextIndex becomes -1
			if nextIndex >= 0 { //this is AE request with last entry sent (this will be considered as HB when log of follower is consistent)
				//				fmt.Println("Next index map is:", r.MyMetaData.NextIndexMap, "follower:", i, r.myId())
				Term = r.MyLog[nextIndex].Term
				Entries = r.MyLog[nextIndex].Cmd //entry to be replicated
				PrevLogIndex = nextIndex - 1     //should be changed to nextIndex-1
				if nextIndex == 0 {
					PrevLogTerm = -1 //since indexing will be log[-1] so it must be set explicitly
				} else {
					PrevLogTerm = r.MyLog[PrevLogIndex].Term //this is the way to get new PrevLogTerm to be sent
				}
				//fmt.Println("in prep, last entry is:", Entries)

			} else { //so this is prepReq for heartbeat for empty log as nextIndex is -1
				//when log is empty indexing to log shouldn't be done hence copy old values
				Term = r.CurrentTerm
				Entries = nil
				PrevLogIndex = r.MyMetaData.PrevLogIndex
				PrevLogTerm = r.MyMetaData.PrevLogTerm
				//				fmt.Println("In prep, entries is nil for ", i, r.myId(), "nextIndexMap is", r.MyMetaData.NextIndexMap)
			}

			LeaderCommitIndex := r.MyMetaData.CommitIndex
			LeaderLastLogIndex := r.MyMetaData.LastLogIndex
			appendEntriesObj := AppendEntriesReq{Term, LeaderId, PrevLogIndex, PrevLogTerm, Entries, LeaderCommitIndex, LeaderLastLogIndex}
			appendEntriesReqArray[i] = appendEntriesObj

		}

	}
	return appendEntriesReqArray

}

//prepares the object for sending to RequestVoteRPC, requesting the Vote
func (r *Raft) prepRequestVote() RequestVote {
	LastLogIndex := r.MyMetaData.LastLogIndex
	//if this is the request when log is empty
	var lastLogTerm int
	if len(r.MyLog) == 0 {
		//fmt.Println("In if of prepRV()")
		lastLogTerm = -1 //Just for now--Modify later
		//lastLogTerm = r.CurrentTerm //HOW??-A4 shouldnt it be -1?
	} else {
		//fmt.Println("In else of prepRV()")
		lastLogTerm = r.MyLog[LastLogIndex].Term
	}
	//fmt.Println("here2")
	reqVoteObj := RequestVote{r.CurrentTerm, r.Myconfig.Id, LastLogIndex, lastLogTerm}
	return reqVoteObj
}

func (r *Raft) setNextIndex_All() {
	nextIndex := r.MyMetaData.LastLogIndex //given as LastLogIndex+1 in paper..don't know why,seems wrong.
	//fmt.Println("============================================================In setNextIndex_All,nextIndex is", nextIndex, r.myId())
	for k := range r.ClusterConfigObj.Servers {
		if r.Myconfig.Id != k {
			//fmt.Println("Setting nextIndexMap for", k)
			r.MyMetaData.NextIndexMap[k] = nextIndex
		}
	}
	//	fmt.Println("==****************In setNextIndex_All********************,map prep is:", r.MyMetaData.NextIndexMap)
	return
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return string(e)
}

//checks the number of true Votes in follower_specific member
func (r *Raft) countVotes() (VoteCount int) {
	for i := 0; i < noOfServers; i++ {
		if r.f_specific[i].Vote {
			VoteCount += 1
		}
	}

	return
}

//Starts the timer with appropriate random number secs, also timeout object is needed for distinguishing between different timeouts
func (r *Raft) StartTimer(timeoutObj int, waitTime int) (timerObj *time.Timer) {
	//expInSec := msecs * time.Duration(waitTime) //gives in seconds
	//	fmt.Println("Timer started, time is", time.Now().Format(layout), r.myId())
	expInSec := time.Duration(waitTime) * msecs
	timerObj = time.AfterFunc(expInSec, func() {
		r.TimeOut(timeoutObj)
	})
	return
}

//Places timeOutObject on the EventCh of the caller
func (r *Raft) TimeOut(timeoutObj int) {
	//	fmt.Println("Timed out at", time.Now().Format(layout), r.myId())
	r.EventCh <- timeoutObj
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
