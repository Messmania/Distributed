package raft

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"
)

//Global map for serverid->raftObj mapping
var server_raft_map = make(map[int]*Raft)

const (
	follower  = iota
	leader    = iota
	candidate = iota
)

//===============================================+++NEW CODE+++============================================

func ServerStart(cluster *ClusterConfig, thisServerId int, timeout int) {
	commitCh := make(chan *LogEntry, 4)
	raftObj, err := NewRaft(cluster, thisServerId, commitCh)
	//fmt.Println("Raft obj:", raftObj)
	if err != nil {
		checkErr(err)
		return ///os.exit? since server failed to initialize??
	} else {
		raftObj.connHandler(timeout)
	}
}

func (r *Raft) connHandler(timeout int) {
	go r.listenToServers()
	go r.listenToClients()
	go r.ServerSM(timeout)
	fmt.Println("Launched listeners and SM")
}

func NewRaft(cluster *ClusterConfig, thisServerId int, commitCh chan *LogEntry) (raftObj *Raft, err error) {
	err = nil
	var myObj, leaderObj ServerConfig
	var nextIndexMap = make(map[int]int)
	var f_details = make(map[int]*followerDetails)
	f_obj := followerDetails{false}
	var pathString_Log, pathString_CV string
	//Intialising the ServerConfig struct for itself assuming id is already set in ServerConfig struct
	servArr := (*cluster).Servers
	for i, server := range servArr { //finding self details from cluster object and loading to myConfig of raft
		if i == 0 {
			//read leader details into raftObj
			leaderObj.Id = servArr[i].Id
			leaderObj.Hostname = servArr[i].Hostname
			leaderObj.ClientPort = servArr[i].ClientPort
			leaderObj.LogPort = servArr[i].LogPort
		}

		if server.Id == thisServerId {
			//read leader details into raftObj
			myObj.Id = thisServerId
			myObj.Hostname = servArr[i].Hostname
			myObj.ClientPort = servArr[i].ClientPort
			myObj.LogPort = servArr[i].LogPort
		}
		nextIndexMap[i] = -1 //initialising nextIndexes for all in each server
		f_details[i] = &f_obj
	}

	//Setting paths of disk files
	pathString_CV, pathString_Log = CreateDiskFiles(thisServerId)
	//fmt.Println("I am:", thisServerId, " Path strings are:", pathString_CV, pathString_Log)
	eventCh := make(chan interface{})

	myLog := make([]LogVal, 0, 10)

	metaData := LogMetadata{-1, -2, -2, -1, nextIndexMap}

	raftObj = &Raft{*cluster, myObj, leaderObj, 0, commitCh, eventCh, -1, -1, myLog, metaData, f_details, pathString_CV, pathString_Log}

	//server_raft_map[myObj.Id] = raftObj //NOT NEEDED NOW--REMOVE

	return raftObj, err
}

func checkErr(err error) {
	if err != nil {
		log.Println("Error encountered in KVStore.go:", err)

	}
}

//=====================++New Code++=========================

//timeout param added Only for testing
func (r *Raft) ServerSM(timeout int) {
	state := follower //how to define type for this?--const
	for {
		switch state {
		case follower:
			//fmt.Println("in case follower")
			state = r.follower(timeout)
		case candidate:
			//fmt.Println("in case candidate of ServSM()")
			state = r.candidate()
		case leader:
			//fmt.Println("in case leader")
			state = r.leader()
		default:
			return
		}
	}
}

func CreateDiskFiles(thisServerId int) (pathString_CV string, pathString_Log string) {
	//Setting paths of disk files
	pathString_Log = "./Disk_Files/S" + strconv.Itoa(thisServerId) + "/Log.log"
	pathString_CV = "./Disk_Files/S" + strconv.Itoa(thisServerId) + "/CV.log"

	//Creating files
	fh_cv, err_cv := os.Create(pathString_CV)

	if err_cv != nil {
		log.Println("Error creating cv file", err_cv)
		panic(err_cv)
	} else {
		fh_cv.Close()
	}
	fh_log, err_log := os.Create(pathString_Log)

	if err_log != nil {
		log.Println("Error creating log file", err_log)
		panic(err_log)
	} else {
		fh_log.Close()
	}
	//fmt.Print("Paths are:", pathString_CV, pathString_Log)
	return pathString_CV, pathString_Log

}

//==============+Assign4+===========

func (r *Raft) listenToServers() {
	//fmt.Println("In listen to servers", r.myId())
	port := r.Myconfig.LogPort
	service := r.Myconfig.Hostname + ":" + strconv.Itoa(port)
	tcpaddr, err := net.ResolveTCPAddr("tcp", service)
	if err != nil {
		checkErr(err)
		return
	} else {
		//fmt.Println("No error!")
		listener, err := net.ListenTCP("tcp", tcpaddr)
		if err != nil {
			checkErr(err)
			//fmt.Println("Error after Listen")
			return
		} else {
			for {
				//fmt.Println("looping")
				conn, err := listener.Accept()
				fmt.Println("Accepted!", r.myId())
				if err != nil {
					fmt.Println("Err in listener.Accept")
					checkErr(err)
					continue
				} else if conn != nil { //Added if to remove nil pointer reference error
					go r.writeToEvCh(conn)
				}
			}

		}
	}
}

func (r *Raft) listenToClients() {
	port := r.Myconfig.ClientPort
	service := r.Myconfig.Hostname + ":" + strconv.Itoa(port)
	tcpaddr, err := net.ResolveTCPAddr("tcp", service)
	if err != nil {
		checkErr(err)
		return
	} else {
		listener, err := net.ListenTCP("tcp", tcpaddr)
		if err != nil {
			checkErr(err)
			return
		} else {
			for {
				conn, err := listener.Accept()
				if err != nil {
					continue
				} else if conn != nil { //Added if to remove nil pointer reference error
					//r.writeToEvCh(conn)
					go r.writeToEvCh(conn)
				}
			}

		}
	}
}

func (r *Raft) writeToEvCh(conn net.Conn) {
	//fmt.Println("In write to channel", r.myId(), "conn is", conn)

	//registerTypes()

	//Decode(conn)

	//DecodeString(conn)
	//DecodeInterface(conn)
	Receiver(conn)
}

//Dummy
func Receiver(conn net.Conn) {
	fmt.Println("in receiver,conn is", conn)
	gob.Register(RequestVote{})
	dec_net := gob.NewDecoder(conn)
	var obj_dec interface{}
	err_dec := dec_net.Decode(&obj_dec)
	if err_dec != nil {
		fmt.Println("Err in decoding", err_dec)
		return
	}
	//fmt.Printf("After decoding from gob,type is %T \n", obj_dec)
	fmt.Printf("decoded value %T %v \n", obj_dec, obj_dec)
	conn.Close()
	return
}

func registerTypes() {
	gob.Register(AppendEntriesReq{})
	gob.Register(AppendEntriesResponse{})
	gob.Register(RequestVote{})
	gob.Register(RequestVoteResponse{})
	gob.Register(ClientAppendReq{})
	gob.Register(ClientAppendResponse{})
	//gob.Register(int())
}

func (r *Raft) Decode(conn net.Conn) {
	dec := gob.NewDecoder(conn)
	var reqStr interface{}
	//wait for sometime till data is written to conn--
	time.Sleep(time.Second * 1)
	fmt.Println("After sleep of 1 sec", r.myId())
	fmt.Println("About to decode the msg")
	err := dec.Decode(&reqStr)
	fmt.Println("Err of decoding is:", err)
	if err != nil {
		fmt.Println("Err in decoding", err)
		checkErr(err)
		return
	}
	fmt.Printf("After decoding from gob,type is %T %v\n", reqStr, r.myId())
	r.eventCh <- reqStr
	conn.Close()
}

//DUmmy func for testing
func DecodeString(conn net.Conn) {
	var reply [512]byte
	n, err := conn.Read(reply[0:])
	if err != nil {
		fmt.Println("Err in conn.Read", err)
		return
	}
	fmt.Println("Value read from conn.Read:", string(reply[0:n]))

}

//Dummy--DOESNT WORK FOR CUSTOM TYPE!
func DecodeInterface(conn net.Conn) {
	gob.Register(RequestVote{})
	//var reply interface{}
	var reply RequestVote
	dec := gob.NewDecoder(conn)
	fmt.Println("About to decode into", reply)
	err := dec.Decode(&reply)
	if err != nil {
		fmt.Println("Err in dec.Decode()", err)
		return
	}
	fmt.Println("Value decoded is", reply)
	conn.Close()
}
