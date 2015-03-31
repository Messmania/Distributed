package raft

import (
	"fmt"
	"log"
	//"math"
	//"math/rand"
	//"net"
	"strconv"
	//"fmt"
	//"strings"
	//"sync"
	//"time"
	"os"
)

//Global map for serverid->raftObj mapping
var server_raft_map = make(map[int]*Raft)

const (
	follower  = iota
	leader    = iota
	candidate = iota
)

//===============================================+++NEW CODE+++============================================

func ServerStart(cluster *ClusterConfig, thisServerId int) *Raft {
	commitCh := make(chan *LogEntry, 4)
	raftObj, err := NewRaft(cluster, thisServerId, commitCh)
	if err != nil {
		checkErr(err)
		//return
		return raftObj //TO BE CHANGED
	} else {
		//fmt.Println("In else of Serverstart()")
		//go raftObj.ServerSM(timeout) //fire all the servers
		return raftObj
	}
}

func NewRaft(cluster *ClusterConfig, thisServerId int, commitCh chan *LogEntry) (raftObj *Raft, err error) {
	err = nil
	var myObj, leaderObj ServerConfig
	var nextIndexMap = make(map[int]int)
	var f_details = make(map[int]followerDetails)
	f_obj := followerDetails{false}
	var fh_log, fh_cv *os.File
	var err1, err2 error

	//Intialising the ServerConfig struct for itself assuming id is already set in ServerConfig struct
	servArr := (*cluster).Servers
	for i, server := range servArr { //finding self details from cluster object and loading to myConfig of raft
		if i == 0 {
			//read leader details into raftObj
			leaderObj.Id = servArr[i].Id
		}

		if server.Id == thisServerId {
			//read leader details into raftObj
			myObj.Id = thisServerId
		}
		nextIndexMap[i] = -1 //initialising nextIndexes for all in each server
		f_details[i] = f_obj
		pathString_Log := "./Disk_Files/S" + strconv.Itoa(i) + "/Log.log"
		pathString_C_V := "./Disk_Files/S" + strconv.Itoa(i) + "/CV.log"
		fh_log, err1 = os.Create(pathString_Log)
		if err1 != nil {
			panic(err1)
		}
		fh_cv, err2 = os.Create(pathString_C_V)
		if err2 != nil {
			panic(err1)
		}
	}

	//Initialise raftObj---UPDATE this according to changed raft struct--PENDING
	//clientCh := make(chan ClientAppendResponse)
	eventCh := make(chan interface{})
	//logValue := LogVal{-1, nil}
	myLog := []LogVal{} //TO BE CHECKED

	metaData := LogMetadata{-1, -2, -2, -1, nextIndexMap} //MODIFY raftObj init
	//how to make directories???--for now --path must exist for file to be created in that path

	raftObj = &Raft{*cluster, myObj, leaderObj, 0, commitCh, eventCh, -1, -1, myLog, metaData, f_details, fh_log, fh_cv}

	server_raft_map[myObj.Id] = raftObj //mapping server id to its raft object
	return raftObj, err
}

func checkErr(err error) {
	if err != nil {
		log.Println("Error encountered in server.go:", err)

	}
}

//=====================++New Code++=========================

//timeout param added Only for testing
func (r *Raft) ServerSM(timeout int) {
	fmt.Println("In server sm", r.Myconfig.Id)
	state := follower //how to define type for this?--const
	for {
		//		if r.Myconfig.Id == 1 {
		//			fmt.Println("In ServerSM, I am 1 and state!", state)
		//		}
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
