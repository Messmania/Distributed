package raft

import (
	//"fmt"
	"log"
	//"math"
	//"math/rand"
	//"net"
	//"strconv"
	//"fmt"
	//"strings"
	//"sync"
	//"time"
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
		go raftObj.ServerSM() //fire all the servers
		return raftObj
	}
}

func NewRaft(cluster *ClusterConfig, thisServerId int, commitCh chan *LogEntry) (raftObj *Raft, err error) {
	err = nil
	var myObj ServerConfig
	var leaderObj ServerConfig

	//Intialising the ServerConfig struct for itself assuming id is already set in ServerConfig struct
	servArr := (*cluster).Servers
	for i, server := range servArr {
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
	}

	//Initialise raftObj---UPDATE this according to changed raft struct--PENDING
	clientCh := make(chan ClientAppendResponse)
	eventCh := make(chan interface{})
	//logValue := LogVal{-1, nil}
	myLog := []LogVal{}                                            //TO BE CHECKED
	metaData := LogMetadata{-1, -2, -2, -1, make(map[int]int), -1} //MODIFY raftObj init
	raftObj = &Raft{*cluster, myObj, leaderObj, 0, commitCh, clientCh, eventCh, 0, -1, 0, myLog, metaData}

	server_raft_map[myObj.Id] = raftObj //mapping server id to its raft object
	go raftObj.ServerSM()               //Start the raft State Machine
	return raftObj, err
}

func checkErr(err error) {
	if err != nil {
		log.Println("Error encountered in server.go:", err)

	}
}

//=====================++New Code++=========================

func (r *Raft) ServerSM() {
	log.Println("In server sm")
	state := follower //how to define type for this?--const
	for {
		switch state {
		case follower:
			state = r.follower()
		case candidate:
			state = r.candidate()
		case leader:
			state = r.leader()
		default:
			return
		}
	}
}
