package raft

import (
	//"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
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

func ServerStart(cluster *ClusterConfig, thisServerId int) {
	commitCh := make(chan *LogEntry, 4)
	raftObj, err := NewRaft(cluster, thisServerId, commitCh)
	if err != nil {
		checkErr(err)
		return
	} else {
		connHandler(raftObj)
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
	//serverCount := len(r.ClusterConfigObj.Servers)
	//majorityCount := serverCount/2 + 1

	//Initialise raftObj---UPDATE this according to changed raft struct--PENDING
	clientCh := chan ClientAppendResponse
	eventCh := chan interface{}
	myLog := []LogVal{nil,nil}
	metaData := LogMetadata{-1,-2,-1,false,-1} //MODIFY raftObj init
	raftObj = &Raft{*cluster, myObj, leaderObj, 0, commitCh, clientCh,eventCh,0,0,-1,-1,myLog,metaData}
	//mapping server id to its raft object
	server_raft_map[myObj.Id] = &raftObj
	//Start the raft State Machine
	go raftObj.ServerSM()

	return raftObj, err
}

func connHandler(r *Raft) {
	//go listenToServers(r)   no need of this as it is already done in RegisterTCP_RPCService
	// server must also keep listening on eventCh for comm with other servers
	// putting both in go routines.
	go r.ServerSM()
	go listenToClients(r) // can be called using r
}

func listenToClients(r *Raft) {
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
					go handleClient(conn, r)
				}
			}

		}
	}
}

func handleClient(conn net.Conn, r *Raft) {
	for {
		var cmd [512]byte
		n, err1 := conn.Read(cmd[0:])
		if err1 != nil {
			break
		} else {
			var l SharedLog
			l = r
			logEntry, err := l.Append(cmd[0:n]) //This is Client calling append --will now be called from testmethod
			//write the logEntry:conn to map
			connLog[&logEntry] = conn

			if err == nil {
				r.commitCh <- (&logEntry)
				go kvStoreProcessing(r.commitCh)

			} else {
				//REDUNTANT: Leader info is already known and present in raftObj, so err is useless for now
				ldrHost, ldrPort := r.LeaderConfig.Hostname, r.LeaderConfig.ClientPort
				errRedirectStr := "ERR_REDIRECT " + ldrHost + " " + strconv.Itoa(ldrPort)
				_, err1 := conn.Write([]byte(errRedirectStr))
				checkErr(err1)
			}
		}
	}
}

//=====================++New Code++=========================

func (r *Raft) ServerSM() {
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
