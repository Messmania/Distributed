package raft

//package main

import (
	"fmt"
	//"log"
	"os"
	"strconv"
	//"time"
	//"sync"
)

//Global map for serverid->raftObj mapping
//var server_raft_map = make(map[int]*Raft)

const (
	follower  = iota
	leader    = iota
	candidate = iota
)

//===============================================++A-3:NEW CODE+++============================================

func ServerStart(cluster *ClusterConfig, thisServerId int, ftimeout int, etimeout int) {
	commitCh := make(chan *LogEntry, 4)
	raftObj, err := NewRaft(cluster, thisServerId, commitCh)
	//fmt.Println("Raft obj:", raftObj)
	if err != nil {
		checkErr("Error in ServerStart(),NewRaft call", err)
		return ///os.exit? since server failed to initialize??
	} else {
		raftObj.connHandler(ftimeout, etimeout)
	}
}

func NewRaft(cluster *ClusterConfig, thisServerId int, commitCh chan *LogEntry) (raftObj *Raft, err error) {
	err = nil
	var myObj, leaderObj ServerConfig
	var nextIndexMap = make(map[int]int)
	var f_details = make(map[int]*followerDetails)

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
		f_obj := followerDetails{false, nil}
		f_details[i] = &f_obj
	}

	//Setting paths of disk files
	pathString_CV, pathString_Log = CreateDiskFiles(thisServerId)
	//fmt.Println("I am:", thisServerId, " Path strings are:", pathString_CV, pathString_Log)
	eventCh := make(chan interface{})

	myLog := make([]LogVal, 0, 10)

	metaData := LogMetaData{-1, -2, -2, -1, nextIndexMap}
	raftObj = &Raft{*cluster, myObj, leaderObj, 0, commitCh, eventCh, -1, -1, myLog, metaData, f_details, pathString_CV, pathString_Log}

	//server_raft_map[myObj.Id] = raftObj //NOT NEEDED NOW--REMOVE
	fmt.Println("Exiting NewRaft")
	return raftObj, err
}

//=====================++New Code++=========================

//timeout param added Only for testing
func (r *Raft) ServerSM(f int, e int) {
	state := follower //how to define type for this?--const
	for {
		switch state {
		case follower:
			//fmt.Println("in case follower")
			state = r.follower(f)
		case candidate:
			//fmt.Println("in case candidate of ServSM()")
			state = r.candidate(e)
		case leader:
			//fmt.Println("in case leader")
			state = r.leader()
		default:
			return

		}
	}
}

func CreateDiskFiles(thisServerId int) (pathString_CV string, pathString_Log string) {
	folderString := "./DiskLog/S" + strconv.Itoa(thisServerId)
	err := os.MkdirAll(folderString, 0777)
	if err != nil {
		checkErr("Error in creating directory DiskLog", err)
		panic(err)
	}

	pathString_CV = folderString + "/CV.log"
	pathString_Log = folderString + "/Log.log"
	fhcv, err1 := os.Create(pathString_CV)
	if err1 != nil {
		checkErr("Error in creating file CV.log", err)
		panic(err1)
	} else {
		fhcv.Close()
	}

	fhlog, err2 := os.Create(pathString_Log)
	if err2 != nil {
		checkErr("Error in creating file Log.log", err)
		panic((err2))
	} else {
		fhlog.Close()
	}
	return pathString_CV, pathString_Log

}
