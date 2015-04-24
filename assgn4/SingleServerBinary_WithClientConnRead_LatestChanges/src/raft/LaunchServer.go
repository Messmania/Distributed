package raft

//package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
)

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
	//var nextIndexMap = make(map[int]int)
	var f_details = make(map[int]*followerDetails)

	//var pathString_Log, pathString_CV string
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
		//nextIndexMap[i] = -1 //initialising nextIndexes for all in each server
		//f_obj := followerDetails{false, nil}
		f_obj := followerDetails{false, nil, -1}
		f_details[i] = &f_obj
	}

	pathString_CV, pathString_Log, myLog, metaData, myCV := ReadOrCreateDiskFiles(thisServerId)
	eventCh := make(chan interface{})
	raftObj = &Raft{*cluster, myObj, leaderObj, 0, commitCh, eventCh, myCV, myLog, metaData, f_details, pathString_CV, pathString_Log}
	return raftObj, err
}

//=====================A4: ++New Code++=========================

//Reads disk files and updates database, if they do not exist, then creates them
func ReadOrCreateDiskFiles(thisServerId int) (pathString_CV string, pathString_Log string, myLog []LogVal, myMD LogMetaData, myCV TermVotedFor) {
	myLog = make([]LogVal, 0)
	myMD = LogMetaData{-1, -2, -2, -1}
	myCV = TermVotedFor{-1, -1}
	folderString := "./DiskLog/S" + strconv.Itoa(thisServerId)
	pathString_CV = folderString + "/CV.log"
	pathString_Log = folderString + "/Log.log"
	_, err := os.Stat(folderString)
	if err != nil { //folder doesn't exist , then create the folder and files
		err := os.MkdirAll(folderString, 0777)
		if err != nil {
			checkErr("Error in creating directory DiskLog", err)
			panic(err)
		}

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
	} else { // folders and files exist
		//read the files into self data. i.e. myLog , metaData,cv
		myLog, myMD, myCV = ReadDiskFiles(pathString_Log, pathString_CV)
	}
	return pathString_CV, pathString_Log, myLog, myMD, myCV

}

func ReadDiskFiles(pathString_Log string, pathString_CV string) (arrMyLog []LogVal, myMD LogMetaData, myCV TermVotedFor) {
	arrMyLog, myMD = ReadLogFile(pathString_Log)
	myCV = ReadCVFile(pathString_CV)
	return arrMyLog, myMD, myCV
}

//This reads only the last line,
// each line size is 31bytes
func ReadCVFile(pathString_CV string) (myCV TermVotedFor) {
	fh, _ := os.Open(pathString_CV)
	stat, _ := os.Stat(pathString_CV)
	myCV = TermVotedFor{}
	a := make([]byte, 31)
	_, err2 := fh.ReadAt(a, stat.Size()-31)
	if err2 == io.EOF {
		checkErr("Error in ReadCVFile, end of file", err2)
		return
	}
	in := &TermVotedFor{}
	json.Unmarshal(a, &in)
	myCV = *in
	return

}
func ReadLogFile(pathString_Log string) (arrMyLog []LogVal, myMD LogMetaData) {
	fh, _ := os.Open(pathString_Log)
	bufReader := bufio.NewReader(fh)
	arrMyLog = make([]LogVal, 0)
	for {
		a, _, err := bufReader.ReadLine()
		if err == io.EOF {
			checkErr("End of file reached", err)
			break
		}

		in := &LogVal{}
		err1 := json.Unmarshal(a, &in)
		if err1 != nil {
			checkErr("In ReadLogFile,Error in unmarshalling", err1)
			break
		}
		arrMyLog = append(arrMyLog, *in)
	}
	for i := 0; i < len(arrMyLog); i++ {
		fmt.Println("val:", arrMyLog[i])
	}
	myMD = setMetaData(len(arrMyLog), arrMyLog)
	return arrMyLog, myMD

}

func setMetaData(l int, MyLog []LogVal) (MyMetaData LogMetaData) {
	MyMetaData.CommitIndex = l - 1 //for now
	MyMetaData.LastLogIndex = l - 1
	MyMetaData.PrevLogIndex = MyMetaData.LastLogIndex - 1
	if l == 0 {
		MyMetaData.PrevLogTerm = -2
	} else if l == 1 {
		MyMetaData.PrevLogTerm = -1
	} else {
		MyMetaData.PrevLogTerm = MyLog[MyMetaData.PrevLogIndex].Term
	}
	return MyMetaData
}
