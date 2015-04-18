package main

import (
	"fmt"
	"math/rand"
	"raft"
)

func main() {
	Server0 := raft.ServerConfig{0, "localhost", 9000, 8000}
	Server1 := raft.ServerConfig{1, "localhost", 9001, 8001}
	Server2 := raft.ServerConfig{2, "localhost", 9002, 8002}
	Server3 := raft.ServerConfig{3, "localhost", 9003, 8003}
	Server4 := raft.ServerConfig{4, "localhost", 9004, 8004}
	ServerArr := []raft.ServerConfig{Server0, Server1, Server2, Server3, Server4}
	clustObj := &raft.ClusterConfig{ServerArr}

	//Just to pass timeouts, serversm is launched here, otherwise it should be launched in connHandler
	w0 := rand.Intn(13)
	w1 := rand.Intn(5)
	w2 := rand.Intn(10)
	w3 := rand.Intn(10)
	w4 := rand.Intn(22)

	fmt.Println("Waits are", w0, w1, w2, w3, w4)
	//fmt.Println("Waits are", w0, w1, w2)
	go raft.ServerStart(clustObj, 0, w0)
	go raft.ServerStart(clustObj, 1, w1)
	go raft.ServerStart(clustObj, 2, w2)
	go raft.ServerStart(clustObj, 3, w3)
	go raft.ServerStart(clustObj, 4, w4)

	myChan := make(chan int)
	fmt.Println("Waiting on channel")
	<-myChan

}
