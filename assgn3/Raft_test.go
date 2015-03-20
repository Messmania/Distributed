//Raft_test
package raft

import (
	//"fmt"
	//"strings"
	"testing"
	"time"
)

var r0, r1, r2, r3, r4 *Raft

var hostname string = "localhost"
var port int = 9000

func Test_StartServers(t *testing.T) {
	//make ClusterConfig object, init it and set 0 as leader for start
	clustObj := &ClusterConfig{} //set values}
	clustObj.Path = "null"
	servers := []ServerConfig{ServerConfig{0}, ServerConfig{1}, ServerConfig{2}, ServerConfig{3}}
	clustObj.Servers = servers
	//start all the servers by calling ServerStart for each
	//Their raft object must also be collected as returned value so that appriopriate server's Append is called
	r0 = ServerStart(clustObj, 0)
	r1 = ServerStart(clustObj, 1)
	r2 = ServerStart(clustObj, 2)
	r3 = ServerStart(clustObj, 3)
	r4 = ServerStart(clustObj, 4)
	//var testChan = make(chan int)
	t.Log("End of test method")
	a := time.Duration(100)
	time.Sleep(time.Second * a)
}
