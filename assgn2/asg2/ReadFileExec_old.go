package main

import (
"encoding/json"
"fmt"
"os"
"io/ioutil"
"strconv"
"sync"
"time"
"./raft"
)

func readJson() raft.ClusterConfig{
	var v raft.ClusterConfig;
	fi, err := os.Open("json.json")

	if err != nil {
		fmt.Println(err)
		return v
	}
	b,e := ioutil.ReadAll(fi);
	if e!=nil{
		return v
	}

	var f interface{}
		
	err = json.Unmarshal(b, &f)
	if err !=nil{
		fmt.Println(err)
		return v
	}
	m := f.(map[string]interface{})
	for _, v1 := range m {
	switch vv1 := v1.(type){
		case string:						
			v.Path = vv1
					
		case []interface{}:
		switch vv := m["ServerConfig"].(type) {						
			case []interface{}:	
												
				for _, u := range vv {
					m2 := u.(map[string]interface{})
					var servCfg raft.ServerConfig
					switch vv2 := m2["ClientPort"].(type){
					case string:						
						servCfg.ClientPort,_ = strconv.Atoi(vv2)	
					
					}
					switch vv2 := m2["Hostname"].(type){
					case string:
						servCfg.Hostname = vv2
				
					}
					switch vv2 := m2["LogPort"].(type){
					case string:			
						servCfg.LogPort,_ = strconv.Atoi(vv2)
					}
					switch vv2 := m2["Id"].(type){
					case string:						
						servCfg.Id,_ = strconv.Atoi(vv2)
					}
					v.Servers = append(v.Servers,servCfg)

				}
							
	        	}
						
		}
					
	}	
	return v;
}


func main(){
	var v raft.ClusterConfig;
	
	v = readJson()			
	
	i := len(v.Servers)
	k := -1
	valueLock := &sync.Mutex{}
	var wg sync.WaitGroup
	for j:=0;j<i;j++ {		

		go func(){
			wg.Add(1)
			valueLock.Lock()
			
			k = k+1
			thisServer := v.Servers[k].Id
			valueLock.Unlock()
			raft.ServerStart(&v,thisServer)			
		}()			
	}
	time.Sleep(time.Second*5)
	wg.Wait()
}
