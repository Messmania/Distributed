package main

import (
	"encoding/json"
	///"fmt"
	"io/ioutil"
	"os"
	"raft"
	"strconv"
)

func readJson(filename string) raft.ClusterConfig {
	var v raft.ClusterConfig
	fi, err := os.Open(filename)

	if err != nil {
		//		fmt.Println(err)
		return v
	}
	b, e := ioutil.ReadAll(fi)
	if e != nil {
		return v
	}

	var f interface{}

	err = json.Unmarshal(b, &f)
	if err != nil {
		//		fmt.Println(err)
		return v
	}
	m := f.(map[string]interface{})
	for _, v1 := range m {
		switch vv1 := v1.(type) {
		case string:
			v.Path = vv1

		case []interface{}:
			switch vv := m["ServerConfig"].(type) {
			case []interface{}:

				for _, u := range vv {
					m2 := u.(map[string]interface{})
					var servCfg raft.ServerConfig
					switch vv2 := m2["ClientPort"].(type) {
					case string:
						servCfg.ClientPort, _ = strconv.Atoi(vv2)

					}
					switch vv2 := m2["Hostname"].(type) {
					case string:
						servCfg.Hostname = vv2

					}
					switch vv2 := m2["LogPort"].(type) {
					case string:
						servCfg.LogPort, _ = strconv.Atoi(vv2)
					}
					switch vv2 := m2["Id"].(type) {
					case string:
						servCfg.Id, _ = strconv.Atoi(vv2)
					}
					v.Servers = append(v.Servers, servCfg)

				}

			}

		}

	}
	return v
}

func main() {
	args := os.Args
	//args are filePath,Id,fTimeout,eTimeout,(crash var may b)
	if len(args) < 4 {
		panic("Argument length insufficient")
	}

	filename := args[1]

	v := readJson(filename)

	Id, _ := strconv.Atoi(args[2])
	eTimeout, _ := strconv.Atoi(args[4])
	fTimeout, _ := strconv.Atoi(args[3])

	raft.ServerStart(&v, Id, fTimeout, eTimeout)
	wait := make(chan int)
	<-wait
}
