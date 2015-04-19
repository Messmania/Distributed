package main

import (
	"encoding/json"
	//"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"raft"
	"strconv"
	"sync"
	"time"
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
	var v raft.ClusterConfig
	args := os.Args
	filename := "json.json"
	if len(args) > 1 {
		filename = args[1]
	}
	//for waits
	w0 := rand.Intn(13)
	w1 := rand.Intn(5)
	w2 := rand.Intn(10)
	w3 := rand.Intn(10)
	w4 := rand.Intn(22)
	w := []int{w0, w1, w2, w3, w4}

	v = readJson(filename)

	i := len(v.Servers)
	k := -1

	vstr := v.Path + "."
	for k1 := 0; k1 < i; k1++ {
		if k1 != 0 {
			vstr = vstr + ";"
		}
		vstr = vstr + strconv.Itoa(v.Servers[k1].Id) + "," + v.Servers[k1].Hostname + "," + strconv.Itoa(v.Servers[k1].ClientPort) + "," + strconv.Itoa(v.Servers[k1].LogPort)
	}
	valueLock := &sync.Mutex{}
	for j := 0; j < i; j++ {

		go func() {
			valueLock.Lock()

			k = k + 1
			thisServer := strconv.Itoa(v.Servers[k].Id)
			valueLock.Unlock()
			//cmd := exec.Command("go", "run", "./serverStarter/start1.go", vstr ,  thisServer)
			cmd := exec.Command("serverStarter", vstr, thisServer)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			err := cmd.Run()
			if err != nil {
				panic(err)
			}
			cmd.Wait()
		}()

	}
	time.Sleep(time.Second * 5)

}
