package Raft
import (
	//"strings"
	"testing"
	//"time"
	"fmt"
)

func TestServer(t *testing.T) {
    //give dummy values
    clustObj:=new(ClusterConfig)
    fmt.Println("In test server")
    var slObj ServerConfig    
    slObj.Id=0
    slObj.Hostname="Server0"
    slObj.ClientPort=8000
    slObj.LogPort=9000
    
    sfObj := ServerConfig{1,"Server1",8001,9001}
    clustObj.Servers=[]ServerConfig{slObj, sfObj}
    
    go ServerStart(custObj,0)
    go ServerStart(custObj,1)
}
