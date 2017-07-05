package zkclient

import (
	"testing"
	"fmt"
)

func TestSetupZk(t *testing.T){
	path := "./config/zkConfig"
	zkerr := SetupZk(path)
	if zkerr != nil {
		fmt.Println(zkerr.Error())
	}
}
