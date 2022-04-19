package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//
// map 输出的阶段要立即写到directory  以便与 reduce 及时消费
import (
	"../mr"
	"log"
)
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	log.Println("ccccc")
	for m.Done() == false {
		time.Sleep(time.Second)
	}
	log.Println("ccccc1")

	time.Sleep(time.Second)
	log.Println("ccccc2")

}
