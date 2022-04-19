package main

import (
	"fmt"
	"math/rand"
	"time"
)


type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}
type Raft struct {
	log      []*LogEntry
}

func main() {

	//raft := Raft{}
	//raft.log = append(raft.log, &LogEntry{
	//	Term:    0,
	//	Index:   0,
	//	Command: nil,
	//})
	//raft.log = append(raft.log, &LogEntry{
	//	Term:    0,
	//	Index:   0,
	//	Command: nil,
	//})
	//raft.log = append(raft.log, &LogEntry{
	//	Term:    0,
	//	Index:   0,
	//	Command: nil,
	//})
	//raft.log = append(raft.log, nil)
	//index:=0
	//raft.log = raft.log[:index]
	//fmt.Printf("%v-%v-%v",raft.log[0],raft.log[0], len(raft.log))
	//fmt.Println()


	var aaa []int
	aaa = append(aaa, 1)
	aaa = append(aaa, 2)

	var bbb []int
	bbb = append(bbb, 3)
	bbb = append(bbb, 4)

	aaa = append(aaa, bbb...)

	fmt.Printf("%v",aaa[2:])


	//fmt.Println("asd")
	//value := []string{}
	//
	//for i:=1; i< 10 ; i++ {
	//	value = append(value, "1")
	//	fmt.Println(value)
	//}
	//
	//unix := time.Now().Unix()
	//a:=0
	//fmt.Println(a)
	//i := time.Now().Unix()
	//fmt.Println(i - unix)
	//
	////strconv.Itoa(value)
	//
	//var wg sync.WaitGroup
	//wg.Add(4)
	//for i := 0; i < 4; i++ {
	//	go func(lost int) {
	//		time.Sleep(5 * time.Second)
	//		fmt.Println("哈哈 " + strconv.Itoa(lost))
	//		wg.Done()
	//	}(i )
	//}
	//wg.Wait()
	//fmt.Println("结束")
	//
	for i :=0; i < 10 ; i++  {
		rand.Seed(time.Now().Unix())
		intn := rand.Intn(150)
		fmt.Println(intn)
		time.Sleep(50*time.Millisecond)
		//c := make([]int,5)
		//fmt.Println(c)
	}
	fmt.Println("Asd")
	//k :=1
	//go func(lost int) {
	//	fmt.Println("dsa")
	//	for i:=1 ; i < 100000; i++{
	//		fmt.Println(i)
	//	}
	//}(k)
	//fmt.Println("Asd")
	//time.Sleep(10*time.Second)
}
