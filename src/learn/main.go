package main

import (
	"fmt"
	"sync"
	"time"
)

func exitTest(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	time.Sleep(time.Second * 2)
	fmt.Println("exitTest")
}

func chanTest(c chan int) {
	time.Sleep(2 * time.Second)
	c <- 5
}

func chanRangeTest(c chan int) {
	defer close(c)
	for i := 20; i < 30; i++ {
		c <- i
		time.Sleep(time.Millisecond * 200)
	}
}

func selectTest(c1 chan int, c2 chan int) {
	time.Sleep(time.Second)
	c1 <- 11
	time.Sleep(time.Second)
	c2 <- 22
}

func main() {
	fmt.Println("Hello, World!")
	var z map[string]float64
	z["pi"] = 3.1415
	fmt.Println("start wait group")
	var waitGroup sync.WaitGroup
	for i := 1; i <= 5; i++ {
		waitGroup.Add(1)
		go exitTest(&waitGroup)
	}
	waitGroup.Wait()

	fmt.Println("start chan")
	var chan1 chan int = make(chan int, 1)
	go chanTest(chan1)
	var getChanValue int = <-chan1
	fmt.Println("get chan value: " + fmt.Sprint(getChanValue))

	fmt.Println("start chan range")
	chan2 := make(chan int, 10)
	go chanRangeTest(chan2)
	for i := range chan2 {
		fmt.Println("chan2 range: " + fmt.Sprint(i))
	}

	fmt.Println("start chan select")
	chan3, chan4 := make(chan int), make(chan int)
	go selectTest(chan3, chan4)
	for {
		c := 0
		select {
		case a := <-chan3:
			fmt.Println("chan3 get Value: " + fmt.Sprint(a))
		case b := <-chan4:
			fmt.Println("chan4 get Value: " + fmt.Sprint(b))
			c = 1
		default:
			fmt.Println("no chan get value")
			time.Sleep(100 * time.Millisecond)
		}
		if c == 1 {
			break
		}
	}

	fmt.Println("start condition")
	var m sync.Mutex
	cond := sync.NewCond(&m)
	count, finished := 0, 0
	for i := 0; i < 10; i++ {
		go func() {
			vote := true
			cond.L.Lock()
			defer cond.L.Unlock()
			if vote {
				count++
			}
			finished++
			cond.Broadcast()
		}()
	}
	cond.L.Lock()
	for count < 5 && finished != 10 {
		cond.Wait()
	}
	if count >= 5 {
		fmt.Println("condition pass")
	} else {
		fmt.Println("condition fail")
	}
	cond.L.Unlock()
	// cond.Broadcast()
	// cond.Signal()
	// cond.Wait() // 调用Wait之前需要先获取锁
	fmt.Println("finish main")
}
