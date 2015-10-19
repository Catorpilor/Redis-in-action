package main

import (
	"fmt"
	"sync"
	"time"
)

//first stage
func gen(done <-chan struct{}, nums ...int) <-chan int {
	out := make(chan int, len(nums))
	defer close(out)
	for _, num := range nums {
		select {
		case out <- num:
		case <-done:
			fmt.Println("gen out")
		}
	}
	return out
}

//second stage receive from gen and emit square of each received integer
func sq(done <-chan struct{}, in <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for n := range in {
			select {
			case out <- n * n:
			case <-done:
				fmt.Println("sq out")
				return
			}
		}
	}()
	return out
}

//fan-out
//multiple functions can read from the same channel until that channel is
//closed which provides a way to distribute work amongst a group of workers
//to parallize CPU use and I/O

//fan-in
//A function can read from multiple inputs and proceed until all are closed
//by multiplexing the input channels onto a single channel that's closed when
//all the inputs are closed.

func merge(done <-chan struct{}, cs ...<-chan int) <-chan int {
	var wg sync.WaitGroup
	// out := make(chan int, 1) //bad approach
	out := make(chan int)
	//Starts an ouput goroutine for each input channel in cs. output
	//copies values from c to out until c is closed,
	// (newly add)
	// or it receives a value from done,
	//  then call wg.Done
	output := func(c <-chan int) {
		defer wg.Done()
		for n := range c {
			select {
			case out <- n:
			case res := <-done:
				fmt.Println(res)
				return
			}
		}
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}
	//Start a goroutine to close out once all the output goroutine are
	//done. This must start after the wg.Add call
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func main() {
	// quit := make(chan bool)
	// go func() {
	// 	for {
	// 		select {
	// 		case <-quit:
	// 			fmt.Println("1 exit")
	// 			return
	// 			// default:
	// 			// 	fmt.Println("test")
	// 		}
	// 	}
	// }()
	// go func() {
	// 	for {
	// 		select {
	// 		case <-quit:
	// 			fmt.Println("2 exit")
	// 			return
	// 			// default:
	// 			// 	fmt.Println("test2")
	// 		}
	// 	}
	// }()
	// time.Sleep(time.Second * 2)
	// quit <- true
	// quit <- true
	//set up the pipeline and runs the final stage
	done := make(chan struct{})
	defer func() {
		close(done)
		time.Sleep(time.Second * 5)
		fmt.Println("dfd")
	}()
	c := gen(done, 2, 3, 4, 5, 6)
	c1 := sq(done, c)
	c2 := sq(done, c)

	//consume the output
	out := merge(done, c1, c2)
	fmt.Println(<-out)

	//Tell the remaining senders we're leaving

}
