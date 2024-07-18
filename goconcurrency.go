package main

import (
	"container/heap"
	"fmt"
	"time"
)

type Request struct {
	id       int
	arrival  time.Time
	priority int
}

// defined priorityy queue

type PriorityQueue []*Request

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*Request)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}
type MultiPlexer struct {
	pq PriorityQueue
	//workerPool *WorkerPool
}

func NewMultiplexer() *MultiPlexer {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	return &MultiPlexer{
		pq: pq,
		//workerPool: workerPool,
	}
}

// func (m *MultiPlexer) scheduleNext() {
// 	if len(m.pq) > 0 {
// 		req := heap.Pop(&m.pq).(*Request)
// 		m.workerPool.submitRequest(req)
// 	}
// }

func (mux *MultiPlexer) handleRequest(inputChannel <-chan *Request) <-chan Request {
	// Apply logic
	// 1. add the request to priority que
	// 2. submit request to worker pool to handle it
	// 3. expect next request

	processChannel := make(chan Request, 9)
	go func() {
		for req := range inputChannel {
			fmt.Printf("req :%v pushed to heap \n", req.id)
			req.priority = int(time.Since(req.arrival).Milliseconds())
			heap.Push(&mux.pq, req)
			processChannel <- *req
		}
		close(processChannel)
	}()
	return processChannel
}

func (mux *MultiPlexer) popFromHeap(inputChannel <-chan Request) <-chan Request {
	processChannel := make(chan Request, 9)
	go func() {
		for req := range inputChannel {
			fmt.Printf("channel id : %v is processing \n", req.id)
			item := heap.Pop(&mux.pq).(*Request)
			fmt.Printf("pop request: %v is processing \n", item.id)
			processChannel <- *item
		}
		close(processChannel)
	}()
	return processChannel
}

func (mux *MultiPlexer) ProcessRequest(inputChannel <-chan Request) <-chan Request {
	processChannel := make(chan Request, 9)
	go func() {
		for req := range inputChannel {
			time.Sleep(time.Second * 1)
			fmt.Printf("finished processing request %d\n", req.id)
			processChannel <- req
		}
		close(processChannel)
	}()
	return processChannel
}

func main() {
	inputChannel := make(chan *Request, 9)
	mux := NewMultiplexer()
	go func() {
		for i := 0; i < 10; i++ {
			inputChannel <- &Request{
				id:      i,
				arrival: time.Now(),
			}
		}
		close(inputChannel)
	}()
	b := mux.handleRequest(inputChannel)
	d := mux.popFromHeap(b)
	z := mux.ProcessRequest(d)
	for n := range z {
		fmt.Printf("request : %v is processed \n", n.id)
	}
	fmt.Println("channel closed")
}
