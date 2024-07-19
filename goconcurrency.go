package main

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

type Payload struct {
	uuid string
}
type Request struct {
	id       string
	payLoad  Payload
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
	pq         PriorityQueue
	wg         sync.WaitGroup
	workerPool *WorkerPool
}

func NewMultiplexer(numWorkers int) *MultiPlexer {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	workerPool := NewWorkerPool(numWorkers)
	return &MultiPlexer{
		pq:         pq,
		workerPool: workerPool,
	}
}

type WorkerPool struct {
	workChan chan *Request
	outChan  chan *Request
	wg       sync.WaitGroup
}

func NewWorkerPool(numWorkers int) *WorkerPool {
	workChan := make(chan *Request, 10000000)
	outChan := make(chan *Request, 10000000)
	pool := &WorkerPool{workChan: workChan, outChan: outChan}
	pool.startWorkers(numWorkers)
	return pool
}

func (p *WorkerPool) startWorkers(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		p.wg.Add(1)
		go func(id int) {
			//defer close(p.outChan)
			defer p.wg.Done()
			for req := range p.workChan {
				// Simulate work
				time.Sleep(10 * time.Second) // Assume each request takes 10 seconds to process
				//fmt.Printf("Worker %d finished processing request %s\n", id, req.payLoad.uuid)
				p.outChan <- req
			}
		}(i)

	}
	// for i := 0; i < numCPU; i++ {
	//     <-c    // wait for one task to complete
	// }
}

func (p *WorkerPool) submitRequest(req *Request) {
	p.workChan <- req
}

func (m *MultiPlexer) AddRequest(req *Request) {
	//m.mu.Lock()
	//defer m.mu.Unlock()
	req.priority = int(time.Since(req.arrival).Milliseconds())

	heap.Push(&m.pq, req)
	m.scheduleNext()
}

func (m *MultiPlexer) scheduleNext() {
	if len(m.pq) > 0 {
		req := heap.Pop(&m.pq).(*Request)
		m.workerPool.submitRequest(req)
	}
}

func (p *WorkerPool) shutdown() {
	close(p.workChan)
	close(p.outChan)
	p.wg.Wait()
}

func simulateRequests(numUsers int, requestsPerUser int, inputChannel chan *Request) {
	go func() {
		for j := 0; j < numUsers; j++ {
			for i := 0; i < requestsPerUser; i++ {
				inputChannel <- &Request{
					id:      fmt.Sprintf("id-%v-%v", j, i),
					arrival: time.Now(),
					payLoad: Payload{uuid: fmt.Sprintf("uuid-%d-%d", j, i)},
				}
			}
		}
		close(inputChannel)
	}()
}

// func (m *MultiPlexer)AddRequestToPriorityQue(){

// }

func main() {
	numUsers := 20
	requestsPerUser := 1200
	totalRequests := numUsers * requestsPerUser
	numWorkers := 18000 / 6
	inputChannel := make(chan *Request, totalRequests)
	mux := NewMultiplexer(numWorkers)
	simulateRequests(numUsers, requestsPerUser, inputChannel)
	start := time.Now()
	processed := totalRequests
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		batchSize := 18000
		requestsProcessed := 0
		for req := range inputChannel {
			if requestsProcessed < batchSize {
				mux.AddRequest(req)
				requestsProcessed++
			} else {
				mux.AddRequest(req)
				processed = processed - requestsProcessed
				fmt.Printf("queued : %v \n", processed)
				fmt.Printf("processed : %v \n", requestsProcessed)
				<-ticker.C
				requestsProcessed = 1
			}
		}
		if requestsProcessed > 0 {
			processed = processed - requestsProcessed
			fmt.Printf("queued : %v \n", processed)
			fmt.Printf("processed : %v \n", requestsProcessed)
		}

	}()
	fmt.Println("channel closed")
	time.Sleep(2 * time.Minute)
	mux.workerPool.shutdown()
	if mux.pq.Len() == 0 {
		fmt.Println("all request processed")
	}
	fmt.Printf("heap length :  %v \n", mux.pq.Len())
	duration := time.Since(start)
	fmt.Printf("Total time taken: %s\n", duration)
}
