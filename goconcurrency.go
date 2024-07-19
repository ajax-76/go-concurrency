package main

import (
	"container/heap"
	"fmt"
	"math"
	"os"
	"strconv"
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

func NewMultiplexer(numWorkers int, totalRequests int) *MultiPlexer {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	workerPool := NewWorkerPool(numWorkers, totalRequests)
	return &MultiPlexer{
		pq:         pq,
		workerPool: workerPool,
	}
}

type WorkerPool struct {
	workChan chan *Request
	wg sync.WaitGroup
}

func NewWorkerPool(numWorkers int, totalRequests int) *WorkerPool {
	workChan := make(chan *Request, totalRequests)
	pool := &WorkerPool{workChan: workChan}
	pool.startWorkers(numWorkers)
	return pool
}

func (p *WorkerPool) startWorkers(numWorkers int) {
	// workers working to process the data
	for i := 0; i < numWorkers; i++ {
		p.wg.Add(1)
		go func(id int) {
			defer p.wg.Done()
			for req := range p.workChan {
				time.Sleep(10 * time.Second)
				reqCh := req
				_ = reqCh // Assume each request takes 10 seconds to process
				//p.outChan <- req
			}
		}(i)

	}
}

func (p *WorkerPool) submitRequest(req *Request) {
	p.workChan <- req
}

func (m *MultiPlexer) AddRequest(req *Request) {
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

func (mux *MultiPlexer) addRequestToPriorityQue(
	inputChannel chan *Request, totalRequests int, k int) {
	processed := totalRequests
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	batchSize := k
	requestsProcessed := 0
	// processing request batch size per minute
	for req := range inputChannel {
		if requestsProcessed < batchSize {
			mux.AddRequest(req)
			requestsProcessed++
		} else {
			mux.AddRequest(req)
			processed = processed - requestsProcessed
			fmt.Printf("requests : %v \n", requestsProcessed)
			fmt.Printf("queued : %v \n", processed)
			<-ticker.C
			requestsProcessed = 1
		}
	}
	if requestsProcessed > 0 {
		processed = processed - requestsProcessed
		fmt.Printf("requests : %v \n", requestsProcessed)
		fmt.Printf("queued : %v \n", processed)
	}
}

func main() {

	m, _ := strconv.Atoi(os.Getenv("M_REQUESTS_PER_SEC"))
	u, _ := strconv.Atoi(os.Getenv("U_USERS"))
	us, _ := strconv.Atoi(os.Getenv("REQUEST_PER_USER"))
	// Validate constraints
	fmt.Printf("constraints %v : %v : %v \n", m, u, us)
	requestCompletionTimeSeconds := 10
	numUsers := u
	requestsPerUser := us
	totalRequests := numUsers * requestsPerUser
	n := m * 60                           // request per minute per user
	k := n * requestCompletionTimeSeconds // request  per user
	if !(u < m*60 && m*60 <= n && n < k) {
		fmt.Println("Invalid constraints.")
		return
	}
	numWorkers := k * requestCompletionTimeSeconds / 60                         // To handle k requests in a minute, with each taking 10 seconds
	estimatedTimeToComplete := int(math.Ceil(float64(totalRequests*60/k) / 60)) //estimated time to complete
	fmt.Printf("Ceiling Value: %v\n", estimatedTimeToComplete)
	inputChannel := make(chan *Request, totalRequests)
	mux := NewMultiplexer(numWorkers, totalRequests)
	simulateRequests(numUsers, requestsPerUser, inputChannel)
	start := time.Now()
	go mux.addRequestToPriorityQue(inputChannel, totalRequests, k)
	time.Sleep(time.Duration(estimatedTimeToComplete) * time.Minute)
	mux.workerPool.shutdown()
	if mux.pq.Len() == 0 {
		fmt.Println("all request processed")
	}
	fmt.Printf("heap length :  %v \n", mux.pq.Len())
	duration := time.Since(start)
	fmt.Printf("Total time taken: %s\n", duration)
}
