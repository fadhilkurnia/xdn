package batcher

import (
	"fmt"
	"log"
	"time"

	"github.com/rqlite/gorqlite"
)

type writeRequest struct {
	stmt     gorqlite.ParameterizedStatement
	response chan writeResponse
}

type writeResponse struct {
	result gorqlite.WriteResult
	err    error
}

// Batcher collects concurrent write operations and sends them to rqlite
// in a single HTTP request using gorqlite's WriteParameterized API.
// Callers block until the batch is flushed and their individual result
// is available.
type Batcher struct {
	conn     *gorqlite.Connection
	reqCh    chan writeRequest
	maxBatch int
	window   time.Duration
	stopCh   chan struct{}
	done     chan struct{}
}

// New creates a Batcher that connects to rqlite at the given DSN.
// maxBatch is the maximum number of statements per batch.
// windowMs is the maximum time (in milliseconds) to wait before flushing.
func New(dsn string, maxBatch int, windowMs int) (*Batcher, error) {
	conn, err := gorqlite.Open(dsn)
	if err != nil {
		return nil, fmt.Errorf("batcher: failed to open gorqlite connection: %w", err)
	}
	// Disable transaction wrapping so one failing statement does not
	// roll back the entire batch. Each UPSERT is independent.
	conn.SetExecutionWithTransaction(false)

	return &Batcher{
		conn:     conn,
		reqCh:    make(chan writeRequest, 4096),
		maxBatch: maxBatch,
		window:   time.Duration(windowMs) * time.Millisecond,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}, nil
}

// Start launches the background goroutine that collects and flushes batches.
func (b *Batcher) Start() {
	go b.loop()
	log.Printf("batcher: started (maxBatch=%d, window=%v)", b.maxBatch, b.window)
}

// Stop signals the batcher to shut down and waits for it to drain.
func (b *Batcher) Stop() {
	close(b.stopCh)
	<-b.done
	b.conn.Close()
	log.Println("batcher: stopped")
}

// Submit sends a write statement to the batcher and blocks until the
// batch containing this statement is flushed and the result is available.
func (b *Batcher) Submit(query string, args ...interface{}) error {
	req := writeRequest{
		stmt: gorqlite.ParameterizedStatement{
			Query:     query,
			Arguments: args,
		},
		response: make(chan writeResponse, 1),
	}
	select {
	case b.reqCh <- req:
	case <-b.stopCh:
		return fmt.Errorf("batcher: shutting down")
	}
	resp := <-req.response
	return resp.err
}

func (b *Batcher) loop() {
	defer close(b.done)
	for {
		// Wait for the first request or shutdown.
		var first writeRequest
		select {
		case first = <-b.reqCh:
		case <-b.stopCh:
			b.drain()
			return
		}

		// Collect a batch: accumulate more requests until the time
		// window expires or the batch is full.
		batch := []writeRequest{first}
		timer := time.NewTimer(b.window)

	collect:
		for len(batch) < b.maxBatch {
			select {
			case req := <-b.reqCh:
				batch = append(batch, req)
			case <-timer.C:
				break collect
			case <-b.stopCh:
				timer.Stop()
				b.flush(batch)
				b.drain()
				return
			}
		}
		timer.Stop()

		b.flush(batch)
	}
}

func (b *Batcher) flush(batch []writeRequest) {
	if len(batch) == 0 {
		return
	}

	// Build the slice of statements.
	stmts := make([]gorqlite.ParameterizedStatement, len(batch))
	for i, req := range batch {
		stmts[i] = req.stmt
	}

	// Send the batch to rqlite.
	results, err := b.conn.WriteParameterized(stmts)

	// Distribute results to callers.
	if err != nil {
		// API-level error: all callers get the same error.
		for _, req := range batch {
			req.response <- writeResponse{err: err}
		}
		return
	}

	// Per-statement results.
	for i, req := range batch {
		if i < len(results) {
			req.response <- writeResponse{
				result: results[i],
				err:    results[i].Err,
			}
		} else {
			req.response <- writeResponse{
				err: fmt.Errorf("batcher: missing result for statement %d", i),
			}
		}
	}
}

// drain flushes any remaining requests in the channel before shutdown.
func (b *Batcher) drain() {
	for {
		select {
		case req := <-b.reqCh:
			b.flush([]writeRequest{req})
		default:
			return
		}
	}
}
