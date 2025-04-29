package crawl

import (
	"sync"
)

// MultiErrGroup is a wait group struct similar to errgroup.Group
// but collects all errors and supports limiting concurrency.
type MultiErrGroup struct {
	wg     sync.WaitGroup
	mu     sync.Mutex
	errors []error
	sem    chan struct{}
}

// SetLimit sets the maximum number of goroutines that can run concurrently.
// Should be called before any Go() calls.
func (g *MultiErrGroup) SetLimit(n int) {
	if n > 0 {
		g.sem = make(chan struct{}, n)
	}
}

// Go starts a goroutine and captures any returned error.
func (g *MultiErrGroup) Go(f func() error) {
	g.wg.Add(1)
	go func() {
		// Acquire a token if limiting is enabled
		if g.sem != nil {
			g.sem <- struct{}{}
			defer func() { <-g.sem }()
		}

		defer g.wg.Done()
		if err := f(); err != nil {
			g.mu.Lock()
			defer g.mu.Unlock()
			g.errors = append(g.errors, err)
		}
	}()
}

// Wait blocks until all goroutines have finished and returns all collected errors.
func (g *MultiErrGroup) Wait() []error {
	g.wg.Wait()
	if len(g.errors) == 0 {
		return nil
	}
	return g.errors
}
