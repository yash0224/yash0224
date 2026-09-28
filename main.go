
// This file is intentionally self-contained. Run only it with:
//
//	go run main.go
package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Order is a small business object. Structs group related data together.
type Order struct {
	ID       string
	Product  string
	Quantity int
}

// Result is sent back to the main goroutine after an order is handled.
type Result struct {
	OrderID string
	Status  string
	Err     error
}

// Inventory is shared by multiple worker goroutines, so its map needs a mutex.
type Inventory struct {
	mu    sync.Mutex
	stock map[string]int
}

//	type Inventory struct {
//		mu sync.Mutex
//		stock map[string]int
//	}
//
// reserve safely changes shared state and returns whether stock was available.
func (i *Inventory) reserve(product string, quantity int) bool {
	i.mu.Lock()
	defer i.mu.Unlock() // Always unlock, including if this function returns early.

	if i.stock[product] < quantity {
		return false
	}
	i.stock[product] -= quantity
	return true
}

func main() {
	// rootCtx is the parent context for the application.
	rootCtx := context.Background()
	// requestCtx is a child context for this order-processing request.
	requestCtx, cancelRequest := context.WithTimeout(rootCtx, time.Second)
	defer cancelRequest()

	inventory := &Inventory{stock: map[string]int{
		"laptop": 5,
		"mouse":  5,
	}}

	orders := []Order{
		{ID: "ORD-100", Product: "laptop", Quantity: 1},
		{ID: "ORD-101", Product: "laptop", Quantity: 2}, // not enough stock
		{ID: "ORD-102", Product: "mouse", Quantity: 1},
	}

	jobs := make(chan Order)     // channel used to send work to workers
	results := make(chan Result) // channel used to receive completed work

	var workers sync.WaitGroup // waits until all goroutines have finished
	for workerID := 1; workerID <= 2; workerID++ {
		workers.Add(1)
		go processOrders(requestCtx, workerID, jobs, results, inventory, &workers)
	}

	// Send jobs in a goroutine so main can concurrently receive results.
	go func() {
		defer close(jobs) // closed channels tell workers that no more jobs are coming
		for _, order := range orders {
			select {
			case jobs <- order:
				// The order was sent to a worker.
			case <-requestCtx.Done():
				return // Stop sending when the context is cancelled.
			}
		}
	}()

	// Close results only after every sender (worker) has stopped.
	go func() {
		workers.Wait()
		close(results)
	}()

	// range receives until the results channel is closed.
	for result := range results {
		switch {
		case result.Err != nil:
			fmt.Printf("%s: failed: %v\n", result.OrderID, result.Err)
		case result.Status == "approved":
			fmt.Printf("%s: approved\n", result.OrderID)
		default:
			fmt.Printf("%s: %s\n", result.OrderID, result.Status)
		}
	}

	fmt.Println("remaining stock:", inventory.stock)
}

// processOrders is run as a goroutine. It reads jobs and sends results.
func processOrders(ctx context.Context, workerID int, jobs <-chan Order, results chan<- Result, inventory *Inventory, workers *sync.WaitGroup) {
	defer workers.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case order, open := <-jobs:
			if !open {
				return // jobs was closed
			}

			result := fulfillOrder(ctx, workerID, order, inventory)
			select {
			case results <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}

func fulfillOrder(ctx context.Context, workerID int, order Order, inventory *Inventory) Result {
	if !inventory.reserve(order.Product, order.Quantity) {
		return Result{OrderID: order.ID, Err: fmt.Errorf("not enough %s in stock", order.Product)}
	}

	// paymentCtx is a child of ctx. It may have a shorter payment-specific deadline.
	paymentCtx, cancelPayment := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelPayment()

	if err := chargePayment(paymentCtx, order); err != nil {
		return Result{OrderID: order.ID, Err: err}
	}
	return Result{OrderID: order.ID, Status: fmt.Sprintf("approved by worker %d", workerID)}
}

// chargePayment runs local business checks before authorizing a payment.
// A production version would make an HTTP/RPC call to a payment provider here.
func chargePayment(ctx context.Context, order Order) error {
	if order.ID == "" {
		return fmt.Errorf("payment rejected: missing order ID")
	}
	if order.Quantity <= 0 {
		return fmt.Errorf("payment rejected: quantity must be positive")
	}

	// This non-blocking select checks whether this child context is already done.
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil // The local payment authorization succeeded.
	}
}
