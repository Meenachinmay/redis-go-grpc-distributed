package main

import (
	"context"
	broadcasts "distributed-redis/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	totalClients         = 30000
	connectionsPerSecond = 1000
)

func main() {
	var wg sync.WaitGroup
	var connectedClients int32
	done := make(chan struct{})

	// Rate limiter for connection
	rateLimiter := time.NewTicker(time.Second / time.Duration(connectionsPerSecond))
	//rateLimiter := time.NewTicker(time.Millisecond * 100)
	for i := 1; i <= totalClients; i++ {
		<-rateLimiter.C // Wait for the rate limiter
		wg.Add(1)
		go runClient(i, &wg, done, &connectedClients)
	}

	log.Printf("All clients finished. Total connected: %d", atomic.LoadInt32(&connectedClients))
	wg.Wait()
}

func runClient(id int, wg *sync.WaitGroup, done chan struct{}, connectedClients *int32) {
	defer wg.Done()

	backoff := time.Second
	maxBackoff := 2 * time.Minute

	for {
		select {
		case <-done:
			log.Printf("Client %d shutting down", id)
			return
		default:
			if err := connectAndListen(id, &backoff, done); err != nil {
				log.Printf("Client %d: connection attempt failed, retrying...", id)
				time.Sleep(backoff)
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			} else {
				atomic.AddInt32(connectedClients, 1)
				// If connectAndListen returns nil, it means we've shut down gracefully
				return
			}
		}
	}
}

func connectAndListen(id int, backoff *time.Duration, done chan struct{}) error {
	log.Printf("Client %d attempting to connect", id)
	conn, err := grpc.Dial("localhost:80", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`))
	if err != nil {
		log.Printf("Client %d failed to connect: %v", id, err)
		return err
	}
	defer conn.Close()
	log.Printf("Client %d connected successfully", id)

	client := broadcasts.NewBroadcasterClient(conn)
	stream, err := client.Subscribe(context.Background(), &broadcasts.SubscribeRequest{})
	if err != nil {
		log.Printf("Client %d failed to subscribe: %v", id, err)
		return err
	}
	log.Printf("Client %d subscribed successfully", id)

	// Reset backoff on successful connection
	*backoff = time.Second

	for {
		select {
		case <-done:
			log.Printf("Client %d shutting down", id)
			return nil
		default:
			msg, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					log.Printf("Client %d: Stream ended", id)
					return err
				}
				log.Printf("Client %d failed to receive message: %v", id, err)
				time.Sleep(time.Second)
				return err
			}
			log.Printf("Client %d received message: %s", id, msg.Content)
			time.Sleep(time.Second)
		}
	}
}
