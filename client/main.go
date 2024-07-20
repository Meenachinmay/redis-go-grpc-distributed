package main

import (
	"context"
	broadcasts "distributed-redis/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	// Start the monitor
	//go monitorConnections(&connectedClients)

	// Rate limiter for connection
	rateLimiter := time.NewTicker(time.Second / time.Duration(connectionsPerSecond))
	//rateLimiter := time.NewTicker(time.Millisecond * 100)
	for i := 1; i <= totalClients; i++ {
		<-rateLimiter.C // Wait for the rate limiter
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if runClient(id) {
				log.Println("client", id, "is connected & running")
				atomic.AddInt32(&connectedClients, 1)
			}
		}(i)
	}

	log.Printf("All clients finished. Total connected: %d", atomic.LoadInt32(&connectedClients))
	wg.Wait()
}

func runClient(id int) bool {
	log.Printf("Client %d attempting to connect", id)
	conn, err := grpc.Dial("localhost:80", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`))
	if err != nil {
		log.Printf("Client %d failed to connect: %v", id, err)
		return false
	}
	defer conn.Close()
	log.Printf("Client %d connected successfully", id)

	client := broadcasts.NewBroadcasterClient(conn)
	stream, err := client.Subscribe(context.Background(), &broadcasts.SubscribeRequest{})
	if err != nil {
		log.Printf("Client %d failed to subscribe: %v", id, err)
		return false
	}
	log.Printf("Client %d subscribed successfully", id)

	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Printf("Client %d failed to receive message: %v", id, err)
			return false
		} else {
			log.Printf("Client %d received message: %s", id, msg.Content)
		}
	}
}

//func monitorConnections(connectedClients *int32) {
//	ticker := time.NewTicker(3 * time.Second)
//	for range ticker.C {
//		connected := atomic.LoadInt32(connectedClients)
//		log.Printf("Currently connected clients: %d / %d", connected, totalClients)
//	}
//}
