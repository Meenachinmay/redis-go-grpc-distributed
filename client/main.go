package main

import (
	"context"
	broadcasts "distributed-redis/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"sync"
)

func main() {
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runClient(id)
		}(i)
	}
	wg.Wait()
}

func runClient(id int) {
	log.Printf("Client %d attempting to connect", id)
	conn, err := grpc.Dial("localhost:80", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Client %d failed to connect: %v", id, err)
		return
	}
	defer conn.Close()
	log.Printf("Client %d connected successfully", id)

	client := broadcasts.NewBroadcasterClient(conn)
	stream, err := client.Subscribe(context.Background(), &broadcasts.SubscribeRequest{})
	if err != nil {
		log.Printf("Client %d failed to subscribe: %v", id, err)
		return
	}
	log.Printf("Client %d subscribed successfully", id)

	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Printf("Client %d failed to receive message: %v", id, err)
			return
		}
		log.Printf("Client %d received: %s", id, msg.Content)
	}
}
