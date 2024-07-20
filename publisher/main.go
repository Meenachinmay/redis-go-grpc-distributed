package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"time"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	// Test connection
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Successfully connected to Redis")

	time.Sleep(1 * time.Second)

	for i := 0; ; i++ {
		// broadcast
		message := fmt.Sprintf("This is secret #%d", i)
		log.Printf("Broadcasting message: %s", message)
		err := rdb.Publish(ctx, "broadcast", message).Err()
		if err != nil {
			log.Printf("Error publishing message: %v", err)
		} else {
			log.Printf("Published message: %s", message)
		}
		time.Sleep(3 * time.Second)
	}
}
