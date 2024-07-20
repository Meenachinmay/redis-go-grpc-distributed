package main

import (
	"context"
	broadcasts "distributed-redis/proto"
	"fmt"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	connectionCounts int64
)

type Server struct {
	broadcasts.UnimplementedBroadcasterServer
	redisClient *redis.Client
	mu          sync.RWMutex
	clients     map[string]chan *broadcasts.BroadcastMessage
	done        struct{}
}

func NewServer(redisAddr string) *Server {
	return &Server{
		redisClient: redis.NewClient(&redis.Options{
			Addr:     redisAddr,
			Password: "",
			DB:       0,
		}),
		clients: make(map[string]chan *broadcasts.BroadcastMessage),
	}
}

func main() {
	// Increase file descriptor limit
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		log.Println("Error getting Rlimit:", err)
	}
	rLimit.Cur = rLimit.Max
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		log.Println("Error setting Rlimit:", err)
	}
	// Increase the number of operating system threads
	runtime.GOMAXPROCS(runtime.NumCPU())

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Configure keepalive and other options
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 15 * time.Minute,
			Time:              5 * time.Second,
			Timeout:           1 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.MaxConcurrentStreams(100000),
		grpc.InitialWindowSize(1 << 26),
		grpc.InitialConnWindowSize(1 << 26),
		grpc.WriteBufferSize(1 << 20),
		grpc.ReadBufferSize(1 << 20),
	}

	s := grpc.NewServer(opts...)
	server := NewServer("redis:6379")
	ctx := context.Background()
	_, err = server.redisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("failed to start server: %v", err)
	} else {
		log.Printf("redis server is running")
	}

	broadcasts.RegisterBroadcasterServer(s, server)

	go server.StartRedisSubscriber(context.Background())
	log.Println("started grpc on port: 50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}

func (s *Server) Subscribe(_ *broadcasts.SubscribeRequest, stream broadcasts.Broadcaster_SubscribeServer) error {
	clientID := generateUniqueID()
	log.Printf("New client subscribed: %s", clientID)
	clientChan := make(chan *broadcasts.BroadcastMessage, 1000000)

	s.mu.Lock()
	s.clients[clientID] = clientChan
	atomic.AddInt64(&connectionCounts, 1)
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.clients, clientID)
		s.mu.Unlock()
		close(clientChan)
		log.Printf("Client unsubscribed: %s", clientID)
	}()

	go func() {
		for msg := range clientChan {
			log.Printf("Sending message to client %s: %s", clientID, msg.Content)
			if err := stream.Send(msg); err != nil {
				log.Printf("Error sending message to client %s: %v", clientID, err)
				continue
			}
		}
	}()

	select {
	case <-stream.Context().Done():
		log.Printf("Client context done: %s", clientID)
		return stream.Context().Err()
	}
}

func (s *Server) StartRedisSubscriber(ctx context.Context) {
	pubsub := s.redisClient.Subscribe(ctx, "broadcast")
	defer pubsub.Close()

	log.Println("Started Redis subscriber")

	ch := pubsub.Channel()
	log.Println("Waiting for messages...")
	for msg := range ch {
		log.Printf("Received message from Redis: %s", msg.Payload)
		broadcastMsg := &broadcasts.BroadcastMessage{Content: msg.Payload}
		s.mu.RLock()
		for clientID, clientChan := range s.clients {
			select {
			case clientChan <- broadcastMsg:
				log.Printf("Queued message for client %s", clientID)
			default:
				log.Printf("Warning: Broadcast channel for client %s is full", clientID)
			}
		}
		s.mu.RUnlock()
	}
}

func generateUniqueID() string {
	return fmt.Sprintf("client-%d", time.Now().UnixNano())
}

//// monitor for create transfer service
//func (s *Server) monitorProgress() {
//	ticker := time.NewTicker(3 * time.Second)
//	defer ticker.Stop()
//
//	for {
//		select {
//		case <-ticker.C:
//			fmt.Printf("total connections: %d\n", atomic.LoadInt64(&connectionCounts))
//		}
//	}
//}
