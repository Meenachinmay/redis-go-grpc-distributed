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
	batchSize                  = 1000
	batchTimeout               = 500 * time.Millisecond
	connectionsPerSecond       = 100
	currentCount         int64 = 0
)

type Server struct {
	broadcasts.UnimplementedBroadcasterServer
	redisClient       *redis.Client
	mu                sync.RWMutex
	clients           map[string]chan *broadcasts.BroadcastMessage
	done              struct{}
	newConnections    chan broadcasts.Broadcaster_SubscribeServer
	connectionBatches chan []broadcasts.Broadcaster_SubscribeServer
	broadcastChannel  chan *broadcasts.BroadcastMessage
	messageCounter    int64
	messageStats      map[int64]*MessageStats
	statsMu           sync.RWMutex
}

type MessageStats struct {
	TotalClients int
	SentCount    int64
}

func NewServer(redisAddr string) *Server {
	return &Server{
		redisClient: redis.NewClient(&redis.Options{
			Addr:     redisAddr,
			Password: "",
			DB:       0,
		}),
		clients:           make(map[string]chan *broadcasts.BroadcastMessage),
		newConnections:    make(chan broadcasts.Broadcaster_SubscribeServer, 100000),
		connectionBatches: make(chan []broadcasts.Broadcaster_SubscribeServer, 1000),
		broadcastChannel:  make(chan *broadcasts.BroadcastMessage, 100000),
		messageStats:      make(map[int64]*MessageStats),
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
			MaxConnectionIdle: 20 * time.Minute,
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

	context, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.StartRedisSubscriber(context)
	go server.batchConnections()
	go server.handleConnections(runtime.NumCPU() * 2)
	go server.handleBroadcasts(runtime.NumCPU() / 2)
	go server.monitorMessageStats()

	log.Println("started grpc on port: 50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}

func (s *Server) Subscribe(_ *broadcasts.SubscribeRequest, stream broadcasts.Broadcaster_SubscribeServer) error {
	s.newConnections <- stream
	<-stream.Context().Done()
	return nil
}

func (s *Server) batchConnections() {
	var batch []broadcasts.Broadcaster_SubscribeServer
	timer := time.NewTimer(batchTimeout)
	rateLimiter := time.NewTicker(time.Second / time.Duration(connectionsPerSecond))

	for {
		select {
		case <-rateLimiter.C:
			select {
			case conn := <-s.newConnections:
				batch = append(batch, conn)
				if len(batch) >= batchSize {
					s.connectionBatches <- batch
					batch = nil
					timer.Reset(batchTimeout)
				}
			default:

			}
		case <-timer.C:
			if len(batch) > 0 {
				s.connectionBatches <- batch
				batch = nil
			}
			timer.Reset(batchTimeout)
		}
	}
}

func (s *Server) handleConnections(workers int) {
	for i := 0; i < workers; i++ {
		go func() {
			for batch := range s.connectionBatches {
				for _, stream := range batch {
					clientID := generateUniqueID()
					clientChan := make(chan *broadcasts.BroadcastMessage, 100)

					s.mu.Lock()
					s.clients[clientID] = clientChan
					atomic.AddInt64(&currentCount, 1)
					s.mu.Unlock()

					log.Printf("New client subscribed: %s, currentCount: %d", clientID, currentCount)

					go s.handleClientStream(clientID, clientChan, stream)
				}
			}
		}()
	}
}

func (s *Server) handleClientStream(clientID string, clientChan chan *broadcasts.BroadcastMessage, stream broadcasts.Broadcaster_SubscribeServer) {
	defer func() {
		s.mu.Lock()
		delete(s.clients, clientID)
		s.mu.Unlock()
		close(clientChan)
		log.Printf("Client unsubscribed: %s", clientID)
	}()

	for msg := range clientChan {
		if err := stream.Send(msg); err != nil {
			log.Printf("Error sending message to client %s: %v", clientID, err)
			return
		}
	}
}

func (s *Server) StartRedisSubscriber(ctx context.Context) {
	pubsub := s.redisClient.Subscribe(ctx, "broadcast")
	defer pubsub.Close()

	log.Println("Started Redis subscriber")

	ch := pubsub.Channel()
	for msg := range ch {
		messageID := atomic.AddInt64(&s.messageCounter, 1)
		log.Printf("Received message from Redis: %s (ID: %d)", msg.Payload, messageID)

		s.statsMu.Lock()
		s.messageStats[messageID] = &MessageStats{TotalClients: len(s.clients)}
		s.statsMu.Unlock()

		broadcastMsg := &broadcasts.BroadcastMessage{
			Content: msg.Payload,
			Id:      messageID,
		}
		s.broadcastChannel <- broadcastMsg
	}
}

func (s *Server) handleBroadcasts(workers int) {
	for i := 0; i < workers; i++ {
		go func() {
			for msg := range s.broadcastChannel {
				s.mu.RLock()
				clientCount := len(s.clients)
				for _, clientChan := range s.clients {
					select {
					case clientChan <- msg:
						atomic.AddInt64(&s.messageStats[msg.Id].SentCount, 1)
					default:
						// Channel full, skip this client
						log.Printf("Warning: Client channel full, skipping message %d", msg.Id)
					}
				}
				s.mu.RUnlock()

				// Check if all clients received the message
				if atomic.LoadInt64(&s.messageStats[msg.Id].SentCount) == int64(clientCount) {
					log.Printf("Message %d successfully sent to all %d clients", msg.Id, clientCount)
					s.statsMu.Lock()
					delete(s.messageStats, msg.Id)
					s.statsMu.Unlock()
				}
			}
		}()
	}
}

func (s *Server) monitorMessageStats() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		s.statsMu.RLock()
		for msgID, stats := range s.messageStats {
			sentCount := atomic.LoadInt64(&stats.SentCount)
			if sentCount < int64(stats.TotalClients) {
				log.Printf("Warning: Message %d sent to %d/%d clients", msgID, sentCount, stats.TotalClients)
			}
		}
		s.statsMu.RUnlock()
	}
}

func generateUniqueID() string {
	return fmt.Sprintf("client-%d", time.Now().UnixNano())
}
