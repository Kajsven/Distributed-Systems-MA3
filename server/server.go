package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"

	pb "Distributed-Systems-MA3/grpc"
)

type chatServer struct {
	pb.UnimplementedChatServiceServer
	idCount      int32
	usersMap     map[int32]client
	mu           sync.Mutex
	lamportClock int64
}

func newChatServer() *chatServer {
	return &chatServer{idCount: 0, usersMap: make(map[int32]client)}
}

type client struct {
	username string
	stream   pb.ChatService_ConnectServer
}

func (s *chatServer) GetId(ctx context.Context, req *pb.RequestId) (*pb.ResponseId, error) {
	s.mu.Lock()
	s.idCount++
	s.lamportClock++
	s.mu.Unlock()
	log.Printf("Server, LC(%d), Client getId event, Generated new ID: %d", s.lamportClock, s.idCount)
	return &pb.ResponseId{ClientId: s.idCount}, nil
}

func (s *chatServer) Connect(req *pb.ConnectRequest, stream pb.ChatService_ConnectServer) error {
	s.mu.Lock()
	s.usersMap[req.ClientId] = client{username: req.Username, stream: stream}
	s.lamportClock++
	log.Printf("Server, LC(%d), Client connection event, User %s joined Chit Chat", s.lamportClock, req.Username)

	broadcast(&pb.BroadCast{
		Time:     s.lamportClock,
		Message:  "Participant " + req.Username + " joined Chit Chat at logical time " + fmt.Sprintf("%d", s.lamportClock),
		Username: "Server",
	}, -1, s)
	s.mu.Unlock()

	<-stream.Context().Done()

	s.mu.Lock()
	delete(s.usersMap, req.ClientId)

	s.lamportClock++
	log.Printf("Server, LC(%d), Client disconnection event, User %s left Chit Chat", s.lamportClock, req.Username)
	broadcast(&pb.BroadCast{
		Time:     s.lamportClock,
		Message:  "Participant " + req.Username + " left Chit Chat at logical time " + fmt.Sprintf("%d", s.lamportClock),
		Username: "Server",
	}, -1, s)
	s.mu.Unlock()
	return nil
}

func (s *chatServer) PostMessage(ctx context.Context, msg *pb.PostCurrentMessage) (*pb.PostCurrentMessageResponse, error) {
	s.mu.Lock()
	s.lamportClock = max(s.lamportClock, msg.Time) + 1
	log.Printf("Server, LC(%d), Client post message event, User %s: %s", s.lamportClock, s.usersMap[msg.ClientId].username, msg.Message)

	broadcast(&pb.BroadCast{
		Time:     s.lamportClock,
		Message:  msg.Message,
		Username: s.usersMap[msg.ClientId].username,
	}, msg.ClientId, s)
	s.mu.Unlock()

	return &pb.PostCurrentMessageResponse{
		Success: true,
	}, nil
}

func broadcast(msg *pb.BroadCast, senderId int32, s *chatServer) {
	for id, user := range s.usersMap {
		if id == senderId {
			continue
		}
		err := user.stream.Send(msg)
		if err != nil {
			log.Printf("Error sending join message to user %s %v", user.username, err)
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterChatServiceServer(srv, newChatServer())

	log.Println("TimeService listening on :50051")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
