package main

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "Distributed-Systems-MA3/grpc"
)

type chatServer struct {
	pb.UnimplementedChatServiceServer
	idCount  int32
	usersMap map[int32]client
	mu 	 sync.Mutex
}

func newChatServer() *chatServer {
	return &chatServer{idCount: 0, usersMap: make(map[int32]client)}
}

type client struct {
	username string
	stream   pb.ChatService_ConnectServer
}
func (s *chatServer) GetId(ctx context.Context, req *pb.RequestId) (*pb.ResponseId, error) {
	s.idCount++
	log.Printf("Generated new ID: %d", s.idCount)
	return &pb.ResponseId{ClientId: s.idCount}, nil
}

func (s *chatServer) Connect(req *pb.ConnectRequest, stream pb.ChatService_ConnectServer) error {
	s.mu.Lock()
	s.usersMap[req.ClientId] = client{username: req.Username, stream: stream}
	s.mu.Unlock()
	log.Printf("user %s joined", req.Username)

	broadcast(&pb.BroadCast{
		Time:     timestamppb.New(time.Now()),
		Message:  req.Username + " joined the chat!",
		Username: "Server",
	}, req.ClientId, s)

	<-stream.Context().Done()
	s.mu.Lock()
	delete(s.usersMap, req.ClientId)
	s.mu.Unlock()

	broadcast(&pb.BroadCast{
		Time:     timestamppb.New(time.Now()),
		Message:  req.Username + " Left the chat!",
		Username: "Server",
	}, req.ClientId, s)

	return nil
}

func (s *chatServer) PostMessage(ctx context.Context, msg *pb.PostCurrentMessage) (*pb.PostCurrentMessageResponse, error) {
	log.Printf("%s", msg.Message)

	broadcast(&pb.BroadCast{
		Time:     msg.Time,
		Message:  msg.Message,
		Username: s.usersMap[msg.ClientId].username,
	}, msg.ClientId, s)

	return &pb.PostCurrentMessageResponse{
		Success: true,
	}, nil
}

func broadcast(msg *pb.BroadCast, senderId int32, s *chatServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
