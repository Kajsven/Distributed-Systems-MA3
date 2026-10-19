package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"

	pb "Distributed-Systems-MA3/grpc"
)

type chatServer struct {
	pb.UnimplementedChatServiceServer
	idCount  int32
	usersMap map[int32]client
}

func newChatServer() *chatServer {
	return &chatServer{idCount: 0, usersMap: make(map[int32]client)}
}

type client struct {
	username string
	stream   pb.ChatService_ConnectServer
}

func (s *chatServer) PostMessage(ctx context.Context, msg *pb.PostCurrentMessage) (*pb.PostCurrentMessageResponse, error) {
	log.Printf("%s", msg.Message)

	for id, user := range s.usersMap {
		if id == msg.ClientId {
			continue
		}
		err := user.stream.Send(&pb.BroadCast{
			Time:     msg.Time,
			Message:  msg.Message,
			Username: s.usersMap[msg.ClientId].username,
		})
		if err != nil {
			log.Printf("Error sending message to user %s: %v", user.username, err)
		}
	}

	return &pb.PostCurrentMessageResponse{
		Success: true,
	}, nil
}

func (s *chatServer) Connect(req *pb.ConnectRequest, stream pb.ChatService_ConnectServer) error {
	s.usersMap[req.ClientId] = client{username: req.Username, stream: stream}
	log.Printf("%s", req.Username)

	<-stream.Context().Done()
	delete(s.usersMap, req.ClientId)

	return nil
}

func (s *chatServer) GetId(ctx context.Context, req *pb.RequestId) (*pb.ResponseId, error) {
	s.idCount++
	log.Printf("Generated new ID: %d", s.idCount)
	return &pb.ResponseId{ClientId: s.idCount}, nil
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
