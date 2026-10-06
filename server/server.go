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
}

func (s *chatServer) PostMessage(ctx context.Context, msg *pb.PostCurrentMessage) (*pb.PostCurrentMessageResponse, error) {
	log.Printf("%s", msg.Message)
	return &pb.PostCurrentMessageResponse{
		Success: true,
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterChatServiceServer(srv, &chatServer{})

	log.Println("TimeService listening on :50051")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}