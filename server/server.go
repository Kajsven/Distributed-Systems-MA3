package main

import (
	"context"
	"log"
	"net"
	"time"
	
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	
	pb "Distributed-Systems-MA3/grpc"
)

type timeServer struct {
	pb.UnimplementedTimeServiceServer
}

func (s *timeServer) GetCurrentTime(ctx context.Context, _ *pb.GetCurrentTimeRequest) (*pb.GetCurrentTimeResponse, error) {
	now := time.Now()
	return &pb.GetCurrentTimeResponse{
		Time:    timestamppb.New(now),
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterTimeServiceServer(srv, &timeServer{})

	log.Println("TimeService listening on :50051")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}