package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "Distributed-Systems-MA3/grpc"
)
 
func main() {
	conn, err := grpc.NewClient("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()
 
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	
	message := pb.PostCurrentMessage{
		Time:    timestamppb.New(time.Now()),
		Message: "Hello",
	}

	resp, err := pb.NewChatServiceClient(conn).PostMessage(ctx, &message)
	if err != nil {
		log.Fatalf("GetCurrentTime: %v", err)
	}
	
	log.Println("Success Status:  ", resp.Success)
}
 