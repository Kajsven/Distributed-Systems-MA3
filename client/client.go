package main
 
import (
	"context"
	"log"
	"time"
 
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
 
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
 
	resp, err := pb.NewTimeServiceClient(conn).GetCurrentTime(ctx, &pb.GetCurrentTimeRequest{})
	if err != nil {
		log.Fatalf("GetCurrentTime: %v", err)
	}
	
	log.Println("server time (local):  ", resp.Time.AsTime().Local())
}
 