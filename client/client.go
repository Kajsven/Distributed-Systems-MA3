package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "Distributed-Systems-MA3/grpc"
)

var myId int32 = -1
var scanner = bufio.NewScanner(os.Stdin)
var stream pb.ChatService_ConnectClient
var wg = sync.WaitGroup{}

func main() {
	conn, err := grpc.NewClient("127.0.0.1:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	idResp, err := pb.NewChatServiceClient(conn).GetId(ctx, &pb.RequestId{})
	myId = idResp.ClientId
	if err != nil {
		log.Fatalf("dial: %v", err)
	}

	scanner.Scan()
	postConnection := pb.ConnectRequest{
		Username: scanner.Text(),
		ClientId: myId,
	}

	conResp, err := pb.NewChatServiceClient(conn).Connect(ctx, &postConnection)
	stream = conResp

	message := pb.PostCurrentMessage{
		Time:     timestamppb.New(time.Now()),
		Message:  "Hello",
		ClientId: myId,
	}

	resp, err := pb.NewChatServiceClient(conn).PostMessage(ctx, &message)
	if err != nil {
		log.Fatalf("GetCurrentTime: %v", err)
	}

	log.Println("Success Status:  ", resp.Success)

	wg.Add(1)
	go broadcastMessages()
	wg.Wait()
}

func broadcastMessages() {
	defer wg.Done()
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatalf("can not receive %v", err)
		}
		log.Printf("%s, %s: %s", resp.Time.AsTime(), resp.Username, resp.Message)
	}
}
