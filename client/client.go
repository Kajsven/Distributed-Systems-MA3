package main

import (
	"bufio"
	"context"
	"fmt"
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

	// message := pb.PostCurrentMessage{
	// 	Time:     timestamppb.New(time.Now()),
	// 	Message:  "Joined the chat!",
	// 	ClientId: myId,
	// }

	// resp, err := pb.NewChatServiceClient(conn).PostMessage(ctx, &message)
	// if err != nil {
	// 	log.Fatalf("GetCurrentTime: %v", err)
	// }

	//log.Println("Success Status:  ", resp.Success)

	wg.Add(2)
	go broadcastMessages()
	go sendMessages(conn, ctx)
	wg.Wait()
}

func sendMessages(conn *grpc.ClientConn, ctx context.Context) {
	defer wg.Done()
	for {
		scanner.Scan()
		msg := pb.PostCurrentMessage{
			Time:     timestamppb.New(time.Now()),
			Message:  scanner.Text(),
			ClientId: myId,
		}
		_, err := pb.NewChatServiceClient(conn).PostMessage(ctx, &msg)
		if err != nil {
			log.Fatalf("GetCurrentTime: %v", err)
		}
	}
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
		fmt.Printf("%s, %s: %s\n", resp.Username, resp.Time.AsTime().Local().Format("15:04"), resp.Message)
	}
}
