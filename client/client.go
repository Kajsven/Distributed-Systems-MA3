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

	pb "Distributed-Systems-MA3/grpc"
)

var myId int32 = -1
var scanner = bufio.NewScanner(os.Stdin)
var stream pb.ChatService_ConnectClient
var wg = sync.WaitGroup{}
var lamportClock int64 = 0
var mu sync.Mutex

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

	wg.Add(2)
	go broadcastMessages()
	go sendMessages(conn, ctx)
	wg.Wait()
}

func sendMessages(conn *grpc.ClientConn, ctx context.Context) {
	defer wg.Done()
	for {
		mu.Lock()
		lamportClock++
		mu.Unlock()
		scanner.Scan()
		msg := pb.PostCurrentMessage{
			Time:     lamportClock,
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
		mu.Lock()
		lamportClock = max(lamportClock, resp.Time) + 1
		mu.Unlock()
		fmt.Printf("%s, %s, LC(%d): %s\n", resp.Username, time.Now().Local().Format("15:04"), lamportClock, resp.Message)
	}
}
