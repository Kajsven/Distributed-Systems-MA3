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

type clockReq struct {
	received int64
	reply    chan int64
}

var clockCh = make(chan clockReq)
var myId int32 = -1
var scanner = bufio.NewScanner(os.Stdin)
var stream pb.ChatService_ConnectClient
var wg = sync.WaitGroup{}
var lamportClock int64 = 0

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

	wg.Add(3)
	go clockKeeper()
	go broadcastMessages()
	go sendMessages(conn, ctx)
	wg.Wait()
}

func clockKeeper() {
	var lamportClock int64
	for req := range clockCh {
		lamportClock = max(lamportClock, req.received) + 1
		req.reply <- lamportClock
	}
}

func getClock(received int64) int64 {
	reply := make(chan int64)
	clockCh <- clockReq{received: received, reply: reply}
	return <-reply
}

func sendMessages(conn *grpc.ClientConn, ctx context.Context) {
	defer wg.Done()
	for {
		scanner.Scan()
		text := scanner.Text()
		if len(text) == 0 || len(text) > 128 {
			log.Println("Text must be between 0 and 128 characters")
			continue
		}
		msg := pb.PostCurrentMessage{
			Time:     getClock(0),
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
		fmt.Printf("%s, %s, LC(%d): %s\n", resp.Username, time.Now().Local().Format("15:04"), getClock(resp.Time)-1, resp.Message)
	}
}
