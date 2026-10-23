# distributed-systems-ma3

Source code for the mandatory activity 3 in Distributed Systems , BSDISYS1KU

## how to use
### Start the server
in a terminal write
```bash
go run ./server
``` 
then logs should start showing

### Start one or more clients 
in a terminal 
```bash
go run ./client
```
then type in a username and hit `Enter`

when the server responds you can start sending messages

Every following message will now be send to every other client through the server.
