# gohub
Azure Event Hub client for Go.

_This repository is under active development._
_Not ready for PRODUCTION._

## Installation
```
go get github.com/recobe182/gohub
```

## Documentation
See [Go Doc](https://godoc.org/github.com/recobe182/gohub).

## Getting started
### Send
```
c, err := gohub.New(`a_namespace`, `a_hub`, `a_key_name`, `a_key`)
if err != nil {
	panic(err)
}
s, err := c.CreateSender()
if err != nil {
	panic(err)
}
s.SendSync("55")
```
### Receive
```
c, err := gohub.New(`a_namespace`, `a_hub`, `a_key_name`, `a_key`)
if err != nil {
	panic(err)
}
ss := gohub.StorageSetting(`an_account`, `a_key`)
r, err := c.CreateReceiver(0, ss, gohub.ConsumerGroup(`$Default`), gohub.PrefetchCount(1), gohub.CheckPointAfter(5))
if err != nil {
    panic(err)
}
o := make(chan gohub.ReceiveMessage)
go r.Receive(o)
for outcome := range o {
    if outcome.Error != nil {
        close(o)
    }
    fmt.Println("Partition 0: " + outcome.Msg)
}
	
```

## Running application
Due to the fact that this library use qpid.apache.org/electron as a AMQP1.0 library, you have to install proton-c on your development environment. 
Unfortunately proton-c is not available for Mac OSX, please use [pre-built docker image](https://hub.docker.com/r/recobe/ubuntu/tags/) as a workaround to build and run an application.

If you want to build an application
```
docker pull recobe/ubuntu:16.10-proton-go
docker run --rm -it -v "$GOPATH":/go recobe/ubuntu:16.10-proton-go bash
```
then map your $GOPATH to /go.

If you just want to run your application inside docker container, just put a binary file inside this container.
```
docker pull recobe/ubuntu:16.10-proton
```

## Development plan
- [ ] Sending to a selected partition.
- [ ] Automatic reconnect.
