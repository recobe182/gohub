# gohub
Azure Event Hub client for Go.

_This repository is under active development._

## Installation
```
go get github.com/recobe182/gohub
```

## Documentation
See [Go Doc](https://godoc.org/qpid.apache.org/amqp).

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
r, err := c.CreateReceiver(`$Default`, 0)
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

## Development plan
- [ ] Integration with Azure blob storage.
- [ ] Receiving message with partition offset.
- [ ] Partition checkpoint.
- [ ] Sending to a selected partition.
- [ ] Automatic reconnect.
