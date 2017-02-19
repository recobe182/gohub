package gohub

import (
	"qpid.apache.org/electron"
	"strconv"
	"net"
	"crypto/tls"
	"math/rand"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"strings"
	"time"
)

const (
	network string = `tcp`
	amqpsPort string = `5671`
	receivePattern string = `%v/ConsumerGroups/%v/Partitions/%v`
	hostPattern string = `%v.servicebus.windows.net`
	selectorFilter string = `apache.org:selector-filter:string`
	offsetFilter string = `amqp.annotation.x-opt-offset > %v`
)

// EVHConnection is a connection interface which connect to Azure Event Hub.
type EVHConnection interface {
	// CreateSender creates a new sender on the DefaultSession.
	CreateSender() (EVHSender, error)

	// CreateReceiver creates a new receiver on the DefaultSession.
	CreateReceiver(p int, ss StorageSetting, opts ...ReceiverOption) (EVHReceiver, error)

	// Close the connection.
	Close() error
}

// EVHConnection implementation.
type evhConnection struct {
	host 	string
	hub 	string
	sasN 	string
	sasK 	string

	conn	electron.Connection
	senders	[]electron.Sender
	recvs	[]electron.Receiver
}

// New creates a new Azure Event Hub connection instance.
// New also try to connect to Azure Event Hub.
// The parameters are service bus namespace, event hub name, SAS key name and SAS key.
func New(ns, hub, sasN, sasK string) (*evhConnection, error) {
	conn := &evhConnection{
		host: fmt.Sprintf(hostPattern, ns),
		hub: hub,
		sasN: sasN,
		sasK: sasK,
	}
	if err := conn.connect(); err != nil {
		return nil, err
	} else {
		log.WithFields(log.Fields{"Host": conn.host, "Hub": conn.hub, }).Debug("Connected")
		return conn, nil
	}
}

/// NewWithConnectionString a new Azure Event Hub connection instance by using connection string.
func NewWithConnectionString(connStr string) (*evhConnection, error) {
	var ns, hub, sasN, sasK string
	m := make(map[string]string)
	l := []string{"Endpoint=", "SharedAccessKeyName=", "SharedAccessKey=", "EntityPath="}
	for _, v := range strings.Split(connStr, ";") {
		for _, pn := range l {
			index := strings.Index(v, pn)
			if index >= 0 {
				m[strings.TrimSuffix(pn, "=")] = v[0 + len(pn):]
				break
			}
		}
	}
	ns = strings.Split(strings.Split(m[`Endpoint`], ".")[0], "//")[1]
	hub = m[`EntityPath`]
	sasN = m[`SharedAccessKeyName`]
	sasK = m[`SharedAccessKey`]
	return New(ns, hub, sasN, sasK)
}

func (c*evhConnection) connect() error {
	container := electron.NewContainer(strconv.Itoa(rand.Int()))
	conn, err := net.Dial(network, c.host + `:` + amqpsPort)
	if err != nil {
		return err
	}
	tlsConf := new(tls.Config)
	tlsConf.ServerName = c.host
	tlsConn := tls.Client(conn, tlsConf)
	if err := tlsConn.Handshake(); err != nil {
		conn.Close()
		return err
	}
	conn = tlsConn
	eConn, err := container.Connection(
		conn,
		electron.SASLAllowInsecure(true),
		electron.VirtualHost(c.host),
		electron.User(c.sasN),
		electron.Password([]byte(c.sasK)),
		electron.Heartbeat(60000 * time.Millisecond),
	)
	if err != nil {
		return err
	}
	c.conn = eConn
	c.senders = []electron.Sender{}
	c.recvs = []electron.Receiver{}
	return nil
}

func (c*evhConnection) CreateSender() (EVHSender, error) {
	s, err := c.conn.Sender(
		electron.LinkName(strconv.Itoa(rand.Int())),
		electron.Target(c.hub),
		electron.Source(strconv.Itoa(rand.Int())),
		electron.AtLeastOnce(),
	)
	if err != nil {
		return nil, err
	}
	c.senders = append(c.senders, s)
	return &evhSender{sender: s}, nil
}

func (c*evhConnection) CreateReceiver(pid int, opts ...ReceiverOption) (EVHReceiver, error) {
	rs := receiverSetting{
		partitionId: strconv.Itoa(pid),
		consumerGroup: defaultConsumerGroup,
		prefetchCount: defaultPrefetchCount,
	}
	for _, set := range opts {
		set(&rs)
	}
	return c.newReceiver(rs)
}

func (c*evhConnection) newReceiver(rs receiverSetting) (EVHReceiver, error) {
	m := make(map[string]string)
	var as storage
	if rs.mode == FromLastOffset {
		as = newAzureStorage(rs.storageSetting)
		as.CreateStorage(c.hub, rs.consumerGroup, rs.partitionId)
		cp, err := as.GetCheckpoint(c.hub, rs.consumerGroup, rs.partitionId)
		if err != nil {
			return nil, err
		}
		m[selectorFilter] = fmt.Sprintf(offsetFilter, cp.offset)
	} else if rs.mode == FromNow {

	} else {
		panic(fmt.Sprintf("Invalid receiver mode. %v", rs.mode))
	}

	r, err := c.conn.Receiver(
		electron.Source(fmt.Sprintf(receivePattern, c.hub, rs.consumerGroup, rs.partitionId)),
		electron.Prefetch(true),
		electron.Capacity(rs.prefetchCount),
		electron.Filter(m),
	)
	if err != nil {
		return nil, err
	}

	c.recvs = append(c.recvs, r)
	return &evhReceiver{
		receiver: r,
		hub: c.hub,
		consumerGroup: rs.consumerGroup,
		partitionId: rs.partitionId,
		p: newPartitionContext(c.hub, rs.consumerGroup, rs.partitionId, as),
	}, nil
}

func (c*evhConnection) Close() error {
	var err error
	for _, sender := range c.senders {
		sender.Close(err)
		if err != nil {
			return err
		}
	}
	for _, recv := range c.recvs {
		recv.Close(err)
		if err != nil {
			return err
		}
	}
	c.conn.Close(err)
	return err
}