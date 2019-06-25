// Copyright 2017-2019, Square, Inc.

package db

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/globalsign/mgo"
)

// A Connector provides a database connection. It encapsulates logic about
// where and how to connect, like the TLS config, so that code using a
// Connector does not need to know this logic. Implementations can also vary
// wrt connection pooling.
type Connector interface {
	Init() error
	Connect() (*mgo.Session, error)
	Close()
}

type connectionPool struct {
	url         string
	timeout     int
	tlsConfig   *tls.Config
	credentials map[string]string
	// --
	session *mgo.Session
}

func NewConnector(url string, timeout int, tlsConfig *tls.Config, credentials map[string]string) Connector {
	return &connectionPool{
		url:         url,
		timeout:     timeout,
		tlsConfig:   tlsConfig,
		credentials: credentials,
	}
}

func (c *connectionPool) Init() error {
	// @todo: changed to ms or duration
	timeoutSec := time.Duration(c.timeout) * time.Second

	// Make custom dialer that can do TLS
	dialInfo, err := mgo.ParseURL(c.url)
	if err != nil {
		return err
	}
	dialInfo.Username = c.credentials["username"]
	dialInfo.Password = c.credentials["password"]
	dialInfo.Source = c.credentials["source"]
	dialInfo.Mechanism = c.credentials["mechanism"]
	dialInfo.Timeout = timeoutSec
	dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
		if c.tlsConfig != nil {
			dialer := &net.Dialer{
				Timeout: timeoutSec,
			}
			conn, err := tls.DialWithDialer(dialer, "tcp", addr.String(), c.tlsConfig)
			if err != nil {
				return nil, err
			}
			return conn, nil
		} else {
			conn, err := net.DialTimeout("tcp", addr.String(), timeoutSec)
			if err != nil {
				return nil, err
			}
			return conn, nil
		}
	}

	// Connect
	s, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		return err
	}
	if err := s.Ping(); err != nil {
		s.Close()
		return err
	}

	c.session = s
	return nil
}

func (c *connectionPool) Connect() (*mgo.Session, error) {
	return c.session.Copy(), nil
}

func (c *connectionPool) Close() {
	if c.session == nil {
		return
	}
	c.session.Close()
	c.session = nil
}
