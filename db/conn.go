// Copyright 2017, Square, Inc.

package db

import (
	"crypto/tls"
	"net"
	"sync"
	"time"

	"gopkg.in/mgo.v2"
)

// A Connector provides a database connection. It encapsulates logic about
// where and how to connect, like the TLS config, so that code using a
// Connector does not need to know this logic. Implementations can also vary
// wrt connection pooling.
type Connector interface {
	Connect() (*mgo.Session, error)
	Close()
}

type connectionPool struct {
	url         string
	timeout     int
	tlsConfig   *tls.Config
	credentials map[string]string
	// --
	session     *mgo.Session
	*sync.Mutex // guards methods
}

func NewConnector(url string, timeout int, tlsConfig *tls.Config, credentials map[string]string) Connector {
	return &connectionPool{
		url:         url,
		timeout:     timeout,
		tlsConfig:   tlsConfig,
		credentials: credentials,
		Mutex:       &sync.Mutex{},
	}
}

func (c *connectionPool) Connect() (*mgo.Session, error) {
	c.Lock()
	defer c.Unlock()

	// If a session already exists (and we can ping mongo), return a copy of it.
	if c.session != nil {
		if err := c.session.Ping(); err == nil {
			return c.session.Copy(), nil
		}
	}

	// Make custom dialer that can do TLS
	dialInfo, err := mgo.ParseURL(c.url)
	if err != nil {
		return nil, err
	}

	timeoutSec := time.Duration(c.timeout) * time.Second

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
	dialInfo.Timeout = timeoutSec

	// Connect
	s, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		return nil, err
	}

	c.session = s

	// Login
	if c.credentials["username"] != "" && c.credentials["source"] != "" && c.credentials["mechanism"] != "" {
		cred := &mgo.Credential{
			Username:  c.credentials["username"],
			Source:    c.credentials["source"],
			Mechanism: c.credentials["mechanism"],
		}
		err = s.Login(cred)
		if err != nil {
			return c.session, err
		}
	}

	return c.session, nil
}

func (c *connectionPool) Close() {
	c.Lock()
	defer c.Unlock()

	if c.session == nil {
		return
	}
	c.session.Close()
	c.session = nil
}
