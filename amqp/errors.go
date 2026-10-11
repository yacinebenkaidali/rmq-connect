package amqp

import "errors"

var errClientClosed = errors.New("client already closed its connections !")
