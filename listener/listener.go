package listener

import (
	"github.com/PreetamJinka/sflow-go"

	"net"
)

type Listener struct {
	conn *net.UDPConn
}

func NewListener(address string) (*Listener, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	return &Listener{
		conn: conn,
	}, nil
}

func (l *Listener) Listen(outbound chan sflow.Datagram) {
	buf := make([]byte, 65535)

	for {
		n, _, err := l.conn.ReadFromUDP(buf)
		if err == nil {
			outbound <- sflow.Decode(buf[:n])
		}
	}
}
