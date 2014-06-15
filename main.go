package main

import (
	"github.com/PreetamJinka/flow-collector/listener"
	"github.com/PreetamJinka/flow-collector/store"
	"github.com/PreetamJinka/sflow-go"

	"flag"
	"log"
)

func main() {
	listenAddr := flag.String("listen", ":6343", "Address to listen for UDP datagrams")
	dataDir := flag.String("data", "/opt/flowdata", "Directory to store flow data")

	flag.Parse()

	listener, err := listener.NewListener(*listenAddr)
	if err != nil {
		log.Fatalf("Unable to start listener:", err)
	}

	datagrams := make(chan sflow.Datagram)
	counterDerivatives := make(chan store.CounterStats)

	counters := NewCounters()
	counterStore := store.NewStore(*dataDir)

	go listener.Listen(datagrams)
	go counters.Update(datagrams, counterDerivatives)
	go counterStore.StoreCounters(counterDerivatives)

	startHttp(counterStore)
}
