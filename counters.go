package main

import (
	"github.com/PreetamJinka/flow-collector/store"
	"github.com/PreetamJinka/sflow-go"

	"fmt"
	"net"
)

type counters struct {
	states map[string]*counterState
}

func NewCounters() *counters {
	return &counters{
		states: make(map[string]*counterState),
	}
}

type counterState struct {
	lastUpdated uint
	inOctets    uint
	outOctets   uint
	inPackets   uint
	outPackets  uint
}

func generateKey(device net.IP, iface uint32) string {
	return device.String() + "-" + fmt.Sprint(iface)
}

func (c *counters) Update(dgrams chan sflow.Datagram, outbound chan store.CounterStats) {
	for dgram := range dgrams {
		c.update(dgram, outbound)
	}
}

func (c *counters) update(dgram sflow.Datagram, outbound chan store.CounterStats) {
	for _, sample := range dgram.Samples {
		switch sample.SampleType() {
		case sflow.TypeCounterSample:
			s := sample.(sflow.CounterSample)
			for _, record := range s.Records {
				if record.RecordType() == sflow.TypeGenericIfaceCounter {
					r := record.(sflow.GenericIfaceCounters)
					lastUpdated := uint(dgram.Header.SwitchUptime)
					inOctets := uint(r.InOctets)
					outOctets := uint(r.OutOctets)
					inPackets := uint(r.InUcastPkts)
					outPackets := uint(r.OutUcastPkts)

					key := generateKey(dgram.Header.IpAddress, r.Index)

					state, present := c.states[key]
					if !present {
						state = &counterState{
							lastUpdated: lastUpdated,
							inOctets:    inOctets,
							outOctets:   outOctets,
							inPackets:   inPackets,
							outPackets:  outPackets,
						}
						c.states[key] = state
						continue
					}

					timeDiffSeconds := (float32(lastUpdated) -
						float32(state.lastUpdated)) /
						1000.0

					if timeDiffSeconds <= 0 {
						c.states[key] = &counterState{
							lastUpdated: lastUpdated,
							inOctets:    inOctets,
							outOctets:   outOctets,
							inPackets:   inPackets,
							outPackets:  outPackets,
						}

						continue
					}

					outbound <- store.CounterStats{
						Interface:  key,
						InOctets:   float32(inOctets-state.inOctets) / timeDiffSeconds,
						InPackets:  float32(inPackets-state.inPackets) / timeDiffSeconds,
						OutOctets:  float32(outOctets-state.outOctets) / timeDiffSeconds,
						OutPackets: float32(outPackets-state.outPackets) / timeDiffSeconds,
					}

					c.states[key] = &counterState{
						lastUpdated: lastUpdated,
						inOctets:    inOctets,
						outOctets:   outOctets,
						inPackets:   inPackets,
						outPackets:  outPackets,
					}
				}
			}
		}
	}
}
