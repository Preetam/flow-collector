package main

import (
	"github.com/PreetamJinka/flow-collector/store"

	"net/http"
	"strconv"
	"strings"
)

func startHttp(store *store.Store) {
	http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		queryVals := req.URL.Query()

		from, _ := strconv.ParseUint(queryVals.Get("from"), 10, 64)
		to, _ := strconv.ParseUint(queryVals.Get("to"), 10, 64)
		w.Write(
			store.FetchCounters(
				queryVals.Get("iface"),
				strings.Split(queryVals.Get("metrics"), ","),
				from,
				to),
		)
	})

	http.ListenAndServe(":8000", nil)
}
