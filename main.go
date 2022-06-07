package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

var options struct {
	listenAddress string
	targetAddress string
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage:  %s [options]\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.StringVar(&options.listenAddress, "listen", "127.0.0.1:5432", "Listen address")
	flag.StringVar(&options.targetAddress, "target", "http://127.0.0.1:8080", "Trino cluster address")
	flag.Parse()

	err := RunServer(nil, options.listenAddress, options.targetAddress)
	if err != nil {
		log.Fatal(err)
	}
}
