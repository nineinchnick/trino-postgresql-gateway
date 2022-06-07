package main

import (
	"log"
	"net"
)

func RunServer(ready chan struct{}, listenAddress string, targetAddress string) error {
	/*
		// TODO use this as the parent x
		ctx, stop := context.WithCancel(context.Background())
		defer stop()

		appSignal := make(chan os.Signal, 3)
		signal.Notify(appSignal, os.terrupt)

		go func() {
			select {
			case <-appSignal:
				stop()
			}

	*/

	ln, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return err
	}
	log.Println("Listening on", ln.Addr())
	if ready != nil {
		close(ready)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		log.Println("Accepted connection from", conn.RemoteAddr())

		b, err := NewBackend(conn, targetAddress)
		if err != nil {
			return err
		}
		go func() {
			err := b.Run()
			if err != nil {
				log.Println(err)
			}
			log.Println("Closed connection from", conn.RemoteAddr())
		}()
	}
}
