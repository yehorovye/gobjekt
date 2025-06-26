package main

import (
	"flag"
	"fmt"
	"net"
	"strings"
)

var aof *Aof
var flagAOF = flag.Bool("aof", false, "enable data persistence")
var flagPort = flag.String("port", "6379", "port to listen to")

func main() {
	flag.Parse()

	if *flagAOF {
		a, err := NewAof("data.aof")
		if err != nil {
			panic(err)
		}
		aof = a
		defer aof.Close()
	}

	// create server instance
	srv, err := net.Listen("tcp", fmt.Sprintf(":%v", *flagPort))
	if err != nil {
		panic(err)
	}

	fmt.Println("started server at port", *flagPort)

	defer srv.Close()

	for {
		// listen
		conn, err := srv.Accept()
		if err != nil {
			continue
		}
		go handle(conn)
	}
}

func handle(c net.Conn) {
	defer c.Close()

	for {
		r := NewResp(c)
		v, err := r.Read()
		if err != nil {
			return
		}
		if v.typ != "array" || len(v.array) == 0 {
			continue
		}

		cmd := strings.ToUpper(v.array[0].bulk)
		args := v.array[1:]
		w := NewWriter(c)
		h, ok := Handlers[cmd]
		if !ok {
			w.Write(Value{typ: "error", str: "unknown command"})
			continue
		}

		if aof != nil && (cmd == "SET" || cmd == "HSET") {
			aof.Write(v)
		}

		w.Write(h(args))
	}
}
