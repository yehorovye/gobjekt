package main

import (
	"fmt"
	"net"
	"strings"
)

func main() {
	fmt.Println("starting server...")

	// create server instance
	server, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("started server in port 6379")

	// listen
	conn, err := server.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}

	// ok ig
	defer conn.Close()

	for {
		resp := NewResp(conn)
		value, err := resp.Read()
		if err != nil {
			fmt.Println(err)
			return
		}

		if value.typ != "array" {
			fmt.Println("expected array")
			continue
		}

		if len(value.array) == 0 {
			fmt.Println("expected non-empty array")
			continue
		}

		command := strings.ToUpper(value.array[0].bulk)
		args := value.array[1:]

		writer := NewWriter(conn)

		handler, ok := Handlers[command]
		if !ok {
			fmt.Println("invalid/unknown command: ", command)
			writer.Write(Value{typ: "string", str: ""})
			continue
		}

		result := handler(args)
		writer.Write(result)
	}
}
