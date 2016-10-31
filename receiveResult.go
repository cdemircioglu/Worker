package main

import (
	"log"
	"fmt"
	"net"
	"time"
	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

var chanNextLine chan string = make(chan string)
var chanRegister chan chan string = make(chan chan string)
var chanUnregister chan chan string = make(chan chan string)
var conns []chan string = make([]chan string, 0, 0)

func main() {
	
	// listen on all interfaces
	ln, _ := net.Listen("tcp", "localhost:8081")

	// accept connection on port
	//conn, _ := ln.Accept()

	go service()
	go data()
	
	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			continue
		}
		go handleConn(conn)
	}
		
}
func data() {
		
		for {
			var xmlmsg string 
			xmlmsg = getMessage()
			fmt.Println(xmlmsg)
			chanNextLine <- xmlmsg + "\n"
			//conn.Write([]byte(xmlmsg + "\n"))					
		}

}

func service() {
		for {
				select {
				case ch := <-chanRegister:
					conns = append(conns, ch)
					log.Println(len(conns), "active connection(s)")
				case ch := <-chanUnregister:
					found := false
					for i, el := range conns {
						if el == ch {
							found = true
							conns[i] = nil
							conns = append(conns[:i], conns[i+1:]...)
							break
						}
					}
					if !found {
						log.Println("Couldn't find channel to unregister!")
					} else {
						log.Println(len(conns), "active connection(s)")
					}
				case str := <-chanNextLine:
					for _, ch := range conns {
						select {
						case ch <- str: break
						default: break
						}
					}
				}
			}

}

func handleConn(conn net.Conn) {
	log.Println("Connection opened from", conn.RemoteAddr())
	ch := make(chan string, 1000)
	chanRegister <- ch
	go doWrites(conn, ch)

	buf := make([]byte, 1, 1)
	conn.Read(buf)
	chanUnregister <- ch
	conn.Close()
	close(ch)
	log.Println("Connection closed from ", conn.RemoteAddr())
}

func doWrites(conn net.Conn, ch chan string) {
	for {
		str, ok := <-ch
		if !ok {
			return
		}
		conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		_, err := conn.Write([]byte(str))
		if err != nil {
			conn.Close()
			return
		}
	}
}


func getMessage() string {
	var msg string	
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"workresult", // name
		false,   // durable
		false,   // delete when usused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)	
	
	go func() {
		for d := range msgs {
			//log.Printf("Received a message: %s", d.Body)
			msg = string(d.Body[:])			
			forever <- true
		}

	}()

	//log.Printf(" [*] Waiting for messages. To exit press CTRL+C -dc")
	<-forever
	return msg
}