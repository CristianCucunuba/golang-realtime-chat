package main

import (
	"bytes"
	"html/template"
	"log"
)

type Message struct {
	ClientID string
	Text     string
}

type WSMessage struct {
	Text    string      `json:"text"`
	Headers interface{} `json:"HEADERS"`
}

type Hub struct {
	clients    map[*Client]bool
	messages   []*Message
	broadcast  chan *Message
	register   chan *Client
	unregister chan *Client
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan *Message),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true

			log.Printf("client registered %s", client.id)

			for _, msg := range h.messages {
				client.send <- getMessageTemplate(msg)
			}

		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				log.Printf("Client unregistered %s", client.id)

				close(client.send)
				delete(h.clients, client)
			}
		case msg := <-h.broadcast:
			h.messages = append(h.messages, msg)

			for client := range h.clients {
				select {
				case client.send <- getMessageTemplate(msg):
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}

		}

	}
}

func getMessageTemplate(msg *Message) []byte {
	tmpl, err := template.ParseFiles("templates/message.html")

	if err != nil {
		log.Fatalf("tempalte parsing error %s", err)
	}

	var renderedMessage bytes.Buffer
	err = tmpl.Execute(&renderedMessage, msg)
	if err != nil {
		log.Fatalf("tempalte parsing error %s", err)
	}
	return renderedMessage.Bytes()

}
