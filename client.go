package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type Client struct {
	id   string
	hub  *Hub
	conn *websocket.Conn
	send chan []byte
}

const (
	writeWait = 10 * time.Second
	// Send pings to peer with this period. Must be less than pongWait
	pingPeriod = (pongWait * 9) / 10
	// Time allowed to read the next pong message from the peer
	pongWait = 60 * time.Second
	// Maximum message size allowed from peer
	maxMessageSize = 512
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func serveWs(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)

	if err != nil {
		log.Println(err)
		return
	}

	id := uuid.New()

	client := &Client{
		id:   id.String(),
		hub:  hub,
		conn: conn,
		send: make(chan []byte),
	}

	client.hub.register <- client
	go client.writePump()
	go client.readPump()

}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)

	defer func() {
		c.conn.Close()
		// c.hub.unregister <- c
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}

			w.Write(msg)

			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write(msg)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))

			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}

	}

}

func (c *Client) readPump() {
	defer func() {
		c.conn.Close()
		c.hub.unregister <- c
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(appData string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, text, err := c.conn.ReadMessage()
		log.Printf("value %v", string(text))

		if err != nil {
			log.Printf("error: %v", err)
			continue
		}

		msg := &WSMessage{}
		reader := bytes.NewReader(text)
		decoder := json.NewDecoder(reader)
		err = decoder.Decode(msg)

		if err != nil {
			log.Printf("error: %v", err)
		}

		c.hub.broadcast <- &Message{ClientID: c.id, Text: msg.Text}
	}
}
