package dashboard

import (
    "encoding/json"
    "net/http"
    "time"

    "github.com/rs/zerolog/log"
    "golang.org/x/net/websocket"
)

type Client struct {
    conn *websocket.Conn
    send chan []byte
}

type Event struct {
    Type      string    `json:"type"`
    JobID     string    `json:"job_id"`
    Status    string    `json:"status"`
    WorkerID  string    `json:"worker_id,omitempty"`
    Timestamp time.Time `json:"timestamp"`
}

type Hub struct {
    clients   map[*Client]bool
    broadcast chan []byte
    register  chan *Client
    unregister chan *Client
}

func NewHub() *Hub {
    return &Hub{
        clients:    make(map[*Client]bool),
        broadcast:  make(chan []byte, 256),
        register:   make(chan *Client),
        unregister: make(chan *Client),
    }
}
func (h *Hub) Run() {
    for {
        select {
        case client := <-h.register:
            h.clients[client] = true
            log.Info().Msg("dashboard client connected")

        case client := <-h.unregister:
            if _, ok := h.clients[client]; ok {
                delete(h.clients, client)
                close(client.send)
                log.Info().Msg("dashboard client disconnected")
            }

        case message := <-h.broadcast:
            for client := range h.clients {
                select {
                case client.send <- message:
                default:
                    delete(h.clients, client)
                    close(client.send)
                }
            }
        }
    }
}

func (h *Hub) Publish(event Event) {
    data, err := json.Marshal(event)
    if err != nil {
        log.Error().Err(err).Msg("failed to marshal event")
        return
    }

    select {
    case h.broadcast <- data:
    default:
        log.Warn().Msg("broadcast channel full, dropping event")
    }
}

func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
    ws := websocket.Server{
        Handler: func(conn *websocket.Conn) {
            client := &Client{
                conn: conn,
                send: make(chan []byte, 64),
            }
            h.register <- client
            defer func() { h.unregister <- client }()

            for msg := range client.send {
                if _, err := conn.Write(msg); err != nil {
                    log.Error().Err(err).Msg("websocket write error")
                    return
                }
            }
        },
    }
    ws.ServeHTTP(w, r)
}

