package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/buger/jsonparser"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
)

type Client struct {
	DB *sql.DB
}

type MyError struct {
	msg string
}

func (error *MyError) Error() string {
	return error.msg
}

const delete = `DROP TABLE IF EXISTS testsdb.ticks`
const tableCreationQuery = `CREATE TABLE testsdb.ticks (
	` + "`timestamp`" + ` int(11) NOT NULL,
	symbol varchar(45) PRIMARY KEY,
	bid FLOAT NOT NULL,
	ask FLOAT NOT NULL
)`
const insert1 = `insert into testsdb.ticks values (0, 'ETH-BTC', 0.0, 0.0)`
const insert2 = `insert into testsdb.ticks values (0, 'BTC-USD', 0.0, 0.0)`
const insert3 = `insert into testsdb.ticks values (0, 'BTC-EUR', 0.0, 0.0)`

func (c *Client) Initialize(user, password, dbname string) {
	connectionString := fmt.Sprintf("%s:%s@/%s", user, password, dbname)

	var err error
	c.DB, err = sql.Open("mysql", connectionString)
	if err != nil {
		log.Fatal(err)
	}

	_, err = c.DB.Exec(delete)
	if err != nil {
		log.Fatal(err)
	}

	_, err = c.DB.Exec(tableCreationQuery)
	if err != nil {
		log.Fatal(err)
	}

	_, err = c.DB.Exec(insert1)
	if err != nil {
		log.Fatal(err)
	}

	_, err = c.DB.Exec(insert2)
	if err != nil {
		log.Fatal(err)
	}

	_, err = c.DB.Exec(insert3)
	if err != nil {
		log.Fatal(err)
	}
}

var addr = flag.String("addr", "ws-feed.pro.coinbase.com", "http service address")

func (c *Client) Run() {
	flag.Parse()
	log.SetFlags(0)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	u := url.URL{Scheme: "wss", Host: *addr, Path: ""}
	log.Printf("connecting to %s", u.String())

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}

	subscribe := `{
		"type": "subscribe",
    	"product_ids": [
        	"ETH-BTC",
			"BTC-USD",
			"BTC-EUR"
    	],
    	"channels": ["ticker"]
	}`

	conn.WriteMessage(1, []byte(subscribe))
	defer conn.Close()

	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}
			log.Printf("recv: %s", message)

			var tick ticks
			tick, err = parseMessage(message)
			if err == nil {
				c.updateTick(&tick)
			} else {
				log.Println("read:", err)
			}
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case t := <-ticker.C:
			err := conn.WriteMessage(websocket.TextMessage, []byte(t.String()))
			if err != nil {
				log.Println("write:", err)
				return
			}
		case <-interrupt:
			log.Println("interrupt")

			// Cleanly close the connection by sending a close message and then
			// waiting (with timeout) for the server to close the connection.
			err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				log.Println("write close:", err)
				return
			}
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			return
		}
	}
}

func parseMessage(message []byte) (ticks, error) {
	mtype, err := jsonparser.GetString(message, "type")
	if err != nil {
		return ticks{}, err
	}
	if mtype != "ticker" {
		return ticks{}, &MyError{"incorrect json"}
	}

	symbol, _, _, err := jsonparser.Get(message, "product_id")
	if err != nil {
		return ticks{}, err
	}

	bidStr, err := jsonparser.GetString(message, "best_bid")
	if err != nil {
		return ticks{}, err
	}
	bid, err := strconv.ParseFloat(bidStr, 32)
	if err != nil {
		return ticks{}, err
	}

	askStr, err := jsonparser.GetString(message, "best_ask")
	if err != nil {
		return ticks{}, err
	}
	ask, err := strconv.ParseFloat(askStr, 32)
	if err != nil {
		return ticks{}, err
	}

	ts, err := jsonparser.GetString(message, "time")
	if err != nil {
		return ticks{}, err
	}

	timestamp, err := time.Parse(time.RFC3339, string(ts))
	if err != nil {
		return ticks{}, err
	}

	return ticks{timestamp.Unix(), string(symbol), bid, ask}, nil
}

func (c *Client) updateTick(tick *ticks) {
	switch tick.symbol {
	case "ETH-BTC":
		tick.update(c.DB)
	case "BTC-USD":
		tick.update(c.DB)
	case "BTC-EUR":
		tick.update(c.DB)
	default:
	}
}
