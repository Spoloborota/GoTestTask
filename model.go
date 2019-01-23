package main

import (
	"database/sql"
	"fmt"
)

type ticks struct {
	timestamp int64   `json:"timestamp"`
	symbol    string  `json:"symbol"`
	bid       float64 `json:"bid"`
	ask       float64 `json:"ask"`
}

func (t *ticks) updateTickAsk(db *sql.DB) error {
	statement := fmt.Sprintf("UPDATE ticks SET ask = '%f', timestamp = %d WHERE symbol = '%s'", t.ask, t.timestamp, t.symbol)
	_, err := db.Exec(statement)
	return err
}

func (t *ticks) updateTickBid(db *sql.DB) error {
	statement := fmt.Sprintf("UPDATE ticks SET bid = '%f', timestamp = %d WHERE symbol = '%s'", t.bid, t.timestamp, t.symbol)
	_, err := db.Exec(statement)
	return err
}
