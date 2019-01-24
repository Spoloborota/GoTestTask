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

func (t *ticks) update(db *sql.DB) error {
	statement := fmt.Sprintf("UPDATE testsdb.ticks SET ask = %f, bid = %f, timestamp = %d WHERE symbol = '%s'", t.ask, t.bid, t.timestamp, t.symbol)
	fmt.Println(statement)
	_, err := db.Exec(statement)
	return err
}
