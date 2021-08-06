package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"log"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	TABLENAME = ""
	FILENAME = ""
	DELIMITER = '\t'		// default delimiter for csv files
	MAX_SQL_CONNECTIONS = 100	// default max_connections of mysql
	CONN_STR = ""
	ON_DUP_KEYS_UPDATE = false
)

// parse flags and command line arguments
func parseSysArgs() {
	db := flag.String("db", CONN_STR, "connection string example shopee_foody_data:@tcp(db-master-foody-algo-data-id-sg1-live.shopeemobile.com:6606)/shopee_foody_algo_data_id_db")
	table := flag.String("table", TABLENAME, "Name of MySQL database table.")
	delimiter := flag.String("d", string(DELIMITER), "Delimiter used in .csv file.")
	max_conns := flag.Int("conns", MAX_SQL_CONNECTIONS, "Maximum number of concurrent connections to database. Value depends on your MySQL configuration.")
	on_dup_keys_update := flag.Bool("enable_update", ON_DUP_KEYS_UPDATE, "enable insert on duplicate key update, BE CAREFUL, it might cause deadlock.")

	flag.Parse()

	TABLENAME = *table
	DELIMITER = []rune(*delimiter)[0]
	MAX_SQL_CONNECTIONS = *max_conns
	CONN_STR = *db
	ON_DUP_KEYS_UPDATE = *on_dup_keys_update
}

func main() {

	parseSysArgs()

	// --------------------------------------------------------------------------
	// prepare buffered file reader
	// --------------------------------------------------------------------------
	file := os.NewFile(uintptr(syscall.Stdin), "/dev/stdin")
	reader := csv.NewReader(file)
	reader.Comma = DELIMITER		// set custom comma for reader (default: ',')
	// a,b,c"d" working as 3 cols when LazyQuotes is true
	reader.LazyQuotes = true

	// --------------------------------------------------------------------------
	// database connection setup
	// --------------------------------------------------------------------------

	db, err := sql.Open("mysql", CONN_STR)
	if err != nil {
		log.Fatal(err.Error())
		return
	}

	// check database connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	// set max idle connections
	db.SetMaxIdleConns(MAX_SQL_CONNECTIONS)
	defer db.Close()


	// --------------------------------------------------------------------------
	// read rows and insert into database
	// --------------------------------------------------------------------------

	start := time.Now()									// to measure execution time


	query := ""											// query statement
	callback 	:= make(chan int, 1024)						// callback channel for insert goroutines
	connections := int64(0)									// number of concurrent connections
	insertions := int64(0)									// counts how many insertions have finished
	fails := int64(0)
	available 	:= make(chan bool, MAX_SQL_CONNECTIONS)	// buffered channel, holds number of available connections
	for i := 0; i < MAX_SQL_CONNECTIONS; i++ {
		available <- true
	}


	// start status logger
	startLogger(&insertions, &fails, &connections)

	// start connection controller
	startConnectionController(&insertions, &fails, &connections, callback, available)

	var wg sync.WaitGroup
	id := -1
	isFirstRow := true

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err.Error())
		}

		if isFirstRow {
			parseColumns(record, &query)
			isFirstRow = false
		} else if <-available {		// wait for available database connection
			id++
			wg.Add(1)
			args := repalceNULLByDEFAULT(record)
			go insert(id, query, db, callback, &connections, &wg, args)
		}
	}
	wg.Wait()

	for ;len(callback) > 0; {}
	elapsed := time.Since(start)
	log.Printf("Execution time: %s\n", elapsed)
	log.Printf("Status: %d insertions, %d valid insertions\n", insertions, insertions - fails)
	log.Printf("QPS: %d , Valid QPS: %d\n",
			  insertions *1000000000/elapsed.Nanoseconds(), (insertions - fails) *1000000000/elapsed.Nanoseconds())
}

// inserts data into database
func insert(id int, query string, db *sql.DB, callback chan<- int, connections *int64, wg *sync.WaitGroup, args []interface{}) {
	// make a new statement for every insert,
	// this is quite inefficient, but since all inserts are running concurrently,
	// it's still faster than using a single prepared statement and
	// inserting the data sequentielly.
	// we have to close the statement after the routine terminates,
	// so that the connection to the database is released and can be reused
	atomic.AddInt64(connections, 1)
	stmt, err := db.Prepare(query)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer wg.Done()
	defer stmt.Close()

	_, err = stmt.Exec(args...)
	if err != nil {
		log.Printf("Row Number: %d, %s\n", id, err.Error())
		id = -id
	}
	atomic.AddInt64(connections, -1)
	// finished inserting, send id over channel to signalize termination of routine
	callback <- id
}

// controls termination of program and number of connections to database
func startConnectionController(insertions, fails, connections *int64, callback <-chan int, available chan<- bool) {
	go func() { for {
		atomic.AddInt64(insertions, 1)	// a routine terminated, increment counter
		id := <-callback	// returns id of terminated routine
		if (id<0) {
		  atomic.AddInt64(fails, 1)
		}
		available <- true	// make new connection available
	}}()
}

// print status update to console every second
func startLogger(insertions, fails, connections *int64) {
	go func() {
		c := time.Tick(time.Second)
		for {
			<-c
			log.Printf("Status: %d insertions, %d inuse connections, %d fails\n", *insertions, *connections, *fails)
		}
	}()
}

// parse csv columns, create query statement
func parseColumns(columns []string, query *string) {
	*query = "INSERT INTO "+TABLENAME+" ("
	placeholder := "VALUES ("
	update := "ON DUPLICATE KEY UPDATE "
	for i, c := range columns {
		if i == 0 {
			*query += c
			placeholder += "?"
			update += (c + "=VALUES(" + c + ")")
		} else {
			*query += ", "+c
			placeholder += ", ?"
			update += (", "+ c + "=VALUES(" + c + ")")
		}
	}
	placeholder += ")"
	*query += ") " + placeholder
	if ON_DUP_KEYS_UPDATE {
		*query += update
	}
}

// convert []string to []interface{}
func repalceNULLByDEFAULT(s []string) []interface{} {
	i := make([]interface{}, len(s))
	for k, v := range s {
	    if v == "NULL" {
	        i[k] = nil  // sql.NullString{}
	    } else {
	        i[k] = v
	    }
	}
	return i
}
