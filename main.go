package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
)

var (
	debug   = flag.Bool("debug", false, "enable debugging")
	config  = flag.String("confit", "config.json", "connection configuration file")
	manager = map[string]*sql.DB{}
	current string
	vars    = map[string][][]map[string]interface{}{}
)

func main() {
	go proxy()
	connect()
	listen()
}

func proxy() {
	// Listen on TCP port  on all interfaces.
	if ln, err := net.Listen("tcp", ":2000"); err == nil {
		defer ln.Close()
		for {
			// Wait for a connection.
			if conn, err := ln.Accept(); err == nil {
				// Handle the connection in a new goroutine.
				// The loop then returns to accepting, so that
				// multiple connections may be served concurrently.
				go scan(conn)
			}
		}
	}
}

func scan(cn net.Conn) {
	scanner := bufio.NewScanner(cn)
	cn.Write([]byte("\n~> "))

	scanner.Scan()
	text := scanner.Text()

	output := interpret(cn, text)
	cn.Write([]byte(output))

	scan(cn)
}

func listen() {
	for {
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Print("~> ")
		scanner.Scan()
		text := scanner.Text()

		output := interpret(nil, text)
		fmt.Println(output)
	}
}

func interpret(cn net.Conn, text string) (output string) {
	txtArr := strings.Split(text, " ")

	switch strings.ToLower(txtArr[0]) {
	case ".quit", ".exit", ".q":
		// os.Exit(1) // change to end connection
		cn.Close()
	case ".current":
		output = fmt.Sprintf("current database: %v\n", current)
	case ".use":
		current = txtArr[1]
	case ".clear":
		cmd, _ := exec.Command("clear").Output()
		output = fmt.Sprintln(string(cmd))
	case ".run":
		b, _ := ioutil.ReadFile(txtArr[1])
		for _, v := range query(current, string(b)) {
			j, _ := json.MarshalIndent(v, "", "\t")
			output += fmt.Sprintf("%s\r\n", string(j))
		}
	// 	b, _ := ioutil.ReadFile("./sql/" + txtArr[1])
	// 	script := string(b)
	// 	Query(db, script)
	// case "export":
	// 	file, _ := ioutil.ReadFile("./sql/" + txtArr[1])
	// 	rows, err := db.Query(string(file))
	// 	if err != nil {
	// 		// log.Fatal("Query Error: ", err.Error())
	// 		fmt.Println(err.Error(), "\n")
	// 		continue
	// 	}
	// 	defer rows.Close()

	// 	columns, _ := rows.Columns()
	// 	count := len(columns)
	// 	values := make([]interface{}, count)
	// 	valuePtrs := make([]interface{}, count)

	// 	var megastore []map[string]interface{}

	// 	for rows.Next() {
	// 		for i, _ := range columns {
	// 			valuePtrs[i] = &values[i]
	// 		}

	// 		rows.Scan(valuePtrs...)
	// 		store := make(map[string]interface{})
	// 		for i, col := range columns {
	// 			var v interface{}
	// 			val := values[i]
	// 			b, ok := val.([]byte)

	// 			if ok {
	// 				v = string(b)
	// 			} else {
	// 				v = val
	// 			}
	// 			store[col] = v
	// 		}
	// 		megastore = append(megastore, store)
	// 		// fmt.Println(store)
	// 	}
	// 	fmt.Println("\n")

	// 	j, err := json.Marshal(megastore)
	// 	ioutil.WriteFile(txtArr[2], j, 0777)

	case ".set":
		vars[txtArr[1]] = query(current, strings.Join(txtArr[2:], " "))
	case ".get":
		for _, v := range vars[txtArr[1]] {
			j, _ := json.MarshalIndent(v, "", "\t")
			output += fmt.Sprintf("%s\r\n", string(j))
		}

	default:
		for _, v := range query(current, text) {
			j, _ := json.MarshalIndent(v, "", "\t")
			output += fmt.Sprintf("%s\r\n", string(j))
		}
	}

	return output

}

func connect() {
	flag.Parse()
	b, _ := ioutil.ReadFile(*config)
	var cf map[string]map[string]interface{}
	if err := json.Unmarshal(b, &cf); err != nil {
		panic(err)
	}

	for name, conn := range cf {
		var connStr string
		switch name {
		case "mssql":
			for k, v := range conn {
				connStr += fmt.Sprintf("%s=%v;", k, v)
			}

			if *debug {
				fmt.Println(connStr)
			}
		case "mysql":
			connStr = fmt.Sprintf("%s:%s@%s(%s:%v)/%s",
				conn["user"],
				conn["password"],
				conn["protocol"],
				conn["host"],
				conn["port"],
				conn["database"],
			)

		}

		db, err := sql.Open(name, connStr)
		if err != nil {
			log.Fatal("Open connection failed:", err.Error())
		} else {
			fmt.Println("connected to database...\nPinging database: ")
			if err = db.Ping(); err != nil {
				fmt.Printf("Error: %s\n%v\n", err)
			} else {
				fmt.Print("success\n")
			}
		}
		// defer db.Close()
		manager[name] = db
	}
}

func query(conn, script string) [][]map[string]interface{} {
	db := manager[conn]
	queryset := strings.Split(script, ";")

	var metastore [][]map[string]interface{}

	for _, query := range queryset {
		query = CleanQuery(query)
		fmt.Printf("\n===========================\n%s\n---------------------------\n", query)

		if query != " " && query != "" {
			rows, err := db.Query(query)
			if err != nil {
				// log.Fatal("Query Error: ", err.Error())
				fmt.Println(err.Error(), "\n")
				continue
			}
			defer rows.Close()

			var megastore []map[string]interface{}

			columns, _ := rows.Columns()
			count := len(columns)
			values := make([]interface{}, count)
			valuePtrs := make([]interface{}, count)

			for rows.Next() {
				for i, _ := range columns {
					valuePtrs[i] = &values[i]
				}

				rows.Scan(valuePtrs...)
				store := make(map[string]interface{})
				for i, col := range columns {
					var v interface{}
					val := values[i]
					b, ok := val.([]byte)

					if ok {
						v = string(b)
					} else {
						v = val
					}
					store[col] = v
				}
				megastore = append(megastore, store)
			}
			metastore = append(metastore, megastore)
		}
	}

	return metastore
}

func CleanQuery(q string) string {
	r1 := regexp.MustCompile(`\s+`)
	r2 := regexp.MustCompile(`--[^\n]*\n`)
	q = r2.ReplaceAllString(q, "")
	return r1.ReplaceAllString(q, " ")
}
