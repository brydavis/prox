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
	"sort"
	"strings"
	"time"

	// _ "github.com/denisenkom/go-mssqldb"
	// _ "github.com/go-sql-driver/mysql"
	_ "./go-mssqldb"
	_ "./odbc"
)

var (
	debug   = flag.Bool("debug", false, "enable debugging")
	config  = flag.String("config", "config.json", "connection configuration file")
	manager = map[string]*sql.DB{}
	current string
	vars    = map[string][][]map[string]interface{}{}
	now     = time.Now()
	mode    = ""

	intro = `
STAX Analytics and ETL Server
=====================================
Today's date: %v %d, %d
Current database: "%s"

`

	help = `
STAX Help Menu
=====================================
	.use
	.quit, .exit, .q
	.clear, .cls
	.current
	.run
	.help, .h
	.set
	.unset
	.get
	.mode // csv, xml, json...

	// .export // "filename.csv" select * from t1
	// .join // x a b
	// .analysis
	// .search, .find //
	// .describe //


`
)

func main() {
	// go proxy()
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

	for k, _ := range manager {
		interpret(nil, fmt.Sprintf(".use %s", k))
		fmt.Printf(intro, now.Month(), now.Day(), now.Year(), k)
		break
	}

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
		if cn == nil {
			os.Exit(1) // change to end connection
		} else {
			cn.Close()
		}
	case ".help", ".h":
		output = fmt.Sprintln(help)
	case ".ping":
		var db *sql.DB
		if len(txtArr) == 1 {
			db = manager[current]
		} else {
			db = manager[txtArr[1]]
		}

		if err := db.Ping(); err != nil {
			fmt.Println(err, "\n")
		} else {
			fmt.Println("pong")
		}
	case ".current":
		output = fmt.Sprintf("current database: %v\n", current)
	case ".use":
		current = txtArr[1]
	case ".mode":
		switch txtArr[1] {
		case "json":
			mode = "json"
		case "csv":
			mode = "csv"
		case "xml":
			mode = "xml"
		default:
			mode = ""
		}
	case ".clear", ".cls":
		output = clear()
	case ".run":
		b, _ := ioutil.ReadFile(txtArr[1])
		output = display(string(b))
	case ".disconnect":
		var db *sql.DB
		if len(txtArr) == 1 {
			db = manager[current]
		} else {
			db = manager[txtArr[1]]
		}
		db.Close()

	case ".set":
		vars[txtArr[1]] = query(current, strings.Join(txtArr[2:], " "))
	case ".unset":
		delete(vars, txtArr[1])
	case ".get":
		for _, v := range vars[txtArr[1]] {
			j, _ := json.MarshalIndent(v, "", "\t")
			output += fmt.Sprintf("%s\r\n", string(j))
		}

	case ".export": // "filename.csv" select * from t1

	case ".join": // x a b
		t := join(vars[txtArr[1]][0], vars[txtArr[2]][0], txtArr[3:]...)
		j, _ := json.MarshalIndent(t, "", "\t")
		fmt.Println(string(j))

	// .analysis
	// .search, .find //
	// .describe //

	default:
		output = display(text)
	}

	return output

}

func join(t1, t2 []map[string]interface{}, on ...string) []map[string]interface{} {
	m := []map[string]interface{}{}
	for a := 0; a < len(t1); a++ {
		for b := 0; b < len(t2); b++ {
			var matches int
			for i := range on {
				on1 := strings.TrimSpace(strings.ToLower(fmt.Sprintf("%v", t1[a][on[i]])))
				on2 := strings.TrimSpace(strings.ToLower(fmt.Sprintf("%v", t2[b][on[i]])))
				// fmt.Printf("%s\t\"%s\"\t\"%s\"\n", on[i], on1, on2)

				if on1 == on2 {
					matches += 1
				}
			}

			if len(on) == matches {
				r := map[string]interface{}{}
				for k, v := range t1[a] {
					r[k] = v
				}
				for k, v := range t2[b] {
					r[k] = v
				}
				m = append(m, r)
			}
		}
	}
	return m
}

func display(text string) (output string) {
	for _, v := range query(current, text) {
		switch mode {
		case "json":
			j, _ := json.MarshalIndent(v, "", "\t")
			output += fmt.Sprintf("%s\r\n", string(j))

		default:
			keys := sortKeys(v)
			for i, row := range v {
				for _, k := range keys {
					output += fmt.Sprintf("\t%s: %v\r\n", k, row[k])
				}
				output += fmt.Sprintf("%0.3d ----\r\n", i)
			}
		}
	}
	return
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

		default:
			for k, v := range conn {
				connStr += fmt.Sprintf("%v=%v;", k, v)
			}
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
		fmt.Println()
		// defer db.Close()
		manager[name] = db
	}
}

func query(conn, script string) [][]map[string]interface{} {
	var (
		db         = manager[conn]
		queryset   = strings.Split(script, ";")
		worksheets [][]map[string]interface{}
	)

	for _, qry := range queryset {
		qry = clean(qry)
		fmt.Printf("\n===========================\n%s\n---------------------------\n", qry)

		if qry != " " && qry != "" {
			rows, err := db.Query(qry)
			if err != nil {
				// log.Fatal("Query Error: ", err.Error())
				fmt.Println(err.Error(), "\n")
				continue
			}
			defer rows.Close()

			var (
				sheet      []map[string]interface{}
				columns, _ = rows.Columns()
				count      = len(columns)
				values     = make([]interface{}, count)
				valuePtrs  = make([]interface{}, count)
			)

			for rows.Next() {
				for i, _ := range columns {
					valuePtrs[i] = &values[i]
				}

				rows.Scan(valuePtrs...)
				row := make(map[string]interface{})
				for i, col := range columns {
					var v interface{}
					val := values[i]
					b, ok := val.([]byte)

					if ok {
						v = string(b)
					} else {
						v = val
					}
					row[col] = v
				}
				sheet = append(sheet, row)
			}
			worksheets = append(worksheets, sheet)
		}
	}
	return worksheets
}

func clean(qry string) string {
	r1 := regexp.MustCompile(`\s+`)
	r2 := regexp.MustCompile(`--[^\n]*\n`)
	qry = r2.ReplaceAllString(qry, "")
	return r1.ReplaceAllString(qry, " ")
}

// func sortKeys(data []map[string]interface{}) (output string) {
// 	for _, m := range data {
// 		var keys []string
// 		for k := range m {
// 			keys = append(keys, k)
// 		}
// 		sort.Strings(keys)
// 		for _, k := range keys {
// 			output += fmt.Sprintf("%s: %v, ", k, m[k])
// 		}
// 		output += "\n"
// 	}
// 	return
// }

func sortKeys(data []map[string]interface{}) []string {
	uniqueKeys := map[string]bool{}
	for _, m := range data {
		for k := range m {
			// keys = append(keys, k)
			uniqueKeys[k] = true
		}
	}

	var keys []string
	for k := range uniqueKeys {
		keys = append(keys, k)
	}

	sort.Strings(keys)
	return keys
}

func clear() string {
	cmd, _ := exec.Command("clear").Output()
	return fmt.Sprintln(string(cmd))
}
