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
	"strconv"
	"strings"
	"time"

	// _ "github.com/denisenkom/go-mssqldb"
	// _ "github.com/go-sql-driver/mysql"
	// _ "github.com/mattn/go-sqlite3"

	_ "./go-mssqldb"
	_ "./mysql"
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
Analytics and ETL Server
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
	for m := range manager {
		interpret(nil, fmt.Sprintf(".use %s", m))
		fmt.Printf(intro, now.Month(), now.Day(), now.Year(), m)
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

func interpret(cn net.Conn, text string) string {
	output := "\n"
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
		output = clearScreen()
	case ".run":
		b, _ := ioutil.ReadFile(txtArr[1])
		for _, res := range query(current, string(b)) {
			j, _ := json.MarshalIndent(res, "", "\t")
			output += fmt.Sprintf("%s\r\n\r\n(%d rows returned)\r\n", string(j), len(res))
		}

	case ".create":
		switch txtArr[1] {
		case "table":

		}

	case ".into":
		if db, ok := manager["main"]; ok {

		} else {
			output = "(please identify a main database)\n"
		}

	case ".main":
		manager["main"] = manager[txtArr[1]]

	case ".temp":
		db := manager["main"]
		for i, q := range query(current, strings.Join(txtArr[2:], " ")) {
			table := fmt.Sprintf("%s%0.2d", txtArr[1], i)
			raw := m2s(q)

			var headers []string
			for _, head := range raw[0] {
				headers = append(headers, strings.Replace(strings.Title(head), " ", "", -1))
			}

			create := fmt.Sprintf(
				"create table %s (_id integer not null primary key, %s text);",
				table,
				strings.Join(headers, " text, "),
			)

			_, err := db.Exec(create)
			if err != nil {
				log.Printf("%q: %s\n", err, create)
				return output
			}

			var id int
			for _, row := range raw[1:] {
				id++

				values := append([]string{strconv.Itoa(id)}, row...)
				for i, s := range values {
					values[i] = fmt.Sprintf("%q", s)
				}

				insert := fmt.Sprintf(
					"insert into %s values(%s);",
					table,
					strings.Join(values, ","),
				)

				_, err = db.Exec(insert)
				if err != nil {
					log.Printf("%q: %s\n", err, insert)
					return output
				}
			}
		}

	case ".set":
		vars[txtArr[1]] = query(current, strings.Join(txtArr[2:], " "))
	case ".unset":
		delete(vars, txtArr[1])
	case ".get":
		for _, res := range vars[txtArr[1]] {
			j, _ := json.MarshalIndent(res, "", "\t")
			// output += fmt.Sprintf("%s\r\n", string(j))
			output += fmt.Sprintf("%s\r\n\r\n(%d rows returned)\r\n", string(j), len(res))

		}

	case ".ping", ".status":
		for name := range manager {
			db := manager[name]
			ping(name, db)
		}

	case ".reconnect", ".reconn", ".connect", ".conn":
		connect()

	default:
		for _, res := range query(current, text) {
			switch mode {
			case "json":
				j, _ := json.MarshalIndent(res, "", "\t")
				// output += fmt.Sprintf("%s\r\n", string(j))
				output += fmt.Sprintf("%s\r\n\r\n(%d rows returned)\r\n", string(j), len(res))

			default:
				// output += fmt.Sprintf("%s\r\n", sortKeys(res))
				output += fmt.Sprintf("%s\r\n\r\n(%d rows returned)\r\n", sortKeys(res), len(res))

			}
		}
	}
	return output
}

func ping(name string, db *sql.DB) error {
	if err := db.Ping(); err != nil {
		fmt.Printf("database '%s'...\t\tFAIL (%s)\n", name, err.Error())
		return err
	} else {
		fmt.Printf("database '%s'...\t\tPASS\n", name)
		return nil
	}
}

func m2s(m []map[string]interface{}) [][]string {
	unicols := map[string]bool{}
	for i := range m {
		for key := range m[i] {
			unicols[key] = true
		}
	}

	cols := []string{}
	for key := range unicols {
		cols = append(cols, key)
	}
	sort.Strings(cols)

	data := [][]string{}
	data = append(data, cols)
	for i := range m {
		row := []string{}
		for _, name := range cols {
			row = append(row, fmt.Sprintf("%v", m[i][name]))
		}
		data = append(data, row)
	}
	return data
}

func connect() {
	fmt.Println(clearScreen())
	flag.Parse()

	b, _ := ioutil.ReadFile(*config)
	var cf map[string]map[string]interface{}
	if err := json.Unmarshal(b, &cf); err != nil {
		panic(err)
	}

	for name, conn := range cf {
		connStr := interpolate(conn["connstr"].(string), conn)
		if *debug {
			fmt.Println(connStr)
		}

		db, _ := sql.Open(conn["pkg"].(string), connStr)
		if ping(name, db) == nil {
			manager[name] = db
		}
	}
	fmt.Println()
}

func query(conn, script string) [][]map[string]interface{} {
	db := manager[conn]
	queryset := strings.Split(script, ";")

	var metastore [][]map[string]interface{}

	for _, qry := range queryset {
		qry = cleanQuery(qry)
		if qry != " " && qry != "" {
			rows, err := db.Query(qry)
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

func cleanQuery(qry string) string {

	comments := regexp.MustCompile(`--[^\n]*\n`)
	spaces := regexp.MustCompile(`\s+`)

	qry = comments.ReplaceAllString(qry, "")
	qry = spaces.ReplaceAllString(qry, " ")

	return strings.TrimSpace(qry)
}

func sortKeys(data []map[string]interface{}) (output string) {
	for _, m := range data {
		var keys []string
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		pairs := []string{}
		for _, k := range keys {
			pairs = append(pairs, fmt.Sprintf("%s: %v", k, m[k]))
		}
		output += strings.Join(pairs, ",\t") + "\n"
	}
	return
}

func clearScreen() string {
	cmd, _ := exec.Command("clear").Output()
	return string(cmd)
}

func interpolate(s string, m map[string]interface{}) string {
	for k := range m {
		old := fmt.Sprintf("$%s", k)
		nue := fmt.Sprintf("%v", m[k])
		s = strings.Replace(s, old, nue, -1)

	}
	return s
}
