	case ".create":
		switch txtArr[1] {
		case "table":
			for _, data := range query(cn, script) {

			}
		}

	// case ".temp":
	// 	db := manager["main"]
	// 	for i, q := range query(current, strings.Join(txtArr[2:], " ")) {
	// 		table := fmt.Sprintf("%s%0.2d", txtArr[1], i)
	// 		raw := m2s(q)

	// 		var headers []string
	// 		for _, head := range raw[0] {
	// 			headers = append(headers, strings.Replace(strings.Title(head), " ", "", -1))
	// 		}

	// 		create := fmt.Sprintf(
	// 			"create table %s (_id integer not null primary key, %s text);",
	// 			table,
	// 			strings.Join(headers, " text, "),
	// 		)

	// 		_, err := db.Exec(create)
	// 		if err != nil {
	// 			log.Printf("%q: %s\n", err, create)
	// 			return
	// 		}

	// 		var id int
	// 		for _, row := range raw[1:] {
	// 			id++

	// 			values := append([]string{strconv.Itoa(id)}, row...)
	// 			for i, s := range values {
	// 				values[i] = fmt.Sprintf("%q", s)
	// 			}

	// 			insert := fmt.Sprintf(
	// 				"insert into %s values(%s);",
	// 				table,
	// 				strings.Join(values, ","),
	// 			)

	// 			_, err = db.Exec(insert)
	// 			if err != nil {
	// 				log.Printf("%q: %s\n", err, insert)
	// 				return
	// 			}
	// 		}
	// 	}
