# Prox

### Configuration
```js
{
	"cloud": {
		"pkg"		: "mssql",
		"connstr"	: "server=$server;user id=$user id;password=$password;port=$port;database=$database;",
		"password"	: "xlkj89kjh84",
		"port"     	: 1433,
		"server"   	: "database.example.net",
		"user id"  	: "user9090",
		"database" 	: "mydb"
	},

	"local": {
		"pkg"		: "mysql",
		"connstr"	: "$user:$password@$host($protocol:$port)/$database", 
		"user"		: "root",
		"password"	: "54321",
		"host"		: "localhost",
		"protocol"  : "tcp",
		"port"	    : 3306,
		"database"	: "mydb"
	}, 

	"main": {
		"pkg"		: "sqlite3",
		"connstr"	: "./$database.db", 
		"database"	: "mydb"
	}

}
```
