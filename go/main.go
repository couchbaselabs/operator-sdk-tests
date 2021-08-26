package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/couchbase/gocb"
)

func main() {
	connection := flag.String("connection", "", "Hostname")
	username := flag.String("username", "", "Username")
	password := flag.String("password", "", "Password")
	cafile := flag.String("cafile", "", "CA filename")
	bucketName := flag.String("bucket", "", "Bucket name")

	flag.Parse()

	connstr := *connection
	if *cafile != "" {
		connstr += fmt.Sprintf("?cacertpath=%s", *cafile)
	}

	cluster, err := gocb.Connect(connstr)
	if err != nil {
		fmt.Println("failed to open connection:", err)
		os.Exit(1)
	}

	authenticator := gocb.PasswordAuthenticator{
		Username: *username,
		Password: *password,
	}

	if err := cluster.Authenticate(authenticator); err != nil {
		fmt.Println("failed to authenticate:", err)
		os.Exit(1)
	}

	if _, err := cluster.OpenBucket(*bucketName, ""); err != nil {
		fmt.Println("failed to use bucket:", err)
		os.Exit(1)
	}
}
