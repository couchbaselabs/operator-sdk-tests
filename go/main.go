package main

import (
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/couchbase/gocb/v2"
)

func main() {
	connection := flag.String("connection", "", "Hostname")
	username := flag.String("username", "", "Username")
	password := flag.String("password", "", "Password")
	cafile := flag.String("cafile", "", "CA filename")
	bucketName := flag.String("bucket", "", "Bucket name")

	flag.Parse()

        options := gocb.ClusterOptions{
                Username: *username,
                Password: *password,
        }

	if *cafile != "" {
		ca, err := ioutil.ReadFile(*cafile)
		if err != nil {
			fmt.Println("failed to load CA:", err)
			os.Exit(1)
		}

		caCerts := x509.NewCertPool()
		caCerts.AppendCertsFromPEM(ca)

		options.SecurityConfig = gocb.SecurityConfig{
			TLSRootCAs: caCerts,
		}
	}

	cluster, err := gocb.Connect(*connection, options)
	if err != nil {
		fmt.Println("failed to open connection:", err)
		os.Exit(1)
	}

	bucket := cluster.Bucket(*bucketName)

	if err := bucket.WaitUntilReady(20*time.Second, nil); err != nil {
		fmt.Println("failed to wait for ready bucket:", err)
		os.Exit(1)
	}
}
