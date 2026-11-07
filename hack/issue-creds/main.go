// Command issue-creds mints one scoped credential and prints it as shell
// exports. It exists to test session policies against a real store.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/chrisyifanjin/wsnap/internal/credentials"
	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

func main() {
	endpoint := flag.String("endpoint", "http://127.0.0.1:9000", "STS endpoint")
	bucket := flag.String("bucket", "wsnap", "bucket")
	prefix := flag.String("prefix", "", "workspace prefix")
	role := flag.String("role", "snapshot", "role")
	del := flag.String("writer-delete", "", "comma-separated writer delete prefixes")
	printPolicy := flag.Bool("print-policy", false, "print the session policy and exit")
	flag.Parse()
	var dels []string
	if *del != "" {
		dels = strings.Split(*del, ",")
	}
	if *printPolicy {
		fmt.Println(credentials.S3SessionPolicy(protocol.Role(*role), *bucket, *prefix, dels))
		return
	}
	p := credentials.S3STS{Endpoint: *endpoint}
	c, err := p.Issue(context.Background(), credentials.Request{
		Role: protocol.Role(*role), Namespace: "exp", Workspace: "exp",
		Backend:              protocol.Backend{S3: &protocol.S3Backend{Bucket: *bucket, Prefix: *prefix}},
		Secret:               map[string][]byte{"AWS_ACCESS_KEY_ID": []byte(os.Getenv("ADMIN_AK")), "AWS_SECRET_ACCESS_KEY": []byte(os.Getenv("ADMIN_SK"))},
		WriterDeletePrefixes: dels,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("export AWS_ACCESS_KEY_ID=%s AWS_SECRET_ACCESS_KEY=%s AWS_SESSION_TOKEN=%s\n", c.AccessKeyID, c.SecretAccessKey, c.SessionToken)
}
