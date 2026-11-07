package credentials

import (
	"encoding/json"
	"strings"

	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

// policyDocument is an IAM policy document. AWS STS, MinIO and Ceph RGW all
// accept this format as an AssumeRole session policy.
type policyDocument struct {
	Version   string            `json:"Version"`
	Statement []policyStatement `json:"Statement"`
}

type policyStatement struct {
	Effect    string                         `json:"Effect"`
	Action    []string                       `json:"Action"`
	Resource  []string                       `json:"Resource"`
	Condition map[string]map[string][]string `json:"Condition,omitempty"`
}

// S3SessionPolicy returns the session policy for role on the repository at
// bucket/prefix. The effective permission of an AssumeRole credential is the
// intersection of the caller's own policy and this session policy, so the
// result can never exceed what the broker's admin user has.
//
// writerDeletePrefixes lists sub-prefixes (relative to prefix) that a
// writing role must be able to delete even though it is otherwise
// append-only. restic, for example, deletes its own lock files.
func S3SessionPolicy(role protocol.Role, bucket, prefix string, writerDeletePrefixes []string) string {
	prefix = strings.Trim(prefix, "/")
	objects := "arn:aws:s3:::" + bucket + "/" + prefix + "/*"
	doc := policyDocument{
		Version: "2012-10-17",
		Statement: []policyStatement{
			{
				Effect:   "Allow",
				Action:   []string{"s3:ListBucket"},
				Resource: []string{"arn:aws:s3:::" + bucket},
				Condition: map[string]map[string][]string{
					// Not the bare prefix: "ns/ws" would also match
					// "ns/ws2/..." and list a sibling workspace's keys.
					"StringLike": {"s3:prefix": {prefix + "/", prefix + "/*"}},
				},
			},
			{
				// Clients such as minio-go look up the bucket region before
				// the first request.
				Effect:   "Allow",
				Action:   []string{"s3:GetBucketLocation"},
				Resource: []string{"arn:aws:s3:::" + bucket},
			},
			{
				Effect:   "Allow",
				Action:   []string{"s3:GetObject"},
				Resource: []string{objects},
			},
		},
	}
	if role.CanWrite() {
		doc.Statement = append(doc.Statement, policyStatement{
			Effect:   "Allow",
			Action:   []string{"s3:PutObject"},
			Resource: []string{objects},
		})
	}
	var deletable []string
	if role.CanDelete() {
		deletable = []string{objects}
	} else if role.CanWrite() {
		for _, p := range writerDeletePrefixes {
			deletable = append(deletable, "arn:aws:s3:::"+bucket+"/"+prefix+"/"+strings.Trim(p, "/")+"*")
		}
	}
	if len(deletable) > 0 {
		doc.Statement = append(doc.Statement, policyStatement{
			Effect:   "Allow",
			Action:   []string{"s3:DeleteObject"},
			Resource: deletable,
		})
	}
	b, err := json.Marshal(doc)
	if err != nil {
		// The document contains only strings, so Marshal cannot fail.
		panic(err)
	}
	return string(b)
}
