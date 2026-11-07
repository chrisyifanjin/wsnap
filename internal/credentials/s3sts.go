package credentials

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

// minSTSDuration is the shortest session AWS STS accepts.
const minSTSDuration = 15 * time.Minute

// placeholderRoleARN satisfies the SDK's RoleArn validation for stores that
// ignore the field, such as MinIO.
const placeholderRoleARN = "arn:aws:iam::000000000000:role/wsnap"

// S3STS issues AssumeRole credentials limited by a session policy.
type S3STS struct {
	// Endpoint is the STS URL including the scheme. Empty means the AWS
	// regional endpoint.
	Endpoint string
	// RoleARN is required on AWS. MinIO ignores it.
	RoleARN string
	Region  string
}

func (p S3STS) Issue(ctx context.Context, req Request) (protocol.Credentials, error) {
	var out protocol.Credentials
	if req.Backend.S3 == nil {
		return out, fmt.Errorf("provider s3-sts needs an s3 backend")
	}
	ak, err := secretValue(req.Secret, KeyAWSAccessKeyID)
	if err != nil {
		return out, err
	}
	sk, err := secretValue(req.Secret, KeyAWSSecretAccessKey)
	if err != nil {
		return out, err
	}
	region := p.Region
	if region == "" {
		region = req.Backend.S3.Region
	}
	if region == "" {
		region = "us-east-1"
	}
	cfg := aws.Config{
		Region:      region,
		Credentials: awscreds.NewStaticCredentialsProvider(ak, sk, string(req.Secret[KeyAWSSessionToken])),
	}
	client := sts.NewFromConfig(cfg, func(o *sts.Options) {
		if p.Endpoint != "" {
			o.BaseEndpoint = aws.String(p.Endpoint)
		}
	})
	ttl := req.TTL
	if ttl < minSTSDuration {
		ttl = minSTSDuration
	}
	roleARN := p.RoleARN
	if roleARN == "" {
		roleARN = placeholderRoleARN
	}
	policy := S3SessionPolicy(req.Role, req.Backend.S3.Bucket, req.Backend.S3.Prefix, req.WriterDeletePrefixes)
	res, err := client.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleARN),
		RoleSessionName: aws.String(sessionName(req)),
		Policy:          aws.String(policy),
		DurationSeconds: aws.Int32(int32(ttl / time.Second)),
	})
	if err != nil {
		return out, fmt.Errorf("sts AssumeRole: %w", err)
	}
	c := res.Credentials
	out.AccessKeyID = aws.ToString(c.AccessKeyId)
	out.SecretAccessKey = aws.ToString(c.SecretAccessKey)
	out.SessionToken = aws.ToString(c.SessionToken)
	out.ExpiresAt = aws.ToTime(c.Expiration)
	out.Scoped = true
	return out, nil
}

// sessionName shows up in CloudTrail and MinIO audit logs. STS limits it to
// 64 characters from [\w+=,.@-], so long names are hashed.
func sessionName(req Request) string {
	name := fmt.Sprintf("wsnap-%s-%s.%s", req.Role, req.Namespace, req.Workspace)
	if len(name) <= 64 {
		return name
	}
	sum := sha256.Sum256([]byte(req.Namespace + "/" + req.Workspace))
	return fmt.Sprintf("wsnap-%s-%s", req.Role, hex.EncodeToString(sum[:])[:40])
}
