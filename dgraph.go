package main

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type readMockSeeker struct {
	Reader io.Reader
}

func (rms *readMockSeeker) Read(p []byte) (n int, err error) {
	return rms.Reader.Read(p)
}

func (rms *readMockSeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (syn *syncer) handleEvent(ctx context.Context, dir string) error {
	stats, err := ioutil.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("error reading dir %s: %w", dir, err)
	}

	for _, stat := range stats {
		if strings.Contains(stat.Name(), "gql_schema") {
			continue
		}

		var file *os.File
		for i := 0; i < 10; i++ {
			file, err = os.OpenFile(filepath.Join(dir, stat.Name()), os.O_EXCL|os.O_RDONLY, 0600)
			if err != nil {
				syn.Logger.Infof("error opening file (O_EXCL): %v", err)
				time.Sleep(time.Second * 5)
				continue
			}
		}

		if err != nil {
			return fmt.Errorf("error opening file %s: %w", filepath.Join(dir, stat.Name()), err)
		}

		syn.Logger.Infof("initiating sync of %s to s3://%s/%s", filepath.Join(dir, stat.Name()), syn.bucketName, syn.findObject(stat.Name()))

		h := md5.New()
		if _, err := h.Write([]byte(syn.cryptoKey)); err != nil {
			return fmt.Errorf("error generating md5 digest of encryption key: %w", err)
		}
		digest := base64.StdEncoding.EncodeToString(h.Sum(nil))

		svc := s3.New(syn.Session)
		_, err = svc.PutObject(&s3.PutObjectInput{
			Bucket:               aws.String(syn.bucketName),
			Key:                  aws.String(syn.findObject(stat.Name())),
			Body:                 file,
			SSECustomerAlgorithm: aws.String("AES256"),
			SSECustomerKey:       aws.String(syn.cryptoKey),
			SSECustomerKeyMD5:    aws.String(digest),
		})
		if err != nil {
			return fmt.Errorf("error storing backup object to s3: %w", err)
		}

		syn.Logger.Printf("synchronized %s to s3://%s/%s", filepath.Join(dir, stat.Name()), syn.bucketName, syn.findObject(stat.Name()))
	}

	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("error cleaning dir %s: %w", dir, err)
	}

	return nil
}

func (*syncer) findObject(in string) string {
	switch {
	case strings.Contains(in, "rdf"):
		return "transformer.rdf.gz"
	case strings.Contains(in, "schema"):
		return "transformer.schema.gz"
	default:
		return "unknown-object"
	}
}
