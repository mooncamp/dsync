package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"

	"github.com/elazarl/goproxy"
	"github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

func main() {
	var endpoint, s3AccessKey, s3Secret string
	var bucketName string
	var toWatch string
	var cryptoKey string

	flag.StringVar(&bucketName, "bucket-name", "", "bucket name")
	flag.StringVar(&toWatch, "towatch", "/dgraph/export", "directory to watch")
	flag.StringVar(&endpoint, "endpoint", "", "s3 endpoint")
	flag.StringVar(&s3AccessKey, "access-key", "", "s3 access key")
	flag.StringVar(&s3Secret, "secret", "", "s3 secret")
	flag.StringVar(&cryptoKey, "crypto-key", "", "data encryption key")
	flag.Parse()

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("de"),
		Endpoint:    aws.String(endpoint),
		Credentials: credentials.NewStaticCredentials(s3AccessKey, s3Secret, ""),
	})
	if err != nil {
		log.Fatalf("error initiating session: %v", err)
	}

	syn := &syncer{
		Logger:  logrus.New(),
		Session: sess,

		toWatch:    toWatch,
		bucketName: bucketName,
		cryptoKey:  cryptoKey,
	}
	syn.run()
}

type syncer struct {
	Session *session.Session
	Logger  logrus.FieldLogger

	bucketName string
	toWatch    string
	cryptoKey  string
}

func (syn *syncer) run() error {
	proxy := goproxy.NewProxyHttpServer()
	proxy.Logger = syn.Logger
	proxy.Verbose = true

	event := make(chan struct{})
	proxy.OnResponse(goproxy.UrlIs("/admin/export")).DoFunc(func(resp *http.Response, _ *goproxy.ProxyCtx) *http.Response {
		event <- struct{}{}
		return resp
	})

	go func() {
		for _ = range event {
			files, err := ioutil.ReadDir(syn.toWatch)
			if err != nil {
				syn.Logger.Errorf("error reading watch dir: %v", err)
			}

			for _, file := range files {
				if !file.IsDir() {
					continue
				}

				syn.Logger.Infof("syncing dir: %s", filepath.Join(syn.toWatch, file.Name()))

				if err := syn.handleEvent(context.Background(), filepath.Join(syn.toWatch, file.Name())); err != nil {
					syn.Logger.Errorf("error syncing dir %s: %v", filepath.Join(syn.toWatch, file.Name()), err)
				}
			}
		}
	}()

	if err := http.ListenAndServe(":10080", proxy); err != nil {
		return fmt.Errorf("error running server: %w", err)
	}

	return nil
}
