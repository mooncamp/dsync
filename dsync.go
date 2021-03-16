package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path/filepath"

	"cloud.google.com/go/storage"
	"github.com/elazarl/goproxy"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

func main() {
	var gcsCredentialsFilePath string
	var bucketName string
	var toWatch string

	flag.StringVar(&gcsCredentialsFilePath, "key-file", "", "service account key file (json)")
	flag.StringVar(&bucketName, "bucket-name", "", "bucket name")
	flag.StringVar(&toWatch, "towatch", "/dgraph/export", "directory to watch")
	flag.Parse()

	client, err := storage.NewClient(context.Background(), option.WithCredentialsFile(gcsCredentialsFilePath))
	if err != nil {
		log.Fatalf("error creating gcs client: %v", err)
	}

	syn := &syncer{
		Client: client,
		Logger: logrus.New(),

		toWatch:    toWatch,
		bucketName: bucketName,
	}
	syn.run()
}

type syncer struct {
	Client *storage.Client
	Logger logrus.FieldLogger

	bucketName string
	toWatch    string
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
