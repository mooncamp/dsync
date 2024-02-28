package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"

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

type ExportResponse struct {
	Data struct {
		Export struct {
			Response struct {
				Message string `json:"message"`
				Code    string `json:"code"`
			} `json:"response"`
		} `json:"export"`
	} `json:"data"`
}

type StatusResponse struct {
	Data struct {
		Task struct {
			Status TaskStatus `json:"status"`
		} `json:"task"`
	} `json:"data"`
}

type TaskStatus string

func (TaskStatus) Queued() TaskStatus {
	return "Queued"
}
func (TaskStatus) Running() TaskStatus {
	return "Running"
}
func (TaskStatus) Failed() TaskStatus {
	return "Running"
}
func (TaskStatus) Success() TaskStatus {
	return "Success"
}
func (TaskStatus) Unknown() TaskStatus {
	return "Unknown"
}

// Export queued with ID 0x125b3ab7a
func (resp ExportResponse) ID() string {
	words := strings.Split(resp.Data.Export.Response.Message, " ")
	slices.Reverse(words)
	return words[0]
}

func (syn *syncer) run() error {
	proxy := goproxy.NewProxyHttpServer()
	proxy.Logger = syn.Logger
	proxy.Verbose = true

	event := make(chan ExportResponse)
	proxy.OnResponse(goproxy.UrlIs("/admin")).DoFunc(func(resp *http.Response, ctx *goproxy.ProxyCtx) *http.Response {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			syn.Logger.WithError(err).Error("error while reading response body")
			return resp
		}

		var exportResponse ExportResponse
		if err := json.Unmarshal(body, &exportResponse); err != nil {
			syn.Logger.WithError(err).Error("error while decoding response body")
			return resp
		}

		event <- exportResponse
		return resp
	})

	go func() {
		for exportResponse := range event {
			if err := syn.busyWait(context.Background(), exportResponse); err != nil {
				syn.Logger.WithField("task.id", exportResponse.ID()).WithError(err).Error("error during busy wait")
				continue
			}

			files, err := os.ReadDir(syn.toWatch)
			if err != nil {
				syn.Logger.WithField("task.id", exportResponse.ID()).WithError(err).Error("error reading watch dir")
				continue
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
