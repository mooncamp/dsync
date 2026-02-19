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
	"time"

	"github.com/elazarl/goproxy"
	"github.com/sirupsen/logrus"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"
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

	ssec, err := encrypt.NewSSEC([]byte(cryptoKey))
	if err != nil {
		log.Fatalf("error initializing crypto key: %v", err)
	}

	transport, err := minio.DefaultTransport(true)
	if err != nil {
		log.Fatalln(err)
	}
	transport.ResponseHeaderTimeout = 15 * time.Minute
	transport.IdleConnTimeout = 90 * time.Second

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(s3AccessKey, s3Secret, ""),
		Secure:    true,
		Transport: transport,
	})
	if err != nil {
		log.Fatalln(err)
	}

	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{TimestampFormat: time.RFC3339})
	syn := &syncer{
		Logger: logger,

		SSEC:        ssec,
		MinioClient: minioClient,

		toWatch:    toWatch,
		bucketName: bucketName,
		cryptoKey:  cryptoKey,
	}
	syn.run()
}

type syncer struct {
	MinioClient *minio.Client
	SSEC        encrypt.ServerSide
	Logger      logrus.FieldLogger

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
	return "Failed"
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

func (syn *syncer) processEvent(exportResponse ExportResponse) error {
	if err := syn.busyWait(context.Background(), exportResponse); err != nil {
		syn.Logger.WithField("task.id", exportResponse.ID()).WithError(err).Error("error during busy wait")
		return err
	}

	files, err := os.ReadDir(syn.toWatch)
	if err != nil {
		syn.Logger.WithField("task.id", exportResponse.ID()).WithError(err).Error("error reading watch dir")
		return err
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

	return nil
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
			_ = syn.processEvent(exportResponse)
		}
	}()

	if err := http.ListenAndServe(":10080", proxy); err != nil {
		return fmt.Errorf("error running server: %w", err)
	}

	return nil
}
