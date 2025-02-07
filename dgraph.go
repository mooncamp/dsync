package main

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
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

func (syn *syncer) busyWait(ctx context.Context, exportResp ExportResponse) error {
	syn.Logger.WithField("task.id", exportResp.ID()).Info("busy waiting for task success")

	reqBody := `
query {
  task(input: { id: "%s" }) {
	status
  }
}
`
	reqBody = fmt.Sprintf(reqBody, exportResp.ID())
	client := http.Client{}

	it := 0
	for {
		req, err := http.NewRequest(http.MethodPost, "http://localhost:8080/admin", strings.NewReader(reqBody))
		if err != nil {
			return fmt.Errorf("unable to form admin request: %w", err)
		}

		syn.Logger.WithField("task.id", exportResp.ID()).WithField("iteration", it).Info("requesting export task status")
		req.Header.Set("Content-Type", "application/graphql")
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("error executing admin status request: %w", err)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("unable to read admin status response: %w", err)
		}

		var statusResponse StatusResponse
		if err := json.Unmarshal(body, &statusResponse); err != nil {
			return fmt.Errorf("unable to decode admin status response: %w", err)
		}

		if statusResponse.Data.Task.Status == new(TaskStatus).Success() {
			break
		}

		if statusResponse.Data.Task.Status == new(TaskStatus).Failed() {
			return fmt.Errorf("export task failed")
		}

		time.Sleep(time.Second * 5)
		it++
	}

	syn.Logger.WithField("task.id", exportResp.ID()).Info("busy wait stop")
	return nil
}

func (syn *syncer) handleEvent(ctx context.Context, dir string) error {
	stats, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("error reading dir %s: %w", dir, err)
	}

	for _, stat := range stats {
		if strings.Contains(stat.Name(), "gql_schema") {
			continue
		}

		var fileStat os.FileInfo
		for i := 0; i < 10; i++ {
			fileStat, err = os.Stat(filepath.Join(dir, stat.Name()))
			if err != nil {
				syn.Logger.Infof("error stating file: %v", err)
				time.Sleep(time.Second * 5)
				continue
			}
		}

		file, err := os.OpenFile(filepath.Join(dir, stat.Name()), os.O_EXCL|os.O_RDONLY, 0600)
		if err != nil {
			return fmt.Errorf("error opening file %s: %w", filepath.Join(dir, stat.Name()), err)
		}

		syn.Logger.Infof("initiating sync of %s to s3://%s/%s", filepath.Join(dir, stat.Name()), syn.bucketName, syn.findObject(stat.Name()))

		h := md5.New()
		if _, err := h.Write([]byte(syn.cryptoKey)); err != nil {
			return fmt.Errorf("error generating md5 digest of encryption key: %w", err)
		}

		_, err = syn.MinioClient.PutObject(
			ctx,
			syn.bucketName,
			syn.findObject(stat.Name()), file, fileStat.Size(),
			minio.PutObjectOptions{ServerSideEncryption: syn.SSEC},
		)
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
