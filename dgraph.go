package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

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

		rdfWriter := syn.Client.Bucket(syn.bucketName).Object(syn.findObject(stat.Name())).NewWriter(ctx)

		_, err = io.Copy(rdfWriter, file)
		if err != nil {
			return fmt.Errorf("error sending file: %w", err)
		}

		if err := rdfWriter.Close(); err != nil {
			return fmt.Errorf("error flushing %s to gs://%s/%s: %w", filepath.Join(dir, stat.Name()), syn.bucketName, syn.findObject(stat.Name()), err)
		}

		syn.Logger.Printf("synchronized %s to gs://%s/%s", filepath.Join(dir, stat.Name()), syn.bucketName, syn.findObject(stat.Name()))
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
