package regrid

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"

	r "github.com/dancannon/gorethink"
)

func (b *Bucket) Create(filename string, metadata map[string]interface{}) (*File, error) {
	newFile := &File{
		Filename:  filename,
		ChunkSize: b.chunkSizeBytes,
		StartedAt: time.Now(),
		Status:    StatusIncomplete,
		Metadata:  metadata,
	}

	cur, err := r.DB(b.databaseName).Table(b.filesTable).Insert(newFile).OptArgs(r.InsertOpts{
		ReturnChanges: true,
	}).Run(b.session)
	if err != nil {
		return nil, err
	}

	var rsp struct {
		Changes []struct {
			NewVal *File `gorethink:"new_val"`
		}
	}
	if err := cur.One(&rsp); err != nil {
		return nil, err
	}

	if len(rsp.Changes) != 1 {
		return nil, fmt.Errorf("Error opening file for writing")
	}

	file := rsp.Changes[0].NewVal
	file.bucket = b
	file.hash = sha256.New()

	return file, nil
}

func (f *File) Write(b []byte) (n int, err error) {
	if f == nil {
		return 0, os.ErrInvalid
	}
	n, err = f.write(b)
	if n < 0 {
		n = 0
	}
	if n != len(b) {
		err = io.ErrShortWrite
	}
	return n, err
}

func (f *File) closeWrite() error {
	return r.DB(f.bucket.databaseName).Table(f.bucket.filesTable).Get(f.ID).Update(map[string]interface{}{
		"finishedAt": time.Now(),
		"status":     StatusComplete,
		"sha256":     hex.EncodeToString(f.hash.Sum(nil)),
		"length":     f.Length,
	}).Exec(f.bucket.session)
}

func (f *File) write(b []byte) (n int, err error) {
	for {
		bcap := b
		if len(bcap) > f.bucket.chunkSizeBytes {
			bcap = bcap[:f.bucket.chunkSizeBytes]
		}
		m, err := f.writeChunk(b)
		n += m

		// If the chunk was partially written then assume it stopped early for
		// reasons that are uninteresting to the caller, and try again.
		if 0 < m && m < len(bcap) {
			b = b[m:]
			continue
		}

		if len(bcap) != len(b) && err == nil {
			b = b[m:]
			continue
		}

		return n, err
	}
}

func (f *File) writeChunk(b []byte) (n int, err error) {
	if err := r.DB(f.bucket.databaseName).Table(f.bucket.chunksTable).Insert(Chunk{
		FileID: f.ID,
		Num:    f.num,
		Data:   b,
	}).Exec(f.bucket.session); err != nil {
		return 0, err
	}

	f.num++
	f.Length += len(b)
	if _, err = f.hash.Write(b); err != nil {
		return 0, err
	}

	return len(b), nil
}
