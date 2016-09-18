package regrid

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"

	r "github.com/dancannon/gorethink"
)

func (b *Bucket) Open(filename string) (*File, error) {
	return b.OpenRevision(filename, -1)
}

func (b *Bucket) OpenRevision(filename string, revision int) (*File, error) {
	var revSteps int

	query := r.DB(b.databaseName).Table(b.filesTable).Between(
		[]interface{}{StatusComplete, filename, r.MinVal},
		[]interface{}{StatusComplete, filename, r.MaxVal},
	).OptArgs(r.BetweenOpts{
		Index: fileIndexName,
	})
	if revision >= 0 {
		revSteps = revision
		query = query.OrderBy(r.OrderByOpts{Index: r.Asc(fileIndexName)})
	} else {
		revSteps = (revision * -1) - 1
		query = query.OrderBy(r.OrderByOpts{Index: r.Desc(fileIndexName)})
	}

	var files []*File
	cur, err := query.Run(b.session)
	if err != nil {
		return nil, err
	}

	if err := cur.All(&files); err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, os.ErrNotExist
	}
	if len(files) < (revSteps + 1) {
		return nil, ErrRevisionNotExist
	}

	file := files[revSteps]

	file.bucket = b

	return file, nil
}

func (b *Bucket) OpenID(id string) (*File, error) {
	cur, err := r.DB(b.databaseName).Table(b.filesTable).Get(id).Run(b.session)
	if err != nil {
		return nil, err
	}

	if cur.IsNil() {
		return nil, os.ErrNotExist
	}

	var file *File
	if err := cur.One(&file); err != nil {
		return nil, err
	}

	file.bucket = b

	return file, nil
}

func (f *File) Read(b []byte) (n int, err error) {
	if f == nil || f.bucket == nil {
		return 0, os.ErrInvalid
	}
	if !f.opened {
		if err := f.open(); err != nil {
			return 0, err
		}
	}
	n, err = f.read(b)

	// If we have finished reading all the chunks then compare the hash values
	if n == 0 && err == nil {
		sha256 := hex.EncodeToString(f.hash.Sum(nil))
		if sha256 != f.Sha256 {
			return 0, ErrHashMismatch
		}
	}
	if n == 0 && len(b) > 0 && err == nil {
		return 0, io.EOF
	}
	return n, err
}

func (f *File) open() (err error) {
	f.opened = true
	f.hash = sha256.New()
	f.cursor, err = r.DB(f.bucket.databaseName).Table(f.bucket.chunksTable).Between(
		[]interface{}{f.ID, r.MinVal},
		[]interface{}{f.ID, r.MaxVal},
	).OptArgs(r.BetweenOpts{
		Index: chunkIndexName,
	}).OrderBy(r.OrderByOpts{
		Index: chunkIndexName,
	}).Run(f.bucket.session)

	return
}

func (f *File) closeRead() error {
	return f.cursor.Close()
}

func (f *File) read(b []byte) (n int, err error) {
	for {
		if n >= len(b) {
			return n, nil
		}
		if len(f.buf) > 0 {
			n, f.buf = copy(b, f.buf), nil
		} else {
			var chunk *Chunk
			more := f.cursor.Next(&chunk)
			if more {
				f.hash.Write(chunk.Data)
				f.buf = chunk.Data
			}

			err = f.cursor.Err()
			if err != nil {
				return 0, err
			}

			if !more && len(f.buf) == 0 {
				return n, nil
			}
		}
	}
}
