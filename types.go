package regrid

import (
	"errors"
	"hash"
	"time"

	r "github.com/dancannon/gorethink"
)

var (
	ErrRevisionNotExist = errors.New("revision does not exist")
	ErrHashMismatch     = errors.New("sha256 hash mismatch")
)

type Status string

const (
	StatusUnknown    Status = ""
	StatusIncomplete Status = "Incomplete"
	StatusComplete   Status = "Complete"
	StatusDeleted    Status = "Deleted"
)

type File struct {
	// Internal fields used for both reading/writing
	bucket *Bucket
	hash   hash.Hash

	// Internal fields used for reading
	cursor *r.Cursor
	buf    []byte
	opened bool

	// Internal fields used for writing
	num int

	ID         string                 `gorethink:"id,omitempty"`
	Filename   string                 `gorethink:"filename"`
	Status     Status                 `gorethink:"status"`
	Length     int                    `gorethink:"length"`
	ChunkSize  int                    `gorethink:"chunkSize"`
	FinishedAt time.Time              `gorethink:"finishedAt"`
	StartedAt  time.Time              `gorethink:"startedAt"`
	DeletedAt  time.Time              `gorethink:"deletedAt"`
	Sha256     string                 `gorethink:"sha256"`
	Metadata   map[string]interface{} `gorethink:"metadata"`
}

func (f *File) Close() error {
	if f.Status == StatusIncomplete {
		return f.closeWrite()
	} else {
		return f.closeRead()
	}
}

type Chunk struct {
	ID     string `gorethink:"id,omitempty"`
	FileID string `gorethink:"file_id"`
	Num    int    `gorethink:"num"`
	Data   []byte `gorethink:"data"`
}
