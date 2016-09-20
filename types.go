package regrid

import (
	"errors"
	"hash"
	"time"

	r "github.com/dancannon/gorethink"
)

var (
	ErrInvalid          = errors.New("invalid argument")
	ErrNotExist         = errors.New("file does not exist")
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

type FileInfo struct {
	bucket *Bucket

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

func (fi *FileInfo) Open() (*File, error) {
	f := &File{
		FileInfo: fi,
		bucket:   fi.bucket,
	}
	if err := f.open(); err != nil {
		return nil, err
	}

	return f, nil
}

type File struct {
	*FileInfo

	// Internal fields used for both reading/writing
	bucket *Bucket
	hash   hash.Hash

	// Internal fields used for reading
	cursor *r.Cursor
	buf    []byte
	opened bool

	// Internal fields used for writing
	num int
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

type FileInfoChange struct {
	NewVal *FileInfo `gorethink:"new_val"`
	OldVal *FileInfo `gorethink:"old_val"`
}
