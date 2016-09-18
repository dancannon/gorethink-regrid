package regrid

import r "github.com/dancannon/gorethink"

const (
	fileIndexName  = "file_ix"
	chunkIndexName = "chunk_ix"
)

type BucketOptions struct {
	DatabaseName   string
	BucketName     string
	ChunkSizeBytes int
}

type Bucket struct {
	session *r.Session

	databaseName, bucketName string
	chunkSizeBytes           int
	filesTable, chunksTable  string
}

func New(session *r.Session, options BucketOptions) *Bucket {
	if options.BucketName == "" {
		options.BucketName = "fs"
	}
	if options.ChunkSizeBytes == 0 {
		options.ChunkSizeBytes = 1024 * 255
	}

	return &Bucket{
		session: session,

		databaseName:   options.DatabaseName,
		bucketName:     options.BucketName,
		chunkSizeBytes: options.ChunkSizeBytes,
		filesTable:     options.BucketName + "_files",
		chunksTable:    options.BucketName + "_chunks",
	}
}

func (b *Bucket) Init() error {
	if err := b.createTables(); err != nil {
		return err
	}
	if err := b.createFilesIndexes(); err != nil {
		return err
	}
	if err := b.createChunksIndexes(); err != nil {
		return err
	}

	return nil
}

func (b *Bucket) createTables() error {
	cur, err := r.DB(b.databaseName).TableList().Run(b.session)
	if err != nil {
		return err
	}

	tables := []string{}
	if err := cur.All(&tables); err != nil {
		return err
	}

	filesTableExists := false
	chunksTableExists := false
	for _, table := range tables {
		if table == b.filesTable {
			filesTableExists = true
		}
		if table == b.chunksTable {
			chunksTableExists = true
		}
	}

	if !filesTableExists {
		if err := r.DB(b.databaseName).TableCreate(b.filesTable).Exec(b.session); err != nil {
			return err
		}
	}
	if !chunksTableExists {
		if err := r.DB(b.databaseName).TableCreate(b.chunksTable).Exec(b.session); err != nil {
			return err
		}
	}

	return nil
}

func (b *Bucket) createFilesIndexes() error {
	cur, err := r.DB(b.databaseName).Table(b.filesTable).IndexList().Run(b.session)
	if err != nil {
		return err
	}

	indexes := []string{}
	if err := cur.All(&indexes); err != nil {
		return err
	}

	indexExists := false
	for _, index := range indexes {
		if index == fileIndexName {
			indexExists = true
		}
	}

	if !indexExists {
		if err := r.DB(b.databaseName).Table(b.filesTable).IndexCreateFunc(fileIndexName, []interface{}{
			r.Row.AtIndex("status"), r.Row.AtIndex("filename"), r.Row.AtIndex("finishedAt"),
		}).Exec(b.session); err != nil {
			return err
		}
	}

	if err := r.DB(b.databaseName).Table(b.filesTable).IndexWait(fileIndexName).Exec(b.session); err != nil {
		return err
	}

	return nil
}

func (b *Bucket) createChunksIndexes() error {
	cur, err := r.DB(b.databaseName).Table(b.chunksTable).IndexList().Run(b.session)
	if err != nil {
		return err
	}

	indexes := []string{}
	if err := cur.All(&indexes); err != nil {
		return err
	}

	indexExists := false
	for _, index := range indexes {
		if index == chunkIndexName {
			indexExists = true
		}
	}

	if !indexExists {
		if err := r.DB(b.databaseName).Table(b.chunksTable).IndexCreateFunc(chunkIndexName, []interface{}{
			r.Row.AtIndex("file_id"), r.Row.AtIndex("num"),
		}).Exec(b.session); err != nil {
			return err
		}
	}

	if err := r.DB(b.databaseName).Table(b.chunksTable).IndexWait(chunkIndexName).Exec(b.session); err != nil {
		return err
	}

	return nil
}
