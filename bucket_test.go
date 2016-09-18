package regrid

import (
	"math/rand"
	"strconv"
	"testing"

	r "github.com/dancannon/gorethink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketInit(t *testing.T) {
	t.Run("Defaults", func(t *testing.T) {
		bucket := New(session, BucketOptions{
			DatabaseName: db,
		})

		testBucketInit(t, bucket)
	})

	t.Run("CustomName", func(t *testing.T) {
		bucketName := strconv.FormatInt(int64(rand.Int()), 36)
		bucket := New(session, BucketOptions{
			DatabaseName: db,
			BucketName:   bucketName,
		})

		testBucketInit(t, bucket)
	})
}

func testBucketInit(t *testing.T, bucket *Bucket) {
	err := bucket.Init()
	require.Nil(t, err)

	cur, err := r.DB(db).TableList().Run(session)
	require.Nil(t, err)

	tables := []string{}
	err = cur.All(&tables)
	require.Nil(t, err)

	assert.Contains(t, tables, bucket.bucketName+"_files")
	assert.Contains(t, tables, bucket.bucketName+"_chunks")

	// Assert indexes are created correctly
	type indexStatus struct {
		Index string
		Query string
	}

	var fileIndex, chunkIndex *indexStatus
	var fileIndexes, chunkIndexes []*indexStatus

	// Find files index
	cur, err = r.DB(db).Table(bucket.bucketName + "_files").IndexStatus().Run(session)
	require.Nil(t, err)
	require.Nil(t, cur.All(&chunkIndexes))

	for _, index := range chunkIndexes {
		if index.Index == "file_ix" {
			fileIndex = index
		}
	}

	// Find chunks index
	cur, err = r.DB(db).Table(bucket.bucketName + "_chunks").IndexStatus().Run(session)
	require.Nil(t, err)
	require.Nil(t, cur.All(&fileIndexes))

	for _, index := range fileIndexes {
		if index.Index == "chunk_ix" {
			chunkIndex = index
		}
	}

	if assert.NotNil(t, fileIndex) {
		assert.Regexp(
			t,
			`indexCreate\('file_ix', function\(var[0-9]+\) { return r\.expr\(\[r\.row\("status"\), r\.row\("filename"\), r\.row\("finishedAt"\)\]\); }\)`,
			fileIndex.Query,
		)
	}
	if assert.NotNil(t, chunkIndex) {
		assert.Regexp(
			t,
			`indexCreate\('chunk_ix', function\(var[0-9]+\) { return r\.expr\(\[r\.row\("file_id"\), r\.row\("num"\)\]\); }\)`,
			chunkIndex.Query,
		)
	}
}
