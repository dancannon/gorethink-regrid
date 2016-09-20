package regrid

import (
	"io"
	"os"
	"testing"

	r "github.com/dancannon/gorethink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketCreate(t *testing.T) {
	t.Run("Defaults", func(t *testing.T) {
		bucket := New(session, BucketOptions{
			DatabaseName: db,
			BucketName:   "write",
		})
		require.Nil(t, bucket.Init())

		t.Run("lipsum.txt", func(t *testing.T) {
			dst, err := bucket.Create("/docs/lipsum.txt", nil)
			require.Nil(t, err)

			src, err := os.Open("files/lipsum.txt")
			require.Nil(t, err)

			_, err = io.Copy(dst, src)
			assert.Nil(t, err)
			assert.Nil(t, dst.Close())
			assert.Nil(t, src.Close())

			cur, err := r.DB(db).Table("write_files").Filter(map[string]interface{}{
				"filename": "/docs/lipsum.txt",
			}).Nth(0).Without("finishedAt", "startedAt", "id").Default(nil).Run(session)
			require.Nil(t, err)

			var f File
			assert.Nil(t, cur.One(&f))

			assert.Equal(t, 261120, f.ChunkSize)
			assert.Equal(t, "/docs/lipsum.txt", f.Filename)
			assert.Equal(t, 1417, f.Length)
			assert.Equal(t, "1748f5745c3ef44ba4e1f212069f6e90e29d61bdd320a48c0b06e1255864ed4f", f.Sha256)
			assert.Equal(t, StatusComplete, f.Status)
		})
	})
	t.Run("SmallChunks", func(t *testing.T) {
		bucket := New(session, BucketOptions{
			DatabaseName:   db,
			BucketName:     "write_small_chunks",
			ChunkSizeBytes: 100,
		})
		require.Nil(t, bucket.Init())

		t.Run("lipsum.txt", func(t *testing.T) {
			dst, err := bucket.Create("/docs/lipsum.txt", nil)
			require.Nil(t, err)

			src, err := os.Open("files/lipsum.txt")
			require.Nil(t, err)

			_, err = io.Copy(dst, src)
			assert.Nil(t, err)
			assert.Nil(t, dst.Close())
			assert.Nil(t, src.Close())

			cur, err := r.DB(db).Table("write_small_chunks_files").Filter(map[string]interface{}{
				"filename": "/docs/lipsum.txt",
			}).Nth(0).Without("finishedAt", "startedAt", "id").Default(nil).Run(session)
			require.Nil(t, err)

			var f File
			assert.Nil(t, cur.One(&f))

			assert.Equal(t, 100, f.ChunkSize)
			assert.Equal(t, "/docs/lipsum.txt", f.Filename)
			assert.Equal(t, 1417, f.Length)
			assert.Equal(t, "1748f5745c3ef44ba4e1f212069f6e90e29d61bdd320a48c0b06e1255864ed4f", f.Sha256)
			assert.Equal(t, StatusComplete, f.Status)
		})
	})
}
