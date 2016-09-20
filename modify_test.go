package regrid

import (
	"io"
	"os"
	"testing"

	r "github.com/dancannon/gorethink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketDelete(t *testing.T) {
	bucket := New(session, BucketOptions{
		DatabaseName: db,
		BucketName:   "delete",
	})
	require.Nil(t, bucket.Init())

	t.Run("saturnV.jpg", func(t *testing.T) {
		// Upload file
		dst, err := bucket.Create("/images/saturnV.jpg", nil)
		require.Nil(t, err)

		src, err := os.Open("files/saturnV.jpg")
		require.Nil(t, err)

		_, err = io.Copy(dst, src)
		require.Nil(t, err)
		require.Nil(t, dst.Close())
		require.Nil(t, src.Close())

		// Download file
		_, err = bucket.OpenID(dst.ID)
		require.Nil(t, err)

		// Delete file
		err = bucket.Delete(dst.ID)
		require.Nil(t, err)

		// Download file
		file, err := bucket.OpenID(dst.ID)
		assert.Nil(t, err)
		assert.Equal(t, StatusDeleted, file.Status)

		_, err = bucket.Open("/images/saturnV.jpg")
		assert.Equal(t, ErrNotExist, err)
	})

	t.Run("ErrNotExists", func(t *testing.T) {
		err := bucket.Delete("notfound")
		assert.Equal(t, ErrNotExist, err)
	})
}

func TestBucketHardDelete(t *testing.T) {
	bucket := New(session, BucketOptions{
		DatabaseName: db,
		BucketName:   "hard_delete",
	})
	require.Nil(t, bucket.Init())

	t.Run("saturnV.jpg", func(t *testing.T) {
		// Upload file
		dst, err := bucket.Create("/images/saturnV.jpg", nil)
		require.Nil(t, err)

		src, err := os.Open("files/saturnV.jpg")
		require.Nil(t, err)

		_, err = io.Copy(dst, src)
		require.Nil(t, err)
		require.Nil(t, dst.Close())
		require.Nil(t, src.Close())

		// Download file
		_, err = bucket.OpenID(dst.ID)
		require.Nil(t, err)

		// Delete file
		err = bucket.HardDelete(dst.ID)
		require.Nil(t, err)

		// Download file
		_, err = bucket.OpenID(dst.ID)
		assert.Equal(t, ErrNotExist, err)

		cur, err := r.DB(db).Table("hard_delete_files").Get(dst.ID).Run(session)
		assert.Nil(t, err)
		assert.True(t, cur.IsNil())

		cur, err = r.DB(db).Table("hard_delete_chunks").Between(
			[]interface{}{dst.ID, r.MinVal},
			[]interface{}{dst.ID, r.MaxVal},
		).OptArgs(r.BetweenOpts{
			Index: chunkIndexName,
		}).Run(session)
		assert.Nil(t, err)
		assert.True(t, cur.IsNil())
	})

	t.Run("ErrNotExists", func(t *testing.T) {
		err := bucket.HardDelete("notfound")
		assert.Equal(t, ErrNotExist, err)
	})
}

func TestBucketRename(t *testing.T) {
	bucket := New(session, BucketOptions{
		DatabaseName: db,
		BucketName:   "rename",
	})
	require.Nil(t, bucket.Init())

	t.Run("planet.jpg", func(t *testing.T) {
		// Upload file
		dst, err := bucket.Create("/images/saturnV.jpg", nil)
		require.Nil(t, err)

		src, err := os.Open("files/saturnV.jpg")
		require.Nil(t, err)

		_, err = io.Copy(dst, src)
		require.Nil(t, err)
		require.Nil(t, dst.Close())
		require.Nil(t, src.Close())

		// Download file
		file, err := bucket.OpenID(dst.ID)
		require.Nil(t, err)
		assert.Equal(t, "/images/saturnV.jpg", file.Filename)

		// Rename file
		err = bucket.Rename(dst.ID, "/images/planet.jpg")
		require.Nil(t, err)

		// Download file
		file, err = bucket.OpenID(dst.ID)
		require.Nil(t, err)
		assert.Equal(t, "/images/planet.jpg", file.Filename)
	})

	t.Run("saturnV.jpg", func(t *testing.T) {
		// Upload file
		dst, err := bucket.Create("/images/saturnV.jpg", nil)
		require.Nil(t, err)

		src, err := os.Open("files/saturnV.jpg")
		require.Nil(t, err)

		_, err = io.Copy(dst, src)
		require.Nil(t, err)
		require.Nil(t, dst.Close())
		require.Nil(t, src.Close())

		// Download file
		file, err := bucket.OpenID(dst.ID)
		require.Nil(t, err)
		assert.Equal(t, "/images/saturnV.jpg", file.Filename)

		// Rename file
		err = bucket.Rename(dst.ID, "/images/saturnV.jpg")
		require.Nil(t, err)

		// Download file
		file, err = bucket.OpenID(dst.ID)
		require.Nil(t, err)
		assert.Equal(t, "/images/saturnV.jpg", file.Filename)
	})

	t.Run("ErrNotExists", func(t *testing.T) {
		err := bucket.Rename("notfound", "newname")
		assert.Equal(t, ErrNotExist, err)
	})
}

func TestBucketReplaceMetadata(t *testing.T) {
	bucket := New(session, BucketOptions{
		DatabaseName: db,
		BucketName:   "replace_metadata",
	})
	require.Nil(t, bucket.Init())

	t.Run("saturnV.jpg", func(t *testing.T) {
		// Upload file
		dst, err := bucket.Create("/images/saturnV.jpg", map[string]interface{}{"foo": "bar"})
		require.Nil(t, err)

		src, err := os.Open("files/saturnV.jpg")
		require.Nil(t, err)

		_, err = io.Copy(dst, src)
		require.Nil(t, err)
		require.Nil(t, dst.Close())
		require.Nil(t, src.Close())

		// Download file
		file, err := bucket.OpenID(dst.ID)
		require.Nil(t, err)
		assert.Equal(t, "bar", file.Metadata["foo"])

		// Delete file
		err = bucket.ReplaceMetadata(dst.ID, map[string]interface{}{"foo": "baz"})
		require.Nil(t, err)

		// Download file
		file, err = bucket.OpenID(dst.ID)
		require.Nil(t, err)
		assert.Equal(t, "baz", file.Metadata["foo"])
	})

	t.Run("ErrNotExists", func(t *testing.T) {
		err := bucket.ReplaceMetadata("notfound", map[string]interface{}{"foo": "baz"})
		assert.Equal(t, ErrNotExist, err)
	})
}
