package regrid

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketOpen(t *testing.T) {
	bucket := New(session, BucketOptions{
		DatabaseName: db,
		BucketName:   "open",
	})
	require.Nil(t, bucket.Init())

	t.Run("saturnV.jpg", func(t *testing.T) {
		gridHash := sha256.New()
		fileHash := sha256.New()

		// Upload file
		dst, err := bucket.Create("/images/saturnV.jpg", nil)
		require.Nil(t, err)

		src, err := os.Open("files/saturnV.jpg")
		require.Nil(t, err)

		_, err = io.Copy(io.MultiWriter(dst, fileHash), src)
		require.Nil(t, err)
		require.Nil(t, dst.Close())
		require.Nil(t, src.Close())

		// Download file
		file, err := bucket.Open("/images/saturnV.jpg")
		require.Nil(t, err)

		_, err = io.Copy(gridHash, file)
		require.Nil(t, err)

		assert.Equal(t, hex.EncodeToString(fileHash.Sum(nil)), hex.EncodeToString(gridHash.Sum(nil)))
	})

	t.Run("ErrNotExists", func(t *testing.T) {
		file, err := bucket.Open("/images/notfound.jpg")
		assert.Nil(t, file)
		assert.Equal(t, ErrNotExist, err)
	})
}

func TestBucketOpenRevision(t *testing.T) {
	bucket := New(session, BucketOptions{
		DatabaseName: db,
		BucketName:   "open_revision",
	})
	require.Nil(t, bucket.Init())

	t.Run("Revision1", func(t *testing.T) {
		// Upload revision 1
		dst, err := bucket.Create("/docs/document.txt", nil)
		require.Nil(t, err)

		src, err := os.Open("files/empty.txt")
		require.Nil(t, err)

		_, err = io.Copy(dst, src)
		require.Nil(t, err)
		require.Nil(t, dst.Close())
		require.Nil(t, src.Close())

		// Download revision 1
		file, err := bucket.OpenRevision("/docs/document.txt", 0)
		require.Nil(t, err)

		assert.Equal(t, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", file.Sha256)
	})

	t.Run("Revision2", func(t *testing.T) {
		// Upload revision 2
		dst, err := bucket.Create("/docs/document.txt", nil)
		require.Nil(t, err)

		src, err := os.Open("files/lipsum.txt")
		require.Nil(t, err)

		_, err = io.Copy(dst, src)
		require.Nil(t, err)
		require.Nil(t, dst.Close())
		require.Nil(t, src.Close())

		// Download revision 2
		file, err := bucket.OpenRevision("/docs/document.txt", 1)
		require.Nil(t, err)

		assert.Equal(t, "1748f5745c3ef44ba4e1f212069f6e90e29d61bdd320a48c0b06e1255864ed4f", file.Sha256)
	})

	t.Run("ErrRevisionNotExist", func(t *testing.T) {
		file, err := bucket.OpenRevision("/docs/document.txt", 2)
		assert.Nil(t, file)
		assert.Equal(t, ErrRevisionNotExist, err)
	})

	t.Run("ErrNotExists", func(t *testing.T) {
		file, err := bucket.OpenRevision("/images/notfound.jpg", 1)
		assert.Nil(t, file)
		assert.Equal(t, ErrNotExist, err)
	})
}

func TestBucketOpenID(t *testing.T) {
	bucket := New(session, BucketOptions{
		DatabaseName: db,
		BucketName:   "open_id",
	})
	require.Nil(t, bucket.Init())

	t.Run("saturnV.jpg", func(t *testing.T) {
		gridHash := sha256.New()
		fileHash := sha256.New()

		// Upload file
		dst, err := bucket.Create("/images/saturnV.jpg", nil)
		require.Nil(t, err)

		src, err := os.Open("files/saturnV.jpg")
		require.Nil(t, err)

		_, err = io.Copy(io.MultiWriter(dst, fileHash), src)
		require.Nil(t, err)
		require.Nil(t, dst.Close())
		require.Nil(t, src.Close())

		// Download file
		file, err := bucket.OpenID(dst.ID)
		require.Nil(t, err)

		_, err = io.Copy(gridHash, file)
		require.Nil(t, err)

		assert.Equal(t, hex.EncodeToString(fileHash.Sum(nil)), hex.EncodeToString(gridHash.Sum(nil)))
	})

	t.Run("ErrNotExists", func(t *testing.T) {
		file, err := bucket.OpenID("notfound")
		assert.Nil(t, file)
		assert.Equal(t, ErrNotExist, err)
	})
}
