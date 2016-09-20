package regrid

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWatch(t *testing.T) {
	bucket := New(session, BucketOptions{
		DatabaseName: db,
		BucketName:   "watch",
	})
	require.Nil(t, bucket.Init())

	t.Run("Regex", func(t *testing.T) {
		cur, err := bucket.WatchRegex("^/images")
		require.Nil(t, err)

		// Upload file
		dst, err := bucket.Create("/images/saturnV.jpg", nil)
		require.Nil(t, err)

		src, err := os.Open("files/saturnV.jpg")
		require.Nil(t, err)

		_, err = io.Copy(dst, src)
		require.Nil(t, err)
		require.Nil(t, dst.Close())
		require.Nil(t, src.Close())

		// Read from changefeed/cursor
		var change *FileInfoChange
		require.True(t, cur.Next(&change))
		assert.Nil(t, cur.Err())

		assert.Equal(t, "/images/saturnV.jpg", change.NewVal.Filename)
		assert.Equal(t, StatusComplete, change.NewVal.Status)

		assert.Nil(t, cur.Close())
	})

	t.Run("Filename", func(t *testing.T) {
		cur, err := bucket.WatchFilename("/docs/empty.txt")
		require.Nil(t, err)

		// Upload file
		dst, err := bucket.Create("/docs/empty.txt", nil)
		require.Nil(t, err)

		src, err := os.Open("files/empty.txt")
		require.Nil(t, err)

		_, err = io.Copy(dst, src)
		require.Nil(t, err)
		require.Nil(t, dst.Close())
		require.Nil(t, src.Close())

		// Read from changefeed/cursor
		var change *FileInfoChange
		require.True(t, cur.Next(&change))
		assert.Nil(t, cur.Err())

		assert.Equal(t, "/docs/empty.txt", change.NewVal.Filename)
		assert.Equal(t, StatusComplete, change.NewVal.Status)

		assert.Nil(t, cur.Close())
	})

	t.Run("Metadata", func(t *testing.T) {
		cur, err := bucket.WatchMetadata(map[string]interface{}{
			"rocket": true,
		})

		// Upload file
		dst, err := bucket.Create("/images/enterprise.jpg", map[string]interface{}{
			"rocket": true,
		})
		require.Nil(t, err)

		src, err := os.Open("files/enterprise.jpg")
		require.Nil(t, err)

		_, err = io.Copy(dst, src)
		require.Nil(t, err)
		require.Nil(t, dst.Close())
		require.Nil(t, src.Close())

		// Read from changefeed/cursor
		var change *FileInfoChange
		require.True(t, cur.Next(&change))
		assert.Nil(t, cur.Err())

		assert.Equal(t, "/images/enterprise.jpg", change.NewVal.Filename)
		assert.Equal(t, StatusComplete, change.NewVal.Status)

		assert.Nil(t, cur.Close())
	})
}
