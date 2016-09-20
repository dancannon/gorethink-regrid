package regrid

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestList(t *testing.T) {
	bucket := New(session, BucketOptions{
		DatabaseName: db,
		BucketName:   "list",
	})
	require.Nil(t, bucket.Init())

	srcFiles, err := ioutil.ReadDir("files")
	require.Nil(t, err)

	for _, f := range srcFiles {
		filename := ""
		metadata := map[string]interface{}{}
		if strings.HasSuffix(f.Name(), ".jpg") {
			filename = "/images/" + f.Name()
		} else {
			filename = "/docs/" + f.Name()
		}
		if strings.Contains(f.Name(), "enterprise.jpg") || strings.Contains(f.Name(), "saturnV.jpg") {
			metadata["rocket"] = true
		}
		// Upload file
		dst, err := bucket.Create(filename, metadata)
		require.Nil(t, err)

		src, err := os.Open("files/" + f.Name())
		require.Nil(t, err)

		_, err = io.Copy(dst, src)
		require.Nil(t, err)
		require.Nil(t, dst.Close())
		require.Nil(t, src.Close())
	}

	t.Run("Regex", func(t *testing.T) {
		files, err := bucket.ListRegex("^/images", 0, 0, false)
		require.Nil(t, err)

		if assert.Len(t, files, 4) {
			assert.Equal(t, "/images/earth.jpg", files[0].Filename)
			assert.Equal(t, "/images/enterprise.jpg", files[1].Filename)
			assert.Equal(t, "/images/saturnV.jpg", files[2].Filename)
			assert.Equal(t, "/images/venus.jpg", files[3].Filename)
		}
	})

	t.Run("RegexReverse", func(t *testing.T) {
		files, err := bucket.ListRegex("^/images", 0, 0, true)
		require.Nil(t, err)

		if assert.Len(t, files, 4) {
			assert.Equal(t, "/images/venus.jpg", files[0].Filename)
			assert.Equal(t, "/images/saturnV.jpg", files[1].Filename)
			assert.Equal(t, "/images/enterprise.jpg", files[2].Filename)
			assert.Equal(t, "/images/earth.jpg", files[3].Filename)
		}
	})

	t.Run("RegexPaginate", func(t *testing.T) {
		files, err := bucket.ListRegex("^/images", 1, 1, false)
		require.Nil(t, err)

		if assert.Len(t, files, 1) {
			assert.Equal(t, "/images/enterprise.jpg", files[0].Filename)
		}
	})

	t.Run("Filename", func(t *testing.T) {
		files, err := bucket.ListFilename("/docs/empty.txt", 0, 0, false)
		require.Nil(t, err)

		if assert.Len(t, files, 1) {
			assert.Equal(t, "/docs/empty.txt", files[0].Filename)
		}
	})

	t.Run("Metadata", func(t *testing.T) {
		files, err := bucket.ListMetadata(map[string]interface{}{
			"rocket": true,
		}, 0, 0)
		require.Nil(t, err)

		assert.Len(t, files, 2)
	})

	t.Run("Open", func(t *testing.T) {
		files, err := bucket.ListFilename("/images/saturnV.jpg", 0, 0, false)
		require.Nil(t, err)

		if assert.Len(t, files, 1) {
			gridHash := sha256.New()

			file, err := files[0].Open()
			require.Nil(t, err)

			_, err = io.Copy(gridHash, file)
			assert.Nil(t, file.Close())

			require.Nil(t, err)
			assert.Equal(t, file.Sha256, hex.EncodeToString(gridHash.Sum(nil)))
		}
	})
}
