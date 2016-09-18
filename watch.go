package regrid

import r "github.com/dancannon/gorethink"

func (b *Bucket) WatchRegex(pattern string) (*r.Cursor, error) {
	return r.DB(b.databaseName).Table(b.filesTable).Filter(r.And(
		r.Row.Field("status").Eq(StatusComplete),
		r.Row.Field("filename").Match(pattern),
	)).Changes().Run(b.session)
}

func (b *Bucket) WatchFilename(filename string) (*r.Cursor, error) {
	return r.DB(b.databaseName).Table(b.filesTable).Between(
		[]interface{}{StatusComplete, filename, r.MinVal},
		[]interface{}{StatusComplete, filename, r.MaxVal},
	).OptArgs(r.BetweenOpts{
		Index: fileIndexName,
	}).Changes().Run(b.session)
}

func (b *Bucket) WatchMetadata(metadata map[string]interface{}) (*r.Cursor, error) {
	return r.DB(b.databaseName).Table(b.filesTable).Filter(r.And(
		r.Row.Field("status").Eq(StatusComplete),
		r.Row.Field("metadata").Eq(metadata),
	)).Changes().Run(b.session)
}
