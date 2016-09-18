package regrid

import (
	r "github.com/dancannon/gorethink"
)

func (b *Bucket) Delete(id string) error {
	return r.DB(b.databaseName).Table(b.filesTable).Get(id).Update(map[string]interface{}{
		"status": StatusDeleted,
	}).Exec(b.session)
}

func (b *Bucket) Rename(id, filename string) error {
	return r.DB(b.databaseName).Table(b.filesTable).Get(id).Update(map[string]interface{}{
		"filename": filename,
	}).Exec(b.session)
}

func (b *Bucket) ReplaceMetadata(id string, metadata map[string]interface{}) error {
	return r.DB(b.databaseName).Table(b.filesTable).Get(id).Update(map[string]interface{}{
		"metadata": metadata,
	}).Exec(b.session)
}
