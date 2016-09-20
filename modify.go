package regrid

import r "github.com/dancannon/gorethink"

func (b *Bucket) Delete(id string) error {
	rsp, err := r.DB(b.databaseName).Table(b.filesTable).Get(id).Update(map[string]interface{}{
		"status": StatusDeleted,
	}).RunWrite(b.session)
	if err != nil {
		return err
	}

	if rsp.Replaced == 0 && rsp.Unchanged == 0 {
		return ErrNotExist
	}

	return nil
}

func (b *Bucket) HardDelete(id string) error {
	rsp, err := r.DB(b.databaseName).Table(b.filesTable).Get(id).Delete().RunWrite(b.session)
	if err != nil {
		return err
	}

	if rsp.Deleted == 0 {
		return ErrNotExist
	}

	err = r.DB(b.databaseName).Table(b.chunksTable).Between(
		[]interface{}{id, r.MinVal},
		[]interface{}{id, r.MaxVal},
	).OptArgs(r.BetweenOpts{
		Index: chunkIndexName,
	}).Delete().Exec(b.session)
	if err != nil {
		return err
	}

	return nil
}

func (b *Bucket) Rename(id, filename string) error {
	rsp, err := r.DB(b.databaseName).Table(b.filesTable).Get(id).Update(map[string]interface{}{
		"filename": filename,
	}).RunWrite(b.session)
	if err != nil {
		return err
	}

	if rsp.Replaced == 0 && rsp.Unchanged == 0 {
		return ErrNotExist
	}

	return nil
}

func (b *Bucket) ReplaceMetadata(id string, metadata map[string]interface{}) error {
	rsp, err := r.DB(b.databaseName).Table(b.filesTable).Get(id).Update(map[string]interface{}{
		"metadata": metadata,
	}).RunWrite(b.session)
	if err != nil {
		return err
	}

	if rsp.Replaced == 0 && rsp.Unchanged == 0 {
		return ErrNotExist
	}

	return nil
}
