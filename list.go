package regrid

import r "github.com/dancannon/gorethink"

func (b *Bucket) ListRegex(pattern string, skip, limit int, reverse bool) ([]*FileInfo, error) {
	query := r.DB(b.databaseName).Table(b.filesTable).Between(
		[]interface{}{StatusComplete, r.MinVal},
		[]interface{}{StatusComplete, r.MaxVal},
	).OptArgs(r.BetweenOpts{
		Index: fileIndexName,
	})

	if reverse {
		query = query.OrderBy(r.OrderByOpts{Index: r.Desc(fileIndexName)})
	} else {
		query = query.OrderBy(r.OrderByOpts{Index: r.Asc(fileIndexName)})
	}

	query = query.Filter(r.Row.Field("filename").Match(pattern))

	if skip > 0 {
		query = query.Skip(skip)
	}
	if limit > 0 {
		query = query.Limit(limit)
	}

	cursor, err := query.Run(b.session)
	if err != nil {
		return nil, err
	}

	var files []*FileInfo
	if err := cursor.All(&files); err != nil {
		return nil, err
	}

	for _, f := range files {
		f.bucket = b
	}

	return files, nil
}

func (b *Bucket) ListFilename(filename string, skip, limit int, reverse bool) ([]*FileInfo, error) {
	query := r.DB(b.databaseName).Table(b.filesTable).Between(
		[]interface{}{StatusComplete, filename, r.MinVal},
		[]interface{}{StatusComplete, filename, r.MaxVal},
	).OptArgs(r.BetweenOpts{
		Index: fileIndexName,
	})

	if reverse {
		query = query.OrderBy(r.OrderByOpts{Index: r.Desc(fileIndexName)})
	} else {
		query = query.OrderBy(r.OrderByOpts{Index: r.Asc(fileIndexName)})
	}

	if skip > 0 {
		query = query.Skip(skip)
	}
	if limit > 0 {
		query = query.Limit(limit)
	}

	cursor, err := query.Run(b.session)
	if err != nil {
		return nil, err
	}

	var files []*FileInfo
	if err := cursor.All(&files); err != nil {
		return nil, err
	}

	for _, f := range files {
		f.bucket = b
	}

	return files, nil
}

func (b *Bucket) ListMetadata(metadata map[string]interface{}, skip, limit int) ([]*FileInfo, error) {
	query := r.DB(b.databaseName).Table(b.filesTable).Filter(map[string]interface{}{
		"metadata": metadata,
		"status":   StatusComplete,
	})

	if skip > 0 {
		query = query.Skip(skip)
	}
	if limit > 0 {
		query = query.Limit(limit)
	}

	cursor, err := query.Run(b.session)
	if err != nil {
		return nil, err
	}

	var files []*FileInfo
	if err := cursor.All(&files); err != nil {
		return nil, err
	}

	for _, f := range files {
		f.bucket = b
	}

	return files, nil
}
