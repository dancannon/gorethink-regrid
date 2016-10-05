# ReGrid for GoRethink

[![GitHub tag](https://img.shields.io/github/tag/dancannon/gorethink-regrid.svg?style=flat)](https://github.com/dancannon/gorethink-regrid/releases)
[![GoDoc](https://godoc.org/github.com/dancannon/gorethink-regrid?status.png)](https://godoc.org/github.com/dancannon/gorethink-regrid)
[![No Maintenance Intended](http://unmaintained.tech/badge.svg)](http://unmaintained.tech/)

ReGrid is a method of storing large files inside a RethinkDB database, this library implements the [ReGrid](https://github.com/internalfx/regrid/) specification in Go.

### Features

- **Reliable** - Files are replicated across the cluster, benefiting from RethinkDB's automatic failover.
- **Scalable** - Easily store large files in RethinkDB, distributed across the cluster.
- **Consistent** - Sha256 hashes are calculated when the file is written, and verified when read back out.
- **Realtime** - Watch the filesystem for changes and be notified immediately.

The [ReGrid spec](https://github.com/internalfx/regrid-spec) is an open specification free for anyone to implement and use.

## Installation

```
go get -u github.com/dancannon/gorethink-regrid
```

## Usage

```go
package main

import (
    "fmt"
    "log"

    r "gopkg.in/dancannon/gorethink.v2"
    "github.com/dancannon/gorethink-regrid"
)

// Upload the local file "files/lipsum.txt" to RethinkDB under the filename
// "/docs/lipsum.txt"
func Example() {
    session, err := r.Connect(r.ConnectOpts{
        Address: url,
    })
    if err != nil {
        log.Fatalln(err)
    }

    bucket := regrid.New(session, regrid.BucketOptions{
        BucketName:   "example",
    })
    if err := bucket.Init(); err != nil {
        log.Fatalln(err)
    }

    dst, err := bucket.Create("/docs/lipsum.txt", nil)
    if err != nil {
        log.Fatalln(err)
    }
    defer dst.Close()

    src, err := os.Open("files/lipsum.txt")
    if err != nil {
        log.Fatalln(err)
    }
    defer src.Close()

    _, err = io.Copy(dst, src)
    if err != nil {
        log.Fatalln(err)
    }
}
```

## Notes

Apologies for the lack of documentation however due to the closure of RethinkDB I have decided to halt the development of this library.
