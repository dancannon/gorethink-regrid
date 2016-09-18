package regrid

import (
	"flag"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	r "github.com/dancannon/gorethink"
)

var session *r.Session
var url, db string

func init() {
	flag.Parse()
	r.SetVerbose(true)

	url = os.Getenv("RETHINKDB_URL")
	if url == "" {
		url = "localhost:28015"
	}

	db = os.Getenv("RETHINKDB_DB")
	if db == "" {
		db = "gorethink_regrid"
	}
}

func testSetup(m *testing.M) {
	var err error
	session, err = r.Connect(r.ConnectOpts{
		Address: url,
	})
	if err != nil {
		log.Fatalln(err.Error())
	}

	r.DBDrop(db).Exec(session)
	r.DBCreate(db).Exec(session)

}
func testTeardown(m *testing.M) {
	session.Close()
}

func TestMain(m *testing.M) {
	// seed randomness for use with tests
	rand.Seed(time.Now().UTC().UnixNano())

	testSetup(m)
	res := m.Run()
	testTeardown(m)

	os.Exit(res)
}
