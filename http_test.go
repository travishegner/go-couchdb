package couchdb_test

import (
	"context"
	. "net/http"
	"testing"

	couchdb "github.com/travishegner/go-couchdb"
)

type testauth struct{ called bool }

func (a *testauth) AddAuth(context.Context, *Request, *couchdb.Transport) {
	a.called = true
}
func (a *testauth) UpdateAuth(*Response) {}

func TestClientSetAuth(t *testing.T) {
	c := newTestClient(t)
	c.Handle("HEAD /", func(resp ResponseWriter, req *Request) {})

	auth := new(testauth)
	c.SetAuth(auth)
	if err := c.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !auth.called {
		t.Error("AddAuth was not called")
	}

	auth.called = false
	c.SetAuth(nil)
	if err := c.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	if auth.called {
		t.Error("AddAuth was called after removing Auth instance")
	}
}
