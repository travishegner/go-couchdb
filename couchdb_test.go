package couchdb_test

import (
	"context"
	"errors"
	"io"
	. "net/http"
	"net/url"
	"regexp"
	"strconv"
	"testing"

	couchdb "github.com/travishegner/go-couchdb"
)

type roundTripperFunc func(*Request) (*Response, error)

func (f roundTripperFunc) RoundTrip(r *Request) (*Response, error) {
	return f(r)
}

func TestNewClient(t *testing.T) {
	tests := []struct {
		URL                         string
		SetAuth                     couchdb.Auth
		ExpectURL, ExpectAuthHeader string
	}{
		// No Auth
		{
			URL:       "http://127.0.0.1:5984/",
			ExpectURL: "http://127.0.0.1:5984",
		},
		{
			URL:       "http://hostname:5984/foobar?query=1",
			ExpectURL: "http://hostname:5984/foobar",
		},
		// Credentials in URL
		{
			URL:              "http://user:password@hostname:5984/",
			ExpectURL:        "http://hostname:5984",
			ExpectAuthHeader: "Basic dXNlcjpwYXNzd29yZA==",
		},
		// Credentials in URL and explicit SetAuth, SetAuth credentials win
		{
			URL:              "http://urluser:urlpassword@hostname:5984/",
			SetAuth:          couchdb.BasicAuth("user", "password"),
			ExpectURL:        "http://hostname:5984",
			ExpectAuthHeader: "Basic dXNlcjpwYXNzd29yZA==",
		},
	}

	for i, test := range tests {
		rt := roundTripperFunc(func(r *Request) (*Response, error) {
			a := r.Header.Get("Authorization")
			if a != test.ExpectAuthHeader {
				t.Errorf("test %d: auth header mismatch: got %q, want %q", i, a, test.ExpectAuthHeader)
			}
			return nil, errors.New("nothing to see here, move along")
		})
		c, err := couchdb.NewClient(test.URL, rt)
		if err != nil {
			t.Fatalf("test %d: NewClient returned unexpected error: %v", i, err)
		}
		if c.URL() != test.ExpectURL {
			t.Errorf("test %d: ServerURL mismatch: got %q, want %q", i, c.URL(), test.ExpectURL)
		}
		if test.SetAuth != nil {
			c.SetAuth(test.SetAuth)
		}
		c.Ping(context.Background()) // trigger round trip
	}
}

func TestServerURL(t *testing.T) {
	c := newTestClient(t)
	check(t, "c.URL()", "http://testClient:5984", c.URL())
}

func TestPing(t *testing.T) {
	c := newTestClient(t)
	c.Handle("HEAD /", func(resp ResponseWriter, req *Request) {})

	if err := c.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCreateDB(t *testing.T) {
	c := newTestClient(t)
	c.Handle("PUT /db", func(resp ResponseWriter, req *Request) {})

	db, err := c.CreateDB(context.Background(), "db")
	if err != nil {
		t.Fatal(err)
	}

	check(t, "db.Name()", "db", db.Name())
}

func TestDeleteDB(t *testing.T) {
	c := newTestClient(t)
	c.Handle("DELETE /db", func(resp ResponseWriter, req *Request) {})
	if err := c.DeleteDB(context.Background(), "db"); err != nil {
		t.Fatal(err)
	}
}

func TestAllDBs(t *testing.T) {
	c := newTestClient(t)
	c.Handle("GET /_all_dbs", func(resp ResponseWriter, req *Request) {
		io.WriteString(resp, `["a","b","c"]`)
	})

	names, err := c.AllDBs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	check(t, "returned names", []string{"a", "b", "c"}, names)
}

// those are re-used across several tests
var securityObjectJSON = regexp.MustCompile(`\s`).ReplaceAllString(
	`{
		"admins": {
			"names": ["adminName1", "adminName2"]
		},
		"members": {
			"names": ["memberName1"],
			"roles": ["memberRole1"]
		}
	}`, "")
var securityObject = &couchdb.Security{
	Admins: couchdb.Members{
		Names: []string{"adminName1", "adminName2"},
		Roles: nil,
	},
	Members: couchdb.Members{
		Names: []string{"memberName1"},
		Roles: []string{"memberRole1"},
	},
}

func TestSecurity(t *testing.T) {
	c := newTestClient(t)
	c.Handle("GET /db/_security", func(resp ResponseWriter, req *Request) {
		resp.Header().Set("content-length", strconv.Itoa(len(securityObjectJSON)))
		io.WriteString(resp, securityObjectJSON)
	})

	secobj, err := c.DB("db").Security(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	check(t, "secobj", securityObject, secobj)
}

func TestEmptySecurity(t *testing.T) {
	c := newTestClient(t)
	c.Handle("GET /db/_security", func(resp ResponseWriter, req *Request) {
		// CouchDB returns an empty response if no security object has been set.
		resp.Header().Set("content-length", "0")
		resp.WriteHeader(200)
	})

	secobj, err := c.DB("db").Security(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	check(t, "secobj", &couchdb.Security{}, secobj)
}

func TestPutSecurity(t *testing.T) {
	c := newTestClient(t)
	c.Handle("PUT /db/_security", func(resp ResponseWriter, req *Request) {
		body, _ := io.ReadAll(req.Body)
		check(t, "request body", securityObjectJSON, string(body))
		resp.WriteHeader(200)
	})

	err := c.DB("db").PutSecurity(context.Background(), securityObject)
	if err != nil {
		t.Fatal(err)
	}
}

type testDocument struct {
	Rev   string `json:"_rev,omitempty"`
	Field int64  `json:"field"`
}

func TestGetExistingDoc(t *testing.T) {
	c := newTestClient(t)
	c.Handle("GET /db/doc", func(resp ResponseWriter, req *Request) {
		io.WriteString(resp, `{
			"_id": "doc",
			"_rev": "1-619db7ba8551c0de3f3a178775509611",
			"field": 999
		}`)
	})

	var doc testDocument
	if err := c.DB("db").Get(context.Background(), "doc", &doc, nil); err != nil {
		t.Fatal(err)
	}
	check(t, "doc.Rev", "1-619db7ba8551c0de3f3a178775509611", doc.Rev)
	check(t, "doc.Field", int64(999), doc.Field)
}

func TestGetDesignDoc(t *testing.T) {
	c := newTestClient(t)
	c.Handle("GET /db/_design/doc", func(resp ResponseWriter, req *Request) {
		io.WriteString(resp, `{
			"_id": "doc",
			"_rev": "1-619db7ba8551c0de3f3a178775509611"
		}`)
	})

	var doc testDocument
	if err := c.DB("db").Get(context.Background(), "_design/doc", &doc, nil); err != nil {
		t.Fatal(err)
	}
	check(t, "doc.Rev", "1-619db7ba8551c0de3f3a178775509611", doc.Rev)
}

func TestGetNonexistingDoc(t *testing.T) {
	c := newTestClient(t)
	c.Handle("GET /db/doc", func(resp ResponseWriter, req *Request) {
		resp.WriteHeader(404)
		io.WriteString(resp, `{"error":"not_found","reason":"error reason"}`)
	})

	var doc testDocument
	err := c.DB("db").Get(context.Background(), "doc", doc, nil)
	check(t, "couchdb.NotFound(err)", true, couchdb.NotFound(err))
}

func TestRev(t *testing.T) {
	c := newTestClient(t)
	db := c.DB("db")
	c.Handle("HEAD /db/ok", func(resp ResponseWriter, req *Request) {
		resp.Header().Set("ETag", `"1-619db7ba8551c0de3f3a178775509611"`)
	})
	c.Handle("HEAD /db/404", func(resp ResponseWriter, req *Request) {
		NotFound(resp, req)
	})

	rev, err := db.Rev(context.Background(), "ok")
	if err != nil {
		t.Fatal(err)
	}
	check(t, "rev", "1-619db7ba8551c0de3f3a178775509611", rev)

	errorRev, err := db.Rev(context.Background(), "404")
	check(t, "errorRev", "", errorRev)
	check(t, "couchdb.NotFound(err)", true, couchdb.NotFound(err))
	if _, ok := err.(*couchdb.Error); !ok {
		t.Errorf("expected couchdb.Error, got %#+v", err)
	}
}

func TestRevDBSlash(t *testing.T) {
	c := newTestClient(t)
	c.Handle("HEAD /test%2Fdb/doc%2Fid", func(resp ResponseWriter, req *Request) {
		resp.Header().Set("ETag", `"1-619db7ba8551c0de3f3a178775509611"`)
	})

	db := c.DB("test/db")
	rev, err := db.Rev(context.Background(), "doc/id")
	if err != nil {
		t.Fatal(err)
	}
	check(t, "rev", "1-619db7ba8551c0de3f3a178775509611", rev)
}

func TestPut(t *testing.T) {
	c := newTestClient(t)
	c.Handle("PUT /db/doc", func(resp ResponseWriter, req *Request) {
		body, _ := io.ReadAll(req.Body)
		check(t, "request body", `{"field":999}`, string(body))

		resp.Header().Set("ETag", `"1-619db7ba8551c0de3f3a178775509611"`)
		resp.WriteHeader(StatusCreated)
		io.WriteString(resp, `{
			"id": "doc",
			"ok": true,
			"rev": "1-619db7ba8551c0de3f3a178775509611"
		}`)
	})

	doc := &testDocument{Field: 999}
	rev, err := c.DB("db").Put(context.Background(), "doc", doc, "")
	if err != nil {
		t.Fatal(err)
	}
	check(t, "returned rev", "1-619db7ba8551c0de3f3a178775509611", rev)
}

func TestPutWithRev(t *testing.T) {
	c := newTestClient(t)
	c.Handle("PUT /db/doc", func(resp ResponseWriter, req *Request) {
		check(t, "request query string",
			"rev=1-619db7ba8551c0de3f3a178775509611",
			req.URL.RawQuery)

		body, _ := io.ReadAll(req.Body)
		check(t, "request body", `{"field":999}`, string(body))

		resp.Header().Set("ETag", `"2-619db7ba8551c0de3f3a178775509611"`)
		resp.WriteHeader(StatusCreated)
		io.WriteString(resp, `{
			"id": "doc",
			"ok": true,
			"rev": "2-619db7ba8551c0de3f3a178775509611"
		}`)
	})

	doc := &testDocument{Field: 999}
	rev, err := c.DB("db").Put(context.Background(), "doc", doc, "1-619db7ba8551c0de3f3a178775509611")
	if err != nil {
		t.Fatal(err)
	}
	check(t, "returned rev", "2-619db7ba8551c0de3f3a178775509611", rev)
}

func TestDelete(t *testing.T) {
	c := newTestClient(t)
	c.Handle("DELETE /db/doc", func(resp ResponseWriter, req *Request) {
		check(t, "request query string",
			"rev=1-619db7ba8551c0de3f3a178775509611",
			req.URL.RawQuery)

		resp.Header().Set("ETag", `"2-619db7ba8551c0de3f3a178775509611"`)
		resp.WriteHeader(StatusOK)
		io.WriteString(resp, `{
			"id": "doc",
			"ok": true,
			"rev": "2-619db7ba8551c0de3f3a178775509611"
		}`)
	})

	delrev := "1-619db7ba8551c0de3f3a178775509611"
	if rev, err := c.DB("db").Delete(context.Background(), "doc", delrev); err != nil {
		t.Fatal(err)
	} else {
		check(t, "returned rev", "2-619db7ba8551c0de3f3a178775509611", rev)
	}
}

func TestView(t *testing.T) {
	c := newTestClient(t)
	c.Handle("GET /db/_design/test/_view/testview",
		func(resp ResponseWriter, req *Request) {
			expected := url.Values{
				"offset": {"5"},
				"limit":  {"100"},
				"reduce": {"false"},
			}
			check(t, "request query values", expected, req.URL.Query())

			io.WriteString(resp, `{
				"offset": 5,
				"rows": [
					{
						"id": "SpaghettiWithMeatballs",
						"key": "meatballs",
						"value": 1
					},
					{
						"id": "SpaghettiWithMeatballs",
						"key": "spaghetti",
						"value": 1
					},
					{
						"id": "SpaghettiWithMeatballs",
						"key": "tomato sauce",
						"value": 1
					}
				],
				"total_rows": 3
			}`)
		})

	type row struct {
		ID, Key string
		Value   int
	}
	type testviewResult struct {
		TotalRows int `json:"total_rows"`
		Offset    int
		Rows      []row
	}

	var result testviewResult
	err := c.DB("db").View(context.Background(), "_design/test", "testview", &result, couchdb.Options{
		"offset": 5,
		"limit":  100,
		"reduce": false,
	})
	if err != nil {
		t.Fatal(err)
	}

	expected := testviewResult{
		TotalRows: 3,
		Offset:    5,
		Rows: []row{
			{"SpaghettiWithMeatballs", "meatballs", 1},
			{"SpaghettiWithMeatballs", "spaghetti", 1},
			{"SpaghettiWithMeatballs", "tomato sauce", 1},
		},
	}
	check(t, "result", expected, result)
}

func TestAllDocs(t *testing.T) {
	c := newTestClient(t)
	c.Handle("GET /db/_all_docs",
		func(resp ResponseWriter, req *Request) {
			expected := url.Values{
				"offset":   {"5"},
				"limit":    {"100"},
				"startkey": {"[\"Zingylemontart\",\"Yogurtraita\"]"},
			}
			check(t, "request query values", expected, req.URL.Query())

			io.WriteString(resp, `{
				"total_rows": 2666,
				"rows": [
					{
						"value": {
							"rev": "1-a3544d296de19e6f5b932ea77d886942"
						},
						"id": "Zingylemontart",
						"key": "Zingylemontart"
					},
					{
						"value": {
							"rev": "1-91635098bfe7d40197a1b98d7ee085fc"
						},
						"id": "Yogurtraita",
						"key": "Yogurtraita"
					}
				],
				"offset" : 5
			}`)
		})

	type alldocsResult struct {
		TotalRows int `json:"total_rows"`
		Offset    int
		Rows      []map[string]interface{}
	}

	var result alldocsResult
	err := c.DB("db").AllDocs(context.Background(), &result, couchdb.Options{
		"offset":   5,
		"limit":    100,
		"startkey": []string{"Zingylemontart", "Yogurtraita"},
	})
	if err != nil {
		t.Fatal(err)
	}

	expected := alldocsResult{
		TotalRows: 2666,
		Offset:    5,
		Rows: []map[string]interface{}{
			{
				"key": "Zingylemontart",
				"id":  "Zingylemontart",
				"value": map[string]interface{}{
					"rev": "1-a3544d296de19e6f5b932ea77d886942",
				},
			},
			{
				"key": "Yogurtraita",
				"id":  "Yogurtraita",
				"value": map[string]interface{}{
					"rev": "1-91635098bfe7d40197a1b98d7ee085fc",
				},
			},
		},
	}
	check(t, "result", expected, result)
}
