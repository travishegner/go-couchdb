// Package couchdb implements wrappers for the CouchDB HTTP API.
//
// Unless otherwise noted, all functions in this package
// can be called from more than one goroutine at the same time.
package couchdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Client represents a remote CouchDB server.
type Client struct{ *Transport }

// NewClient creates a new client object.
//
// If rawurl contains credentials, the client will authenticate
// using HTTP Basic Authentication. If rawurl has a query string,
// it is ignored.
//
// The second argument can be nil to use http.Transport,
// which should be good enough in most cases.
func NewClient(rawurl string, rt http.RoundTripper) (*Client, error) {
	url, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	url.RawQuery, url.Fragment = "", ""
	var auth Auth
	if url.User != nil {
		passwd, _ := url.User.Password()
		auth = BasicAuth(url.User.Username(), passwd)
		url.User = nil
	}
	return &Client{newTransport(url.String(), rt, auth)}, nil
}

// URL returns the URL prefix of the server.
// The url will not contain a trailing '/'.
func (c *Client) URL() string {
	return c.prefix
}

// Ping can be used to check whether a server is alive.
// It sends an HTTP HEAD request to the server's URL.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.closedRequest(ctx, "HEAD", "/", nil)
	return err
}

// SetAuth sets the authentication mechanism used by the client.
// Use SetAuth(nil) to unset any mechanism that might be in use.
// In order to verify the credentials against the server, issue any request
// after the call the SetAuth.
func (c *Client) SetAuth(a Auth) {
	c.Transport.setAuth(a)
}

// CreateDB creates a new database.
// The request will fail with status "412 Precondition Failed" if the database
// already exists. A valid DB object is returned in all cases, even if the
// request fails.
func (c *Client) CreateDB(ctx context.Context, name string) (*DB, error) {
	if _, err := c.closedRequest(ctx, "PUT", dbpath(name), nil); err != nil {
		return c.DB(name), err
	}
	return c.DB(name), nil
}

// EnsureDB ensures that a database with the given name exists.
func (c *Client) EnsureDB(ctx context.Context, name string) (*DB, error) {
	db, err := c.CreateDB(ctx, name)
	if err != nil && !ErrorStatus(err, http.StatusPreconditionFailed) {
		return nil, err
	}
	return db, nil
}

// DeleteDB deletes an existing database.
func (c *Client) DeleteDB(ctx context.Context, name string) error {
	_, err := c.closedRequest(ctx, "DELETE", dbpath(name), nil)
	return err
}

// DBExists makes a head call for the database by name and uses the response to determine if it exists or not
func (c *Client) DBExists(ctx context.Context, name string) (bool, error) {
	_, err := c.closedRequest(ctx, "HEAD", dbpath(name), nil)
	if err != nil {
		if couchdbError, ok := err.(*Error); ok {
			if couchdbError.StatusCode == http.StatusNotFound {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil

}

// AllDBs returns the names of all existing databases.
func (c *Client) AllDBs(ctx context.Context) (names []string, err error) {
	resp, err := c.request(ctx, "GET", "/_all_dbs", nil)
	if err != nil {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return names, err
	}
	err = readBody(resp, &names)
	return names, err
}

// DB represents a remote CouchDB database.
type DB struct {
	*Transport
	name string
}

// DB creates a database object.
// The database inherits the authentication and http.RoundTripper
// of the client. The database's actual existence is not verified.
func (c *Client) DB(name string) *DB {
	return &DB{c.Transport, name}
}

func (db *DB) path() *pathBuilder {
	return new(pathBuilder).add(db.name)
}

// Name returns the name of a database.
func (db *DB) Name() string {
	return db.name
}

var getJsonKeys = []string{"open_revs", "atts_since"}

// Get retrieves a document from the given database.
// The document is unmarshalled into the given object.
// Some fields (like _conflicts) will only be returned if the
// options require it. Please refer to the CouchDB HTTP API documentation
// for more information.
//
// http://docs.couchdb.org/en/latest/api/document/common.html?highlight=doc#get--db-docid
func (db *DB) Get(ctx context.Context, id string, doc any, opts Options) error {
	path, err := db.path().docID(id).options(opts, getJsonKeys)
	if err != nil {
		return err
	}
	resp, err := db.request(ctx, "GET", path, nil)
	if err != nil {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return err
	}
	return readBody(resp, &doc)
}

func (db *DB) GetBulk(ctx context.Context, ids []string, opts Options) ([]map[string]any, error) {
	type doc struct {
		Ok    map[string]any    `json:"ok"`
		Error map[string]string `json:"error"`
	}
	type result struct {
		Id   string `json:"id"`
		Docs []*doc `json:"docs"`
	}
	type bulkGetResponse struct {
		Results []*result `json:"results"`
	}

	path, err := db.path().add("_bulk_get").options(opts, getJsonKeys)
	if err != nil {
		return nil, err
	}

	root := map[string][]map[string]string{
		"docs": make([]map[string]string, len(ids)),
	}

	for i, id := range ids {
		root["docs"][i] = map[string]string{"id": id}
	}

	b, err := json.Marshal(root)
	if err != nil {
		return nil, err
	}

	resp, err := db.request(ctx, "POST", path, bytes.NewReader(b))
	if err != nil {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return nil, err
	}

	bgResp := &bulkGetResponse{}
	err = readBody(resp, bgResp)
	if err != nil {
		return nil, err
	}

	docs := make([]map[string]any, len(ids))
	for i, result := range bgResp.Results {
		if len(result.Docs) < 1 {
			return nil, fmt.Errorf("result docs array cannot be empty")
		}
		if len(result.Docs[0].Error) > 0 {
			return nil, fmt.Errorf("result doc contains an error at %v %v: %v",
				result.Docs[0].Error["id"], result.Docs[0].Error["error"], result.Docs[0].Error["reason"])
		}
		if len(result.Docs[0].Ok) <= 0 {
			return nil, fmt.Errorf("result doc missing \"ok\" field")
		}
		docs[i] = result.Docs[0].Ok
		delete(docs[i], "_revisions")
	}

	return docs, nil
}

// Exists makes a head call for the document and uses the response to check if the content exists or not
func (db *DB) Exists(ctx context.Context, id string, opts Options) (bool, error) {
	path, err := db.path().docID(id).options(opts, getJsonKeys)
	if err != nil {
		return false, err
	}
	_, err = db.closedRequest(ctx, "HEAD", path, nil)
	if err != nil {
		if couchdbError, ok := err.(*Error); ok {
			if couchdbError.StatusCode == http.StatusNotFound {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil

}

// Rev fetches the current revision of a document.
// It is faster than an equivalent Get request because no body
// has to be parsed.
func (db *DB) Rev(ctx context.Context, id string) (string, error) {
	path := db.path().docID(id).path()
	resp, err := db.closedRequest(ctx, "HEAD", path, nil)
	if resp.StatusCode == http.StatusNotFound {
		return "", nil
	}
	return responseRev(resp, err)
}

// Put stores a document into the given database.
func (db *DB) Put(ctx context.Context, id string, doc any, rev string) (newrev string, err error) {
	path := db.path().docID(id).rev(rev)
	// TODO: make it possible to stream encoder output somehow
	json, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	b := bytes.NewReader(json)
	return responseRev(db.closedRequest(ctx, "PUT", path, b))
}

func (db *DB) PutBulk(ctx context.Context, docs any, opts Options) ([]*Status, error) {
	path, err := db.path().addRaw("_bulk_docs").options(opts, viewJsonKeys)
	if err != nil {
		return nil, fmt.Errorf("error making path: %w", err)
	}
	body := make(map[string]any)
	body["docs"] = docs

	js, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("error marshaling body: %w", err)
	}
	resp, err := db.request(ctx, "POST", path, bytes.NewReader(js))
	if err != nil {
		return nil, fmt.Errorf("error sending bulk request: %w", err)
	}

	statuses := make([]*Status, 0)
	err = readBody(resp, &statuses)
	if err != nil {
		return nil, fmt.Errorf("error reading bulk response body: %w", err)
	}

	return statuses, nil
}

// Delete marks a document revision as deleted.
func (db *DB) Delete(ctx context.Context, id, rev string) (newrev string, err error) {
	path := db.path().docID(id).rev(rev)
	return responseRev(db.closedRequest(ctx, "DELETE", path, nil))
}

// Security represents database security objects.
type Security struct {
	Admins  Members `json:"admins"`
	Members Members `json:"members"`
}

// Members represents member lists in database security objects.
type Members struct {
	Names []string `json:"names,omitempty"`
	Roles []string `json:"roles,omitempty"`
}

// Security retrieves the security object of a database.
func (db *DB) Security(ctx context.Context) (*Security, error) {
	secobj := new(Security)
	path := db.path().addRaw("_security").path()
	resp, err := db.request(ctx, "GET", path, nil)
	if err != nil {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return nil, err
	}
	// The extra check for io.EOF is there because empty responses are OK.
	// CouchDB returns an empty response if no security object has been set.
	if err = readBody(resp, secobj); err != nil && err != io.EOF {
		return nil, err
	}
	return secobj, nil
}

// PutSecurity sets the database security object.
func (db *DB) PutSecurity(ctx context.Context, secobj *Security) error {
	json, _ := json.Marshal(secobj)
	body := bytes.NewReader(json)
	path := db.path().addRaw("_security").path()
	_, err := db.closedRequest(ctx, "PUT", path, body)
	return err
}

var viewJsonKeys = []string{"startkey", "start_key", "key", "endkey", "end_key"}

// View invokes a view.
// The ddoc parameter must be the full name of the design document
// containing the view definition, including the _design/ prefix.
//
// The output of the query is unmarshalled into the given result.
// The format of the result depends on the options. Please
// refer to the CouchDB HTTP API documentation for all the possible
// options that can be set.
//
// http://docs.couchdb.org/en/latest/api/ddoc/views.html
func (db *DB) View(ctx context.Context, ddoc, view string, result any, opts Options) error {
	if !strings.HasPrefix(ddoc, "_design/") {
		return errors.New("couchdb.View: design doc name must start with _design/")
	}
	path, err := db.path().docID(ddoc).addRaw("_view").add(view).options(opts, viewJsonKeys)
	if err != nil {
		return err
	}
	resp, err := db.request(ctx, "GET", path, nil)
	if err != nil {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return err
	}
	return readBody(resp, &result)
}

// AllDocs invokes the _all_docs view of a database.
//
// The output of the query is unmarshalled into the given result.
// The format of the result depends on the options. Please
// refer to the CouchDB HTTP API documentation for all the possible
// options that can be set.
//
// http://docs.couchdb.org/en/latest/api/database/bulk-api.html#db-all-docs
func (db *DB) AllDocs(ctx context.Context, result any, opts Options) error {
	path, err := db.path().addRaw("_all_docs").options(opts, viewJsonKeys)
	if err != nil {
		return err
	}
	resp, err := db.request(ctx, "GET", path, nil)
	if err != nil {
		if resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		return err
	}
	return readBody(resp, &result)
}
