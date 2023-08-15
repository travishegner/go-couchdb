package couchdb

// Status represents the data structure returned from
// couchdb after attempting to create or update a document
type Status struct {
	ID     string `json:"id"`
	OK     bool   `json:"ok"`
	Error  string `json:"error"`
	Reason string `json:"reason"`
}
