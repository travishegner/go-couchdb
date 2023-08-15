package couchdb

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// Auth is implemented by HTTP authentication mechanisms.
type Auth interface {
	// AddAuth should add authentication information (e.g. headers)
	// to the given HTTP request.
	AddAuth(context.Context, *http.Request, *transport)

	// UpdateAuth should do any maintenance required to the credentials
	// based on the response from the server.
	UpdateAuth(*http.Response)
}

type basicauth string

// BasicAuth returns an Auth that performs HTTP Basic Authentication.
func BasicAuth(username, password string) Auth {
	auth := []byte(username + ":" + password)
	hdr := "Basic " + base64.StdEncoding.EncodeToString(auth)
	return basicauth(hdr)
}

func (a basicauth) AddAuth(ctx context.Context, req *http.Request, t *transport) {
	req.Header.Set("Authorization", string(a))
}

func (a basicauth) UpdateAuth(r *http.Response) {}

type cookieauth struct {
	Username string `json:"name"`
	Password string `json:"password"`
	cookie   *http.Cookie
	lock     *sync.RWMutex
}

// CookieAuth returns an Auth that performs cookie based authentication
// https://docs.couchdb.org/en/3.2.2-docs/api/server/authn.html#cookie-authentication
func CookieAuth(username, password string) Auth {
	return &cookieauth{
		Username: username,
		Password: password,
		lock:     &sync.RWMutex{},
	}
}

func (a *cookieauth) AddAuth(ctx context.Context, req *http.Request, t *transport) {
	if a.cookie == nil || time.Now().Add(5*time.Second).After(a.cookie.Expires) {
		body, err := json.Marshal(a)
		if err != nil {
			log.Errorf("error marshaling cookie auth credentials to authenticate to session: %v", err)
			return
		}
		areq, err := http.NewRequestWithContext(ctx, http.MethodPost, t.prefix+"/_session", bytes.NewReader(body))
		if err != nil {
			log.Errorf("error creating cookie auth http request: %v", err)
			return
		}
		areq.Header.Add("Content-type", "application/json")
		resp, err := t.http.Do(areq)
		if err != nil {
			log.Errorf("error executing cookie auth http request: %v", err)
		}
		if resp != nil {
			a.UpdateAuth(resp)
			b, err := io.ReadAll(resp.Body)
			if err != nil {
				log.Warningf("failed to read cookie auth response body: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.Errorf("failed to authenticate with cookie auth to couchdb: %v, %v", resp.StatusCode, string(b))
				return
			}
		}
	}
	a.lock.RLock()
	req.AddCookie(a.cookie)
	a.lock.RUnlock()
}

func (a *cookieauth) UpdateAuth(resp *http.Response) {
	cookies := resp.Cookies()
	a.lock.Lock()
	if len(cookies) > 0 {
		a.cookie = cookies[0]
	}
	a.lock.Unlock()
}

type proxyauth struct {
	username, roles, tok string
}

// ProxyAuth returns an Auth that performs CouchDB proxy authentication.
// Please consult the CouchDB documentation for more information on proxy
// authentication:
//
// http://docs.couchdb.org/en/latest/api/server/authn.html?highlight=proxy#proxy-authentication
func ProxyAuth(username string, roles []string, secret string) Auth {
	pa := &proxyauth{username, strings.Join(roles, ","), ""}
	if secret != "" {
		mac := hmac.New(sha1.New, []byte(secret))
		io.WriteString(mac, username)
		pa.tok = fmt.Sprintf("%x", mac.Sum(nil))
	}
	return pa
}

func (a proxyauth) AddAuth(ctx context.Context, req *http.Request, t *transport) {
	req.Header.Set("X-Auth-CouchDB-UserName", a.username)
	req.Header.Set("X-Auth-CouchDB-Roles", a.roles)
	if a.tok != "" {
		req.Header.Set("X-Auth-CouchDB-Token", a.tok)
	}
}

func (a proxyauth) UpdateAuth(r *http.Response) {}
