// Package redact masks secrets before values reach the logs.
package redact

import (
	"net/url"
	"strings"
)

var secretQueryKeys = map[string]bool{
	"token":    true,
	"password": true,
	"passwd":   true,
	"secret":   true,
	"api_key":  true,
	"apikey":   true,
}

// URL removes user info and masks secret-looking query values. Anything it
// cannot parse is replaced entirely rather than logged as-is.
func URL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid-url>"
	}
	if u.User != nil {
		u.User = url.User("redacted")
	}
	if u.RawQuery != "" {
		q := u.Query()
		for k := range q {
			if secretQueryKeys[strings.ToLower(k)] {
				q.Set(k, "redacted")
			}
		}
		u.RawQuery = q.Encode()
	}
	return u.String()
}
