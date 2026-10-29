package health

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthzIsAlwaysOK(t *testing.T) {
	rr := httptest.NewRecorder()
	handler(func() bool { return false }).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK || rr.Body.String() != "ok\n" {
		t.Errorf("healthz = %d %q", rr.Code, rr.Body.String())
	}
}

func TestReadyzFollowsReadiness(t *testing.T) {
	ready := false
	h := handler(func() bool { return ready })

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusServiceUnavailable || rr.Body.String() != "not ready\n" {
		t.Errorf("readyz before ready = %d %q", rr.Code, rr.Body.String())
	}

	ready = true
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusOK || rr.Body.String() != "ready\n" {
		t.Errorf("readyz after ready = %d %q", rr.Code, rr.Body.String())
	}
}

func TestReadyzWithoutCallback(t *testing.T) {
	rr := httptest.NewRecorder()
	handler(nil).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz = %d, want 503", rr.Code)
	}
}
