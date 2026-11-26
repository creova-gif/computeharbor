package server

import "net/http"

const uiContentSecurityPolicy = "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'"

// SecurityHeadersMiddleware adds defensive HTTP security headers to every
// response, and sets a Content-Security-Policy on the /ui console route.
func SecurityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		if r.URL.Path == "/ui" {
			h.Set("Content-Security-Policy", uiContentSecurityPolicy)
		}
		next.ServeHTTP(w, r)
	})
}
