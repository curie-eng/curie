// The console is always backed by the live API. Calls go to the same-origin
// /api prefix, which Vite proxies to the real API server (apps/api has no CORS,
// so same-origin is required).
// The browser authenticates only with the HttpOnly console session cookie
// (ADR-0083) and never holds the platform key.

// Same-origin prefix; Vite's proxy forwards it to CURIE_API_TARGET.
export const API_PREFIX = "/api";
