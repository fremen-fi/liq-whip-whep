package audio

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// Server hosts the WHEP/WHIP HTTP endpoints and owns the session manager.
type Server struct {
	Manager  *Manager
	BasePath string // e.g. "/audio"; resource URLs are <BasePath>/sessions/<id>

	// Hub broadcasts on-air PCM frames to all WHEP listeners. Required
	// for WHEP; nil makes WHEP requests fail with 503.
	Hub *PCMHub

	// Sink receives decoded mic PCM from WHIP and forwards it to
	// Liquidsoap. Required for WHIP; nil makes WHIP requests fail with 503.
	Sink *PCMSink

	// AllowedOrigins controls CORS. Empty means same-origin only.
	AllowedOrigins []string
}

func NewServer(basePath string) *Server {
	if basePath == "" {
		basePath = "/audio"
	}
	return &Server{
		Manager:  NewManager(),
		BasePath: strings.TrimRight(basePath, "/"),
	}
}

// Handler registers WHEP and WHIP under BasePath.
//
//	POST   <base>/whep                — create downlink session (browser receives audio)
//	POST   <base>/whip                — create uplink session   (browser sends audio)
//	DELETE <base>/sessions/<id>       — terminate a session
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(s.BasePath+"/whep", s.handleWHEP)
	mux.HandleFunc(s.BasePath+"/whip", s.handleWHIP)
	mux.HandleFunc(s.BasePath+"/sessions/", s.handleSession)
	return s.cors(mux)
}

func (s *Server) cors(next http.Handler) http.Handler {
	if len(s.AllowedOrigins) == 0 {
		return next
	}
	wildcard := false
	allowed := make(map[string]bool, len(s.AllowedOrigins))
	for _, o := range s.AllowedOrigins {
		o = strings.TrimRight(o, "/")
		if o == "*" {
			wildcard = true
			continue
		}
		allowed[o] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimRight(r.Header.Get("Origin"), "/")
		if origin != "" && (wildcard || allowed[origin]) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.Header().Set("Access-Control-Allow-Methods", "POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Max-Age", "600")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleWHEP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.Hub == nil {
		http.Error(w, "audio source not configured", http.StatusServiceUnavailable)
		return
	}
	s.handleNegotiate(w, r, "down", s.startWHEPSession)
}

func (s *Server) handleWHIP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.Sink == nil {
		http.Error(w, "audio sink not configured", http.StatusServiceUnavailable)
		return
	}
	s.handleNegotiate(w, r, "up", s.startWHIPSession)
}

func (s *Server) handleNegotiate(w http.ResponseWriter, r *http.Request, direction string, startFn func(*Session, string) (string, error)) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	if !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/sdp") {
		http.Error(w, "Content-Type must be application/sdp", http.StatusUnsupportedMediaType)
		return
	}
	sid, err := newSessionID()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sess := &Session{
		ID:        sid,
		Direction: direction,
		CreatedAt: time.Now(),
	}
	answerSDP, err := startFn(sess, string(body))
	if err != nil {
		slog.Warn("audio: negotiate failed", "direction", direction, "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.Manager.Add(sess)

	w.Header().Set("Content-Type", "application/sdp")
	w.Header().Set("Location", s.BasePath+"/sessions/"+sid)
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(answerSDP))
}

func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, s.BasePath+"/sessions/")
	if id == "" {
		http.Error(w, "missing id", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodDelete:
		sess, ok := s.Manager.Get(id)
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		sess.Close()
		s.Manager.Remove(id)
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// Shutdown closes every active session.
func (s *Server) Shutdown() {
	s.Manager.CloseAll()
}
