package main

import (
	"bytes"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type RouteConfig struct {
	MicroserviceURL  string
	MigrationPercent *int
}

type ProxyConfig struct {
	Port             string
	MonolithURL      string
	GradualMigration bool
	Routes           map[string]RouteConfig
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func parsePercent(envKey string) *int {
	percentStr := os.Getenv(envKey)
	if percentStr == "" {
		return nil
	}
	percent, err := strconv.Atoi(percentStr)
	if err != nil {
		return nil
	}
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	return &percent
}

func loadConfig() ProxyConfig {
	return ProxyConfig{
		Port:             getEnv("PORT", "8000"),
		MonolithURL:      getEnv("MONOLITH_URL", "http://monolith:8080"),
		GradualMigration: os.Getenv("GRADUAL_MIGRATION") == "true",
		Routes: map[string]RouteConfig{
			"/api/movies": {
				MicroserviceURL:  getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081"),
				MigrationPercent: parsePercent("MOVIES_MIGRATION_PERCENT"),
			},
		},
	}
}

func shouldRouteToMicroservice(percent *int) bool {
	if percent == nil {
		return true
	}
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(100) < *percent
}

func proxyHandler(config ProxyConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		target := config.MonolithURL

		for prefix, route := range config.Routes {
			if strings.HasPrefix(r.URL.Path, prefix) {
				if route.MigrationPercent == nil || (config.GradualMigration && shouldRouteToMicroservice(route.MigrationPercent)) {
					target = route.MicroserviceURL
				}
				break
			}
		}

		proxyURL := target + r.RequestURI
		log.Printf("Proxying %s %s -> %s", r.Method, r.URL.Path, proxyURL)

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			log.Printf("Error reading request body: %v", err)
			return
		}
		r.Body.Close()

		req, err := http.NewRequest(r.Method, proxyURL, bytes.NewBuffer(bodyBytes))
		if err != nil {
			http.Error(w, "Error creating request", http.StatusInternalServerError)
			log.Printf("Error creating request: %v", err)
			return
		}
		req.Header = r.Header

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, "Error forwarding request", http.StatusBadGateway)
			log.Printf("Error forwarding request to %s: %v", proxyURL, err)
			return
		}
		defer resp.Body.Close()

		copyHeaders(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		_, copyErr := io.Copy(w, resp.Body)
		if copyErr != nil {
			log.Printf("Error copying response body: %v", copyErr)
		}
	}
}

func copyHeaders(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func main() {
	config := loadConfig()

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	http.HandleFunc("/", proxyHandler(config))

	log.Printf("Proxy service started on port %s", config.Port)
	err := http.ListenAndServe(":"+config.Port, nil)
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
