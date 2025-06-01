// Copyright 2025 Lincoln Institute of Land Policy
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/internetofwater/nabu/internal/common/projectpath"
)

type RedirectRule struct {
	ID          string
	SourcePath  string
	Target      string
	Creator     string
	Description string
}

// XML structures for sitemap index
type SitemapIndex struct {
	XMLName  xml.Name  `xml:"sitemapindex"`
	Xmlns    string    `xml:"xmlns,attr"`
	Sitemaps []Sitemap `xml:"sitemap"`
}

type Sitemap struct {
	Loc     string `xml:"loc"`
	LastMod string `xml:"lastmod,omitempty"`
}

// XML structures for individual sitemaps
type URLSet struct {
	XMLName xml.Name `xml:"urlset"`
	Xmlns   string   `xml:"xmlns,attr"`
	URLs    []URL    `xml:"url"`
}

type URL struct {
	Loc     string `xml:"loc"`
	LastMod string `xml:"lastmod,omitempty"`
}

type Namespace struct {
	Name    string
	CSVFile string
	Rules   map[string]RedirectRule
	LastMod time.Time
}

type RedirectServer struct {
	namespaces map[string]*Namespace
}

func NewRedirectServer() *RedirectServer {
	return &RedirectServer{
		namespaces: make(map[string]*Namespace),
	}
}

func (rs *RedirectServer) AddNamespace(name, csvFile string) {
	rs.namespaces[name] = &Namespace{
		Name:    name,
		CSVFile: csvFile,
		Rules:   make(map[string]RedirectRule),
	}
}

func (rs *RedirectServer) loadNamespaceRules(ns *Namespace) error {
	file, err := os.Open(ns.CSVFile)
	if err != nil {
		return fmt.Errorf("failed to open CSV file %s: %w", ns.CSVFile, err)
	}
	defer file.Close()

	// Check if file has been modified
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file stats for %s: %w", ns.CSVFile, err)
	}

	if !stat.ModTime().After(ns.LastMod) {
		// File hasn't been modified, no need to reload
		return nil
	}

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read CSV %s: %w", ns.CSVFile, err)
	}

	if len(records) == 0 {
		return fmt.Errorf("CSV file %s is empty", ns.CSVFile)
	}

	// Clear existing rules
	ns.Rules = make(map[string]RedirectRule)

	// Skip header row
	for i, record := range records[1:] {
		if len(record) < 4 {
			log.Printf("Warning: Row %d in %s has insufficient columns, skipping", i+2, ns.CSVFile)
			continue
		}

		// Convert geoconnex.us URLs to localhost paths
		sourcePath := strings.Replace(record[0], "https://geoconnex.us", "", 1)
		if sourcePath == "" {
			sourcePath = "/"
		}

		rule := RedirectRule{
			ID:          record[0],
			SourcePath:  sourcePath,
			Target:      record[1],
			Creator:     record[2],
			Description: record[3],
		}

		ns.Rules[sourcePath] = rule
	}

	ns.LastMod = stat.ModTime()
	log.Printf("Loaded %d redirect rules from %s (namespace: %s)", len(ns.Rules), ns.CSVFile, ns.Name)
	return nil
}

func (rs *RedirectServer) loadAllRules() error {
	for _, ns := range rs.namespaces {
		if err := rs.loadNamespaceRules(ns); err != nil {
			log.Printf("Error loading namespace %s: %v", ns.Name, err)
		}
	}
	return nil
}

func (rs *RedirectServer) handleRedirect(w http.ResponseWriter, r *http.Request) {
	// Reload rules if CSV files have been modified
	rs.loadAllRules()

	path := r.URL.Path

	// Search through all namespaces for a matching rule
	for _, ns := range rs.namespaces {
		if rule, exists := ns.Rules[path]; exists {
			log.Printf("Redirecting %s -> %s (namespace: %s, rule: %s)", path, rule.Target, ns.Name, rule.Description)
			http.Redirect(w, r, rule.Target, http.StatusFound)
			return
		}
	}

	http.NotFound(w, r)
	log.Printf("No matching rule for: %s", path)
}

func (rs *RedirectServer) handleSitemapIndex(w http.ResponseWriter, r *http.Request) {
	// Reload rules if CSV files have been modified
	rs.loadAllRules()

	// Get the host from the request
	host := r.Host
	if host == "" {
		host = "localhost:8081"
	}

	// Create sitemap index structure
	sitemapIndex := SitemapIndex{
		Xmlns:    "http://www.sitemaps.org/schemas/sitemap/0.9",
		Sitemaps: make([]Sitemap, 0, len(rs.namespaces)),
	}

	// Add each namespace sitemap to the index
	for _, ns := range rs.namespaces {
		if len(ns.Rules) > 0 {
			sitemapIndex.Sitemaps = append(sitemapIndex.Sitemaps, Sitemap{
				Loc:     fmt.Sprintf("http://%s/sitemap/%s.xml", host, ns.Name),
				LastMod: ns.LastMod.Format("2006-01-02"),
			})
		}
	}

	// Set XML content type
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	// Write XML declaration
	w.Write([]byte(xml.Header))

	// Encode and write the sitemap index
	encoder := xml.NewEncoder(w)
	encoder.Indent("", "  ")
	if err := encoder.Encode(sitemapIndex); err != nil {
		log.Printf("Error encoding sitemap index: %v", err)
		return
	}
}

func (rs *RedirectServer) handleNamespaceSitemap(w http.ResponseWriter, r *http.Request) {
	// Extract namespace from URL path: /sitemap/{namespace}.xml
	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(pathParts) != 2 {
		http.NotFound(w, r)
		return
	}

	namespaceName := strings.TrimSuffix(pathParts[1], ".xml")
	ns, exists := rs.namespaces[namespaceName]
	if !exists {
		http.NotFound(w, r)
		return
	}

	// Reload rules if CSV file has been modified
	rs.loadNamespaceRules(ns)

	// Get the host from the request
	host := r.Host
	if host == "" {
		host = "localhost:8081"
	}

	// Create sitemap structure
	urlSet := URLSet{
		Xmlns: "http://www.sitemaps.org/schemas/sitemap/0.9",
		URLs:  make([]URL, 0, len(ns.Rules)),
	}

	// Add each redirect path to the sitemap
	for path := range ns.Rules {
		urlSet.URLs = append(urlSet.URLs, URL{
			Loc:     fmt.Sprintf("http://%s%s", host, path),
			LastMod: ns.LastMod.Format("2006-01-02"),
		})
	}

	// Set XML content type
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)

	// Write XML declaration
	w.Write([]byte(xml.Header))

	// Encode and write the sitemap
	encoder := xml.NewEncoder(w)
	encoder.Indent("", "  ")
	if err := encoder.Encode(urlSet); err != nil {
		log.Printf("Error encoding sitemap for namespace %s: %v", namespaceName, err)
		return
	}
}

func (rs *RedirectServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	rs.loadAllRules()

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "Redirect Server Status\n")
	fmt.Fprintf(w, "=====================\n")
	fmt.Fprintf(w, "Namespaces: %d\n\n", len(rs.namespaces))

	totalRules := 0
	for _, ns := range rs.namespaces {
		totalRules += len(ns.Rules)
		fmt.Fprintf(w, "Namespace: %s\n", ns.Name)
		fmt.Fprintf(w, "  CSV File: %s\n", ns.CSVFile)
		fmt.Fprintf(w, "  Last Modified: %s\n", ns.LastMod.Format(time.RFC3339))
		fmt.Fprintf(w, "  Rules: %d\n", len(ns.Rules))
		fmt.Fprintf(w, "  Sitemap: /sitemap/%s.xml\n\n", ns.Name)

		fmt.Fprintf(w, "  Available Paths:\n")
		for path, rule := range ns.Rules {
			fmt.Fprintf(w, "    %s -> %s\n", path, rule.Target)
			fmt.Fprintf(w, "      Description: %s\n", rule.Description)
			fmt.Fprintf(w, "      Creator: %s\n\n", rule.Creator)
		}
	}

	fmt.Fprintf(w, "Total Rules: %d\n", totalRules)
}

func main() {
	port := "8081"

	// Check for command line arguments
	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	server := NewRedirectServer()

	// Add default namespaces - you can modify this or make it configurable
	server.AddNamespace("utah", filepath.Join(projectpath.Root, "cmd", "pidServer", "testdata", "utah.csv"))

	// Example of adding multiple namespaces:
	// server.AddNamespace("rise", filepath.Join(projectpath.Root, "cmd", "pidServer", "testdata", "rise.csv"))
	// server.AddNamespace("other", filepath.Join(projectpath.Root, "cmd", "pidServer", "testdata", "other.csv"))

	// Initial load of all rules
	if err := server.loadAllRules(); err != nil {
		log.Printf("Warning: Some rules failed to load: %v", err)
	}

	// Set up HTTP handlers
	http.HandleFunc("/sitemap.xml", server.handleSitemapIndex)
	http.HandleFunc("/sitemap/", server.handleNamespaceSitemap)
	http.HandleFunc("/status", server.handleStatus)
	http.HandleFunc("/", server.handleRedirect)

	log.Printf("Starting redirect server on port %s", port)
	log.Printf("Sitemap index: http://localhost:%s/sitemap.xml", port)
	log.Printf("Status endpoint: http://localhost:%s/status", port)

	for name := range server.namespaces {
		log.Printf("Namespace '%s' sitemap: http://localhost:%s/sitemap/%s.xml", name, port, name)
	}

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
