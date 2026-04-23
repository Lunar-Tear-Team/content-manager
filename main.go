package main

import (
	"bytes"
	"encoding/json"
	"flag"

	"log"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"time"
)

type Bundle struct {
	EventChapters []int32 `json:"event_chapters"`
	GachaIds      []int32 `json:"gacha_ids"`
	LoginBonuses  []int32 `json:"login_bonuses"`
	SideStories   []int32 `json:"side_stories"`
}

type BundleIndex struct {
	Bundles    map[string]Bundle `json:"bundles"`
	Permanent  Bundle            `json:"permanent"`
	Unreleased Bundle            `json:"unreleased"`
}

type ContentSchedule struct {
	ActiveBundles     []string `json:"active_bundles"`
	UnreleasedEnabled bool     `json:"unreleased_enabled"`
}

type ScheduleStats struct {
	ActiveBundles       int `json:"active_bundles"`
	ActiveGachaEntries  int `json:"active_gacha_entries"`
	TotalBundles        int `json:"total_bundles"`
	PermanentEventCount int `json:"permanent_event_count"`
	ActiveLogin         int `json:"active_login"`
	ActiveSideStories   int `json:"active_side_stories"`
}

var (
	bundleIdxPath  string
	schedulePath   string
	dbInputPath    string
	dbOutputPath   string
	lunarTearHooks string
)

var bundleIndex *BundleIndex

func main() {
	basePath := flag.String("data-dir", "../lunar-tear/server", "Path to lunar-tear server directory")
	port := flag.String("port", "8081", "Port to serve the admin UI on")
	webhook := flag.String("webhook", "http://localhost:8082/api/admin/reload", "Webhook URL to ping lunar-tear on reload")
	flag.Parse()

	bundleIdxPath = *basePath + "/assets/bundle_index.json"
	schedulePath = *basePath + "/assets/release/content_schedule.json"
	dbInputPath = *basePath + "/assets/release/20240404193219.bin.e"
	dbOutputPath = *basePath + "/assets/release/database.bin.e"
	lunarTearHooks = *webhook
	if err := loadBundleIndex(); err != nil {
		log.Fatalf("Failed to load bundle index from %s: %v", bundleIdxPath, err)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})

	http.HandleFunc("/api/status", handleStatus)
	http.HandleFunc("/api/schedule", handleSchedule)
	http.HandleFunc("/api/bundles", handleBundles)

	log.Println("Content Manager listening on :" + *port)
	log.Fatal(http.ListenAndServe(":"+*port, nil))
}

func loadBundleIndex() error {
	data, err := os.ReadFile(bundleIdxPath)
	if err != nil {
		return err
	}
	var idx BundleIndex
	if err := json.Unmarshal(data, &idx); err != nil {
		return err
	}
	bundleIndex = &idx
	return nil
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sched := loadSchedule()
	stats := calcStats(sched)

	writeJSON(w, map[string]any{
		"stats":    stats,
		"schedule": sched,
	})
}

func handleSchedule(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, loadSchedule())
	case http.MethodPost:
		var sched ContentSchedule
		if err := json.NewDecoder(r.Body).Decode(&sched); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		
		data, _ := json.MarshalIndent(sched, "", "  ")
		os.WriteFile(schedulePath, data, 0644)

		log.Println("Running patch_masterdata.py...")
		start := time.Now()
		cmd := exec.Command("python", "patch_masterdata.py", "--input", dbInputPath, "--output", dbOutputPath, "--sync-schedule", schedulePath)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Printf("Patcher failed: %v", err)
			http.Error(w, "patcher failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("Patcher completed in %v", time.Since(start))

		// Ping lunar-tear to reload
		go func() {
			http.Post(lunarTearHooks, "application/json", bytes.NewBuffer([]byte{}))
		}()

		writeJSON(w, map[string]any{
			"ok":    true,
			"stats": calcStats(sched),
		})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleBundles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	months := make([]string, 0, len(bundleIndex.Bundles))
	for m := range bundleIndex.Bundles {
		months = append(months, m)
	}
	sort.Strings(months)

	type bundleInfo struct {
		Month          string  `json:"month"`
		EventCount     int     `json:"event_count"`
		GachaCount     int     `json:"gacha_count"`
		LoginCount     int     `json:"login_count"`
		SideStoryCount int     `json:"side_story_count"`
		EventChapters  []int32 `json:"event_chapters"`
		GachaIds       []int32 `json:"gacha_ids"`
		LoginBonuses   []int32 `json:"login_bonuses"`
		SideStories    []int32 `json:"side_stories"`
	}

	bundles := make([]bundleInfo, 0, len(months))
	for _, m := range months {
		b := bundleIndex.Bundles[m]
		bundles = append(bundles, bundleInfo{
			Month:          m,
			EventCount:     len(b.EventChapters),
			GachaCount:     len(b.GachaIds),
			LoginCount:     len(b.LoginBonuses),
			SideStoryCount: len(b.SideStories),
			EventChapters:  b.EventChapters,
			GachaIds:       b.GachaIds,
			LoginBonuses:   b.LoginBonuses,
			SideStories:    b.SideStories,
		})
	}

	writeJSON(w, map[string]any{
		"bundles":    bundles,
		"permanent":  bundleIndex.Permanent,
		"unreleased": bundleIndex.Unreleased,
	})
}

func loadSchedule() ContentSchedule {
	var sched ContentSchedule
	data, err := os.ReadFile(schedulePath)
	if err == nil {
		json.Unmarshal(data, &sched)
	}
	return sched
}

func calcStats(sched ContentSchedule) ScheduleStats {
	events, gacha, login, sideStories := 0, 0, 0, 0
	
	for _, m := range sched.ActiveBundles {
		if b, ok := bundleIndex.Bundles[m]; ok {
			events += len(b.EventChapters)
			gacha += len(b.GachaIds)
			login += len(b.LoginBonuses)
			sideStories += len(b.SideStories)
		}
	}
	if sched.UnreleasedEnabled {
		events += len(bundleIndex.Unreleased.EventChapters)
		gacha += len(bundleIndex.Unreleased.GachaIds)
		login += len(bundleIndex.Unreleased.LoginBonuses)
		sideStories += len(bundleIndex.Unreleased.SideStories)
	}

	return ScheduleStats{
		ActiveBundles:       len(sched.ActiveBundles),
		ActiveGachaEntries:  gacha,
		TotalBundles:        len(bundleIndex.Bundles),
		PermanentEventCount: len(bundleIndex.Permanent.EventChapters),
		ActiveLogin:         login,
		ActiveSideStories:   sideStories,
	}
}

func writeJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(data)
}
