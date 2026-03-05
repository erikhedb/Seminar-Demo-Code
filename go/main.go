// ----------------------------------------------------------------------------
//
//	Delphix DCT Go SDK demo.
//
// Overview
// - Lists all VDBs with size and storage size.
// - Optional: refresh a named VDB to the latest timestamp.
//
// Environment
// - DCT_ENGINE_URL e.g. https://your-engine.example.com
// - DCT_API_KEY API key for DCT, include the required prefix (e.g. "Bearer ").
//
// Example
//
//	export DCT_ENGINE_URL="https://your-engine.example.com"
//	export DCT_API_KEY="Bearer YOUR_API_KEY"
//
// Usage
//
//	go run .
//	go run . "prod01-copy01"
//
// Output (tab-separated)
//
//	name	size_bytes	storage_bytes
//	prod01-copy01	339292672	12345678
//
// ----------------------------------------------------------------------------

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	dct "github.com/delphix/dct-sdk-go/v21"
)

const (
	defaultPageSize = 200
)

// ----------------------------------------------------------------------------
// Main
// - Lists VDBs by default.
// - Refreshes a named VDB when an argument is provided.
// ----------------------------------------------------------------------------
func main() {
	engineURL := mustEnv("DCT_ENGINE_URL")
	apiKey := mustEnv("DCT_API_KEY")

	client := newClient(engineURL, apiKey)
	ctx := context.Background()

	printConfig(engineURL, apiKey)

	if len(os.Args) > 1 {
		vdbName := strings.TrimSpace(os.Args[1])
		if vdbName == "" {
			log.Fatalf("vdb name argument is empty")
		}
		vdbID, resolvedName, err := findVdbByName(ctx, client, vdbName)
		if err != nil {
			log.Fatalf("find vdb: %v", err)
		}
		if err := refreshVdbToLatest(ctx, client, vdbID, resolvedName); err != nil {
			log.Fatalf("refresh vdb: %v", err)
		}
		return
	}

	fmt.Println("name\tsize_bytes\tstorage_bytes")
	cursor := ""
	for {
		req := client.VDBsAPI.GetVdbs(ctx).Limit(defaultPageSize)
		if cursor != "" {
			req = req.Cursor(cursor)
		}

		resp, _, err := req.Execute()
		if err != nil {
			log.Fatalf("list vdbs: %v", err)
		}

		for _, vdb := range resp.Items {
			name := safeNullableString(vdb.Name)
			size := safeNullableInt64(vdb.Size)
			storage := safeNullableInt64(vdb.StorageSize)
			fmt.Printf("%s\t%d\t%d\n", name, size, storage)
		}

		next := ""
		if resp.ResponseMetadata != nil && resp.ResponseMetadata.NextCursor != nil {
			next = *resp.ResponseMetadata.NextCursor
		}
		if next == "" {
			break
		}
		cursor = next
	}
}

// ----------------------------------------------------------------------------
// Client
// - Builds the DCT SDK client.
// - Sets the server URL and Authorization header.
// ----------------------------------------------------------------------------
func newClient(engineURL, apiKey string) *dct.APIClient {
	cfg := dct.NewConfiguration()

	baseURL := strings.TrimRight(engineURL, "/") + "/dct/v3"
	cfg.Servers = dct.ServerConfigurations{
		{URL: baseURL},
	}

	if cfg.DefaultHeader == nil {
		cfg.DefaultHeader = map[string]string{}
	}
	cfg.DefaultHeader["Authorization"] = apiKey

	return dct.NewAPIClient(cfg)
}

// ----------------------------------------------------------------------------
// Env
// - Reads required env vars.
// - Fails fast with a clear error.
// ----------------------------------------------------------------------------
func mustEnv(key string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		log.Fatalf("missing required env var %s", key)
	}
	return value
}

// ----------------------------------------------------------------------------
// Helpers
// - Converts a nullable SDK string to a Go string.
// - Used for VDB fields like `vdb.Name` (NullableString).
// ----------------------------------------------------------------------------
func safeNullableString(value dct.NullableString) string {
	ptr := value.Get()
	if ptr == nil {
		return ""
	}
	return *ptr
}

// ----------------------------------------------------------------------------
// Helpers
// - Converts a nullable SDK int64 to a Go int64.
// - Used for VDB fields like `vdb.Size` and `vdb.StorageSize` (NullableInt64).
// ----------------------------------------------------------------------------
func safeNullableInt64(value dct.NullableInt64) int64 {
	ptr := value.Get()
	if ptr == nil {
		return 0
	}
	return *ptr
}

// ----------------------------------------------------------------------------
// Helpers
// - Converts a regular *string to a safe value for logging/printing.
// - Used for IDs/status fields like `vdb.Id` and `job.Status` (pointer strings).
// ----------------------------------------------------------------------------
func safePtrString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// ----------------------------------------------------------------------------
// Config
// - Prints config for troubleshooting.
// - Masks secrets before printing.
// ----------------------------------------------------------------------------
func printConfig(engineURL, apiKey string) {
	maskedKey := maskSecret(apiKey)
	fmt.Printf("engine_url=%s\n", engineURL)
	fmt.Printf("api_key=%s\n", maskedKey)
}

// ----------------------------------------------------------------------------
// Security
// - Masks secrets while keeping a short suffix for debugging.
// ----------------------------------------------------------------------------
func maskSecret(value string) string {
	const keep = 4
	if len(value) <= keep {
		return strings.Repeat("*", len(value))
	}
	return strings.Repeat("*", len(value)-keep) + value[len(value)-keep:]
}

// ----------------------------------------------------------------------------
// Lookup
// - Resolves a VDB name to a unique ID.
// - Pages through the VDB list.
// ----------------------------------------------------------------------------
func findVdbByName(ctx context.Context, client *dct.APIClient, targetName string) (string, string, error) {
	cursor := ""
	foundID := ""
	foundName := ""

	for {
		req := client.VDBsAPI.GetVdbs(ctx).Limit(defaultPageSize)
		if cursor != "" {
			req = req.Cursor(cursor)
		}

		resp, _, err := req.Execute()
		if err != nil {
			return "", "", fmt.Errorf("list vdbs: %w", err)
		}

		for _, vdb := range resp.Items {
			name := safeNullableString(vdb.Name)
			if name == targetName {
				if foundID != "" {
					return "", "", fmt.Errorf("multiple VDBs named %q", targetName)
				}
				foundID = safePtrString(vdb.Id)
				foundName = name
			}
		}

		next := ""
		if resp.ResponseMetadata != nil && resp.ResponseMetadata.NextCursor != nil {
			next = *resp.ResponseMetadata.NextCursor
		}
		if next == "" {
			break
		}
		cursor = next
	}

	if foundID == "" {
		return "", "", fmt.Errorf("no VDB found named %q", targetName)
	}

	return foundID, foundName, nil
}

// ----------------------------------------------------------------------------
// Refresh
// - Starts a refresh to the latest timestamp.
// - Waits for completion and prints elapsed time.
// ----------------------------------------------------------------------------
func refreshVdbToLatest(ctx context.Context, client *dct.APIClient, vdbID, vdbName string) error {
	start := time.Now()
	params := dct.RefreshVDBByTimestampParameters{}
	resp, _, err := client.VDBsAPI.RefreshVdbByTimestamp(ctx, vdbID).
		RefreshVDBByTimestampParameters(params).
		Execute()
	if err != nil {
		return err
	}

	jobID := ""
	jobStatus := ""
	if resp != nil && resp.Job != nil {
		jobID = safePtrString(resp.Job.Id)
		jobStatus = safePtrString(resp.Job.Status)
	}

	if jobID != "" {
		fmt.Printf("refresh initiated for %s (job_id=%s status=%s)\n", vdbName, jobID, jobStatus)
		finalStatus, err := waitForJob(ctx, client, jobID)
		if err != nil {
			return err
		}
		fmt.Printf("refresh finished for %s (status=%s elapsed=%ds)\n", vdbName, finalStatus, int(time.Since(start).Seconds()))
	} else {
		fmt.Printf("refresh initiated for %s\n", vdbName)
	}
	return nil
}

// ----------------------------------------------------------------------------
// Jobs
// - Polls the job endpoint.
// - Stops at terminal status.
// ----------------------------------------------------------------------------
func waitForJob(ctx context.Context, client *dct.APIClient, jobID string) (string, error) {
	pollTicker := time.NewTicker(2 * time.Second)
	defer pollTicker.Stop()

	animTicker := time.NewTicker(200 * time.Millisecond)
	defer animTicker.Stop()

	spinner := []rune{'|', '/', '-', '\\'}
	spinIndex := 0

	printSpinner := func(status string) {
		msg := fmt.Sprintf("\r%c waiting for job %s status=%s", spinner[spinIndex%len(spinner)], jobID, status)
		fmt.Print(msg)
		spinIndex++
	}

	for {
		job, _, err := client.JobsAPI.GetJobById(ctx, jobID).Execute()
		if err != nil {
			return "", err
		}
		status := safePtrString(job.Status)

		if isTerminalJobStatus(status) {
			fmt.Print("\r")
			fmt.Printf("waiting for job %s status=%s\n", jobID, status)
			return status, nil
		}

		printSpinner(status)

		select {
		case <-pollTicker.C:
		case <-animTicker.C:
		}
	}
}

// ----------------------------------------------------------------------------
// Jobs
// - Determines if the job is finished.
// ----------------------------------------------------------------------------
func isTerminalJobStatus(status string) bool {
	switch strings.ToUpper(status) {
	case "COMPLETED", "FAILED", "CANCELED", "CANCELLED", "ABANDONED":
		return true
	default:
		return false
	}
}
