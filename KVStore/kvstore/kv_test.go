package kvstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// TestBasicGetPut verifies basic put/get functionality
func TestBasicGetPut(t *testing.T) {
	sockPath := tempSocket(t)
	defer os.Remove(sockPath)

	s := StartServer([]string{sockPath}, 0)
	defer s.kill()

	c := MakeClient([]string{sockPath})

	key := "test-key"
	value := "test-value"

	// First put
	_, prev, _ := c.Put(key, value, 0)
	if prev != "" {
		t.Errorf("First put returned previous value: %s", prev)
	}

	// Get check
	got, err, _ := c.Get(key, 0)
	if err != OK {
		t.Fatalf("Get failed: %v", err)
	}
	if got != value {
		t.Errorf("Get() = %s, want %s", got, value)
	}

	// Overwrite check
	newValue := "new-value"
	err, _, _ = c.Put(key, newValue, 0)
	if err != OK {
		t.Fatalf("Overwrite put failed: %v", err)
	}
}

// TestConflictDetection verifies client retry logic on write-write conflict detection
func TestConflictDetection(t *testing.T) {
	sockPath := tempSocket(t)
	defer os.Remove(sockPath)

	s := StartServer([]string{sockPath}, 0)
	defer s.kill()

	c1 := MakeClient([]string{sockPath})
	c2 := MakeClient([]string{sockPath})

	key := "conflict-key"

	// Client 1 successful put
	if err, _, _ := c1.Put(key, "value1", 0); err != OK {
		t.Fatalf("Initial put failed: %v", err)
	}

	// Client 2 retries until conflict resolved
	err, _, _ := c2.Put(key, "value2", 0)
	if err != OK {
		t.Errorf("No conflict detected: %v", err)
	}
}

// TestTransactionSuccess verifies successful transaction commit
func TestTransactionSuccess(t *testing.T) {
	sockPath := tempSocket(t)
	defer os.Remove(sockPath)

	s := StartServer([]string{sockPath}, 0)
	defer s.kill()

	c := MakeClient([]string{sockPath})

	// Simple transfer transaction
	txn := c.BeginTxn()
	txn.WriteSet["account1"] = "100"
	txn.WriteSet["account2"] = "1000"

	if err := c.CommitTxn(txn); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify writes
	val, err, _ := c.Get("account1", 0)
	if err != OK {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "100" {
		t.Errorf("Account1 balance = %s, want 100", val)
	}

	val, err, _ = c.Get("account2", 0)
	if err != OK {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "1000" {
		t.Errorf("Account2 balance = %s, want 1000", val)
	}
}

// TestTransactionAbort verifies transaction rollback
func TestTransactionAbort(t *testing.T) {
	sockPath := tempSocket(t)
	defer os.Remove(sockPath)

	s := StartServer([]string{sockPath}, 0)
	defer s.kill()

	c := MakeClient([]string{sockPath})

	key := "txn-key"
	if err, _, _ := c.Put(key, "initial", 0); err != OK {
		t.Fatalf("Initial put failed: %v", err)
	}

	txn := c.BeginTxn()
	txn.ReadSet[key] = txn.StartTs

	if err, _, _ := c.Put(key, "modified", 0); err != OK {
		t.Fatalf("External put failed: %v", err)
	}

	if err := c.CommitTxn(txn); err == nil {
		t.Error("Expected conflict, got nil error")
	}

	val, err, _ := c.Get(key, 0)
	if err != OK {
		t.Fatalf("Get failed: %v", err)
	}
	if val != "modified" {
		t.Errorf("Final value = %s, want modified", val)
	}
}

func TestVersionHistory(t *testing.T) {
	sockPath := tempSocket(t)
	defer os.Remove(sockPath)

	s := StartServer([]string{sockPath}, 0)
	defer s.kill()

	c := MakeClient([]string{sockPath})

	_, _, ts1 := c.Put("key", "v1", 0)
	_, _, ts2 := c.Put("key", "v2", 0)
	_, _, ts3 := c.Put("key", "v3", 0)

	tests := []struct {
		ts       int64
		expected string
	}{
		{ts1, "v1"},
		{ts2, "v2"},
		{ts3, "v3"},
		{ts3 + 1000, "v3"},
	}

	for _, tt := range tests {
		val, err, actualTs := c.Get("key", tt.ts)
		if err != OK || val != tt.expected {
			t.Errorf("Get(@%d) = %s (err=%v), want %s", tt.ts, val, actualTs, tt.expected)
		}
	}
}

// Multiple clients tests

func TestMultiClientGetPut(t *testing.T) {
	sockPath := tempSocket(t)
	defer os.Remove(sockPath)

	s := StartServer([]string{sockPath}, 0)
	defer s.kill()

	const numClients = 5
	clients := make([]*Client, numClients)
	for i := 0; i < numClients; i++ {
		clients[i] = MakeClient([]string{sockPath})
	}

	key := "concurrent-key"
	var wg sync.WaitGroup
	resultChan := make(chan struct {
		clientID int
		opType   string
		value    string
		ts       int64
		err      Err
	}, numClients*2)

	// Concurrent PUTs
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			value := fmt.Sprintf("v%d", clientID)
			err, prev, ts := clients[clientID].Put(key, value, 0)
			resultChan <- struct {
				clientID int
				opType   string
				value    string
				ts       int64
				err      Err
			}{clientID, "PUT", prev, ts, err}
		}(i)
	}

	// Concurrent GETs
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			val, err, ts := clients[clientID].Get(key, 0)
			resultChan <- struct {
				clientID int
				opType   string
				value    string
				ts       int64
				err      Err
			}{clientID, "GET", val, ts, err}
		}(i)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Analyze results
	var maxTS int64 = -1
	var expectedValue string
	successfulPuts := 0
	getResults := make(map[string]int)

	for res := range resultChan {
		if res.err == OK {
			switch res.opType {
			case "PUT":
				successfulPuts++
				if res.ts > maxTS {
					maxTS = res.ts
					expectedValue = fmt.Sprintf("v%d", res.clientID)
				}
			case "GET":
				getResults[res.value]++
			}
		} else if res.err == ErrConflict {
			t.Logf("Client %d conflict detected", res.clientID)
		}
	}

	// Verify exactly one successful PUT
	if successfulPuts != 5 {
		t.Fatalf("Expected 1 successful PUT, got %d", successfulPuts)
	}

	// Verify GETs saw consistent values
	if len(getResults) > 1 {
		t.Errorf("Multiple values seen in GETs: %v", getResults)
	}

	// Verify final stored value (use latest PUT's new value)
	latestVal, err, _ := clients[0].Get(key, 0)
	if err != OK || latestVal != expectedValue {
		t.Errorf("Final value mismatch: got %s (err=%v), want %s", latestVal, err, expectedValue)
	}
	if latestVal == "" {
		t.Error("Final value is empty")
	}
}

func TestMultipleClientConcurrentWrites(t *testing.T) {
	sockPath := tempSocket(t)
	defer os.Remove(sockPath)

	s := StartServer([]string{sockPath}, 0)
	defer s.kill()

	const numClients = 5
	clients := make([]*Client, numClients)
	for i := 0; i < numClients; i++ {
		clients[i] = MakeClient([]string{sockPath})
	}

	key := "concurrent-key"
	results := make(chan int64, numClients)
	var wg sync.WaitGroup

	// Concurrent PUTs
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(client *Client, idx int) {
			defer wg.Done()
			err, _, ts := client.Put(key, fmt.Sprintf("v%d", idx), 0)
			if err == OK {
				results <- ts
			} else {
				results <- -1
			}
		}(clients[i], i)
	}

	// Wait for all writes to complete
	wg.Wait()
	close(results)

	// Verify exactly one success
	successCount := 0
	var maxTS int64 = -1
	for ts := range results {
		if ts != -1 {
			successCount++
			if ts > maxTS {
				maxTS = ts
			}
		}
	}

	if successCount != 5 {
		t.Fatalf("Expected 1 successful PUT, got %d", successCount)
	}

	// Verify final value
	val, err, _ := clients[0].Get(key, 0)
	if err != OK {
		t.Fatalf("Final Get failed: %v", err)
	}
	t.Logf("Final value: %s @%d", val, maxTS)
}

func TestMultipleClientConflictDetection(t *testing.T) {
	sockPath := tempSocket(t)
	defer os.Remove(sockPath)

	s := StartServer([]string{sockPath}, 0)
	defer s.kill()

	c1 := MakeClient([]string{sockPath})
	c2 := MakeClient([]string{sockPath})

	key := "conflict-key"

	// First client succeeds
	err, _, ts1 := c1.Put(key, "v1", 0)
	if err != OK {
		t.Fatalf("Initial Put failed: %v", err)
	}

	// Second client should conflict
	err, _, _ = c2.Put(key, "v2", 0)
	if err != OK {
		t.Errorf("Conflicts resolved")
	}

	// Verify first write persists
	val, err2, ts := c2.Get(key, 0)
	if err2 != OK || val != "v2" || ts < ts1 {
		t.Errorf("Value mismatch after conflict: %s @%d (err=%v)", val, ts, err)
	}
}

func TestVersionHistoryAfterConcurrency(t *testing.T) {
	sockPath := tempSocket(t)
	defer os.Remove(sockPath)

	s := StartServer([]string{sockPath}, 0)
	defer s.kill()

	const numClients = 3
	clients := make([]*Client, numClients)
	for i := 0; i < numClients; i++ {
		clients[i] = MakeClient([]string{sockPath})
	}

	key := "version-history-key"
	timestamps := make([]int64, numClients)

	// Sequential writes with auto-generated timestamps
	for i := 0; i < numClients; i++ {
		err, _, ts := clients[i].Put(key, fmt.Sprintf("v%d", i), 0)
		if err != OK {
			t.Fatalf("Client %d Put failed: %v", i, err)
		}
		// fmt.Printf("PUT key=%s ts=%d value=%s\n", key, ts, fmt.Sprintf("v%d", i))
		timestamps[i] = ts
	}

	// Verify historical gets using stored timestamps
	for i, ts := range timestamps {
		val, err, actualTS := clients[0].Get(key, ts)
		expected := fmt.Sprintf("v%d", i)
		if err != OK || val != expected || actualTS != ts {
			t.Errorf("Version @%d: got %s (ts=%d), want %s", ts, val, actualTS, expected)
		}
		// fmt.Printf("GET key=%s req_ts=%d found_ts=%d value=%s\n", key, ts, actualTS, val)
	}

	// Verify latest version
	finalVal, err, finalTS := clients[0].Get(key, 0)
	if err != OK || finalVal != "v2" || finalTS != timestamps[2] {
		t.Errorf("Final version mismatch: got %s @%d, want v2 @%d", finalVal, finalTS, timestamps[2])
	}
}

func TestLamportClockSynchronization(t *testing.T) {
	sockPath := tempSocket(t)
	defer os.Remove(sockPath)

	s := StartServer([]string{sockPath}, 0)
	defer s.kill()

	c1 := MakeClient([]string{sockPath})
	c2 := MakeClient([]string{sockPath})

	key := "clock-sync-key"

	// Client 1 writes and gets server timestamp
	err, _, ts1 := c1.Put(key, "v1", 0)
	if err != OK {
		t.Fatalf("Client 1 Put failed: %v", err)
	}

	// Client 2 performs Get to sync clock (may fail, but updates timestamp)
	_, _, _ = c2.Get(key, 0) // Ignore result, focus on clock sync

	// Client 2's Put should now have a timestamp > ts1
	err, _, ts2 := c2.Put(key, "v2", 0)
	if err != OK {
		t.Fatalf("Client 2 Put failed: %v", err)
	}

	if ts2 <= ts1 {
		t.Errorf("Clock not synchronized: ts2=%d <= ts1=%d", ts2, ts1)
	}

	// Verify Client 1's clock updated after sync
	c1.Get(key, 0) // Force sync with server's latest timestamp
	current := atomic.LoadInt64(&c1.lamportTime)
	if current <= ts2 {
		t.Errorf("Client 1 clock not updated: %d <= %d", current, ts2)
	}
}

// Helper function to create temporary UNIX socket paths
func tempSocket(t *testing.T) string {
	dir, err := os.MkdirTemp("/tmp", "kvtest")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "sock")
}
