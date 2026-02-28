package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

func main() {
	inputPath := "./dnsmasq.log"
	//inputPath := "/var/log/dnsmasq.log"
	dbPath := "unique_domains.db"

	fmt.Printf("Parsing: %s\n", inputPath)

	file, err := os.Open(inputPath)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer file.Close()

	err = initDatabase(dbPath)
	if err != nil {
		fmt.Printf("Error initializing database: %v\n", err)
		return
	}

	scanner := bufio.NewScanner(file)

	var linesProcessed uint64
	domainTimesMap := make(map[string]domainTimes)
	stopProgress := startProgressIndicator(file, &linesProcessed)
	defer stopProgress()

	for scanner.Scan() {
		line := scanner.Text()
		atomic.AddUint64(&linesProcessed, 1)
		domain, timestamp := extractDomainAndTimestamp(line)
		if domain != "" {
			reversed := reverseDomainParts(domain)
			current := domainTimesMap[reversed]
			// Initialize first_seen/last_seen for new domains
			if current.FirstSeen == 0 || timestamp < current.FirstSeen {
				current.FirstSeen = timestamp
			}
			if timestamp > current.LastSeen {
				current.LastSeen = timestamp
			}
			domainTimesMap[reversed] = current
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error scanning: %v\n", err)
		return
	}

	if err := saveDomainsToDatabase(dbPath, domainTimesMap); err != nil {
		fmt.Printf("Error saving domains to database: %v\n", err)
		return
	}

	err = sortAndExportDatabase(dbPath)
	if err != nil {
		fmt.Printf("Error sorting and exporting database: %v\n", err)
		return
	}

	fmt.Println("Process completed successfully.")
}

type domainTimes struct {
	FirstSeen int64
	LastSeen  int64
}

func startProgressIndicator(file *os.File, linesProcessed *uint64) func() {
	var totalSize int64
	if st, err := file.Stat(); err == nil {
		totalSize = st.Size()
	}

	start := time.Now()
	ticker := time.NewTicker(250 * time.Millisecond)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				lines := atomic.LoadUint64(linesProcessed)
				pos, err := file.Seek(0, io.SeekCurrent)
				if err == nil && totalSize > 0 {
					pct := (float64(pos) / float64(totalSize)) * 100
					fmt.Fprintf(os.Stderr, "\rProgress: %6.2f%%  %d/%d bytes  %d lines  elapsed %s      ",
						pct, pos, totalSize, lines, time.Since(start).Truncate(time.Second))
				} else if err == nil {
					fmt.Fprintf(os.Stderr, "\rProgress: %d bytes  %d lines  elapsed %s      ",
						pos, lines, time.Since(start).Truncate(time.Second))
				} else {
					fmt.Fprintf(os.Stderr, "\rProgress: %d lines  elapsed %s      ",
						lines, time.Since(start).Truncate(time.Second))
				}
			case <-done:
				return
			}
		}
	}()

	return func() {
		close(done)
		ticker.Stop()
		// Ensure the terminal doesn't stay on the progress line.
		fmt.Fprintln(os.Stderr)
	}
}

func initDatabase(dbPath string) error {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS domains (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		domain TEXT UNIQUE NOT NULL,
		first_seen INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
		last_seen INTEGER NOT NULL
	);
	`

	_, err = db.Exec(createTableSQL)
	if err != nil {
		return err
	}

	return nil
}

func extractDomainAndTimestamp(line string) (string, int64) {
	if len(line) < 15 {
		fmt.Printf("Line is too short: %s\n", line)
		return "", 0
	}

	timestampPart := line[:15]
	domainPart := line[15:]

	layout := "Jan _2 15:04:05"
	timestamp, err := time.Parse(layout, timestampPart)
	if err != nil {
		fmt.Printf("Error parsing timestamp: %v\n", err)
		return "", 0
	}

	// fmt.Printf("Timestamp: %d\n", timestamp.Unix())

	parts := strings.Fields(domainPart)
	for i, part := range parts {
		if strings.HasPrefix(part, "query[") && i+1 < len(parts) {
			domain := parts[i+1]
			// fmt.Printf("Found domain: %s\n", domain)
			return domain, timestamp.Unix()
		}
	}

	return "", 0
}

func reverseDomainParts(domain string) string {
	parts := strings.Split(domain, ".")
	for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
		parts[i], parts[j] = parts[j], parts[i]
	}
	return strings.Join(parts, ".")
}

func saveDomainsToDatabase(dbPath string, domains map[string]domainTimes) error {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
		INSERT INTO domains (domain, first_seen, last_seen)
		VALUES (?, ?, ?)
		ON CONFLICT(domain) DO UPDATE SET
			first_seen = MIN(first_seen, excluded.first_seen),
			last_seen = MAX(last_seen, excluded.last_seen)
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for domain, times := range domains {
		if _, err := stmt.Exec(domain, times.FirstSeen, times.LastSeen); err != nil {
			tx.Rollback()
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func sortAndExportDatabase(dbPath string) error {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	rows, err := db.Query("SELECT domain, first_seen, last_seen FROM domains ORDER BY domain ASC")
	if err != nil {
		return err
	}
	defer rows.Close()

	err = writeRowsToFile(rows, "unique_domains.txt")

	if err != nil {
		return err
	}

	rows, err = db.Query("SELECT domain, first_seen, last_seen FROM domains ORDER BY first_seen DESC")
	if err != nil {
		return err
	}
	defer rows.Close()

	err = writeRowsToFile(rows, "unique_domains_by_first_seen.txt")

	if err != nil {
		return err
	}

	return err
}

func writeRowsToFile(rows *sql.Rows, outputPath string) error {

	var uniqueDomains []struct {
		Domain    string
		FirstSeen int64
		LastSeen  int64
	}
	for rows.Next() {
		var domain sql.NullString
		var firstSeen, lastSeen int64

		err := rows.Scan(&domain, &firstSeen, &lastSeen)
		if err != nil {
			return err
		}

		// Convert null string to regular string if not NULL
		domainStr := ""
		if domain.Valid {
			domainStr = domain.String
		}

		uniqueDomains = append(uniqueDomains, struct {
			Domain    string
			FirstSeen int64
			LastSeen  int64
		}{domainStr, firstSeen, lastSeen})
	}

	if err := rows.Err(); err != nil {
		return err
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	for _, domainInfo := range uniqueDomains {
		fmt.Fprintf(writer, "%s\t%s\t%s\n",
			unixToDateTime(domainInfo.FirstSeen),
			unixToDateTime(domainInfo.LastSeen),
			domainInfo.Domain)
	}
	writer.Flush()

	fmt.Printf("Saved %d unique domains to %s\n", len(uniqueDomains), outputPath)

	return nil
}

func unixToDateTime(unix int64) string {
	t := time.Unix(unix, 0)
	return t.Format("Jan _2 2006 15:04:05 MST")
}
