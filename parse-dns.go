package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

func main() {
	//inputPath := "./dnsmasq-clip.log"
        inputPath := "/var/log/dnsmasq.log"
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

	for scanner.Scan() {
		line := scanner.Text()
		domain, timestamp := extractDomainAndTimestamp(line)
		if domain != "" {
			addOrUpdateUniqueDomain(dbPath, domain, timestamp)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error scanning: %v\n", err)
		return
	}

	err = sortAndExportDatabase(dbPath)
	if err != nil {
		fmt.Printf("Error sorting and exporting database: %v\n", err)
		return
	}

	fmt.Println("Process completed successfully.")
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

func addOrUpdateUniqueDomain(dbPath, domain string, timestamp int64) error {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	domain = reverseDomainParts(domain)

	// Check if the domain already exists
	var firstSeen int64
	err = db.QueryRow("SELECT first_seen FROM domains WHERE domain = ?", domain).Scan(&firstSeen)
	if err == nil {
		// Update the last seen timestamp
		updateSQL := `
		UPDATE domains SET last_seen = ? WHERE domain = ?
		`
		_, err = db.Exec(updateSQL, timestamp, domain)
		return err
	}

	// If not found, insert a new record with last_seen set, first_seen defaults to current time
	insertSQL := `
	INSERT INTO domains (domain, last_seen) VALUES (?, ?);
	`
	_, err = db.Exec(insertSQL, domain, timestamp)
	return err
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
                        domainInfo.Domain, 
                        unixToDateTime(domainInfo.FirstSeen), 
                        unixToDateTime(domainInfo.LastSeen))
	}
	writer.Flush()

	fmt.Printf("Saved %d unique domains to %s\n", len(uniqueDomains), outputPath)

	return nil
}

func unixToDateTime(unix int64) string {
	t := time.Unix(unix, 0)
	return t.Format("Jan _2 2006 15:04:05 MST")
}