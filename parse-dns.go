package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "modernc.org/sqlite"
)

func main() {
	inputPath := "./dnsmasq.log"
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
		domain := extractDomain(line)
		if domain != "" {
			addUniqueDomain(dbPath, domain)
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
		domain TEXT UNIQUE NOT NULL
	);
	`

	_, err = db.Exec(createTableSQL)
	if err != nil {
		return err
	}

	return nil
}

func extractDomain(line string) string {
	parts := strings.Fields(line)
	for i, part := range parts {
		if strings.HasPrefix(part, "query[") && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func addUniqueDomain(dbPath, domain string) error {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	insertSQL := `
	INSERT INTO domains (domain) VALUES (?);
	`

	_, err = db.Exec(insertSQL, domain)
	if err != nil && !strings.Contains(err.Error(), "UNIQUE constraint failed") {
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

	rows, err := db.Query("SELECT domain FROM domains ORDER BY domain ASC")
	if err != nil {
		return err
	}
	defer rows.Close()

	var uniqueDomains []string
	for rows.Next() {
		var domain string
		err = rows.Scan(&domain)
		if err != nil {
			return err
		}
		uniqueDomains = append(uniqueDomains, domain)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	outputPath := "unique_domains.txt"
	outFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	for _, domain := range uniqueDomains {
		writer.WriteString(domain + "\n")
	}
	writer.Flush()

	fmt.Printf("Saved %d unique domains to %s\n", len(uniqueDomains), outputPath)

	return nil
}