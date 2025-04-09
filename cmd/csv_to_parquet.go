package cmd

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

func csvToParquetCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "csv-to-parquet <csv-file> <parquet-file>",
		Short: "Convert CSV file to Parquet format",
		Run:   csvToParquetAction,
	}

	return &cmd
}

func csvToParquetAction(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		log.Fatalf("Missing required arguments. Usage: csv-to-parquet <csv-file> <parquet-file>")
	}

	csvFile := args[0]
	parquetFile := args[1]

	file, err := os.Open(csvFile)
	if err != nil {
		log.Fatalf("Failed to open CSV file %s: %s", csvFile, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("Failed to read CSV file: %s", err)
	}

	if len(records) < 1 {
		log.Fatalf("CSV file is empty")
	}

	fw, err := local.NewLocalFileWriter(parquetFile)
	if err != nil {
		log.Fatalf("Failed to create parquet file: %s", err)
	}
	defer fw.Close()

	headers := records[0]

	md := makeSchemaFromHeaders(headers)
	pw, err := writer.NewCSVWriter(md, fw, 4)
	if err != nil {
		log.Fatalf("Failed to create parquet writer: %s", err)
	}

	for i, record := range records {
		if i == 0 {
			continue // skip header
		}

		rec := make([]*string, len(record))
		for j := 0; j < len(record); j++ {
			rec[j] = &record[j]
		}

		if err = pw.WriteString(rec); err != nil {
			log.Fatalf("Failed to write record to parquet: %s", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		log.Fatalf("Failed to finish writing parquet file: %s", err)
	}

	fmt.Printf("Successfully converted %s to %s\n", csvFile, parquetFile)
}

func makeSchemaFromHeaders(headers []string) []string {
	md := make([]string, len(headers))
	for i, header := range headers {
		md[i] = fmt.Sprintf("name=%s, type=BYTE_ARRAY", header)
	}
	return md
}
