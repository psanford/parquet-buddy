package cmd

import (
	"encoding/json"
	"io"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func toJSONCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "to-json <parquet-file>",
		Short: "Dump data as a JSON stream",
		Run:   toJSONAction,
	}

	return &cmd
}

func toJSONAction(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		log.Fatalf("Missing required <parquet-file> argument")
	}

	fr, err := local.NewLocalFileReader(args[0])
	if err != nil {
		log.Fatalf("Open %s failed: %s", args[0], err)
	}

	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		log.Fatal(err)
		return
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	for {
		got, err := pr.ReadByNumber(1)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Failed to read record: %s", err)
		}

		if err = enc.Encode(got); err != nil {
			log.Fatalf("Failed to marshal record to json: %s", err)
		}
	}

	pr.ReadStop()
	fr.Close()
}
