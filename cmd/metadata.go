package cmd

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/spf13/cobra"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/schema"
)

func metadataCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "metadata",
		Short: "Metadata commands",
	}

	cmd.AddCommand(dumpMetadataCommand())
	return &cmd
}

func dumpMetadataCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "dump <parquet-file>",
		Short: "Dump metadata of a parquet file",
		Run:   dumpMetaAction,
	}

	return &cmd
}

func dumpMetaAction(cmd *cobra.Command, args []string) {
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

	meta := struct {
		SchemaHandler *schema.SchemaHandler
		Footer        *parquet.FileMetaData
	}{
		SchemaHandler: pr.SchemaHandler,
		Footer:        pr.Footer,
	}

	out, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(out))

	pr.ReadStop()
	fr.Close()
}
