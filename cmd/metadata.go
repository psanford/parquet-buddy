package cmd

import (
	"bytes"
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
	cmd.AddCommand(listColumnsCommand())
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

func listColumnsCommand() *cobra.Command {
	cmd := cobra.Command{
		Use:   "columns <parquet-file>",
		Short: "List columns from a parquet file",
		Run:   listColumnsAction,
	}

	return &cmd
}

func listColumnsAction(cmd *cobra.Command, args []string) {
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

	for i, col := range pr.Footer.Schema {
		if col.Type == nil {
			continue
		}

		var min []byte
		var max []byte
		for _, rg := range pr.Footer.RowGroups {
			meta := rg.Columns[i-1].MetaData
			if min == nil {
				min = meta.Statistics.MinValue
			} else {
				if bytes.Compare(meta.Statistics.MinValue, min) < 0 {
					min = meta.Statistics.MinValue
				}
			}

			if max == nil {
				max = meta.Statistics.MaxValue
			} else {
				if bytes.Compare(meta.Statistics.MaxValue, max) > 0 {
					max = meta.Statistics.MaxValue
				}
			}
		}

		fmt.Printf("%s %s %q %q\n", col.Name, col.Type, min, max)
	}

	pr.ReadStop()
	fr.Close()
}
