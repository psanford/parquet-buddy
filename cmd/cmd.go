package cmd

import (
	"github.com/spf13/cobra"
)

var (
	fileFlag string
)

var rootCmd = &cobra.Command{
	Use:   "parquet-buddy",
	Short: "You're parquest friend",
}

func Execute() error {
	rootCmd.AddCommand(metadataCommand())
	rootCmd.AddCommand(toJSONCommand())
	rootCmd.AddCommand(csvToParquetCommand())

	return rootCmd.Execute()
}
