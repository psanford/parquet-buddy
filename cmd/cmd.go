package cmd

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "parquet-buddy",
	Short: "You're parquest friend",
}

func Execute() error {
	rootCmd.AddCommand(metadataCommand())

	return rootCmd.Execute()
}

var (
	fileFlag string
)
