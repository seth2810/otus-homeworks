package commands

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

type versionInfo struct {
	Release, BuildDate, GitHash string
}

func NewVersionCmd(release, buildDate, gitHash string) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version number",
		RunE: func(cmd *cobra.Command, args []string) error {
			info := versionInfo{release, buildDate, gitHash}

			if err := json.NewEncoder(os.Stdout).Encode(info); err != nil {
				return fmt.Errorf("error while decode version info: %w", err)
			}

			return nil
		},
	}
}
