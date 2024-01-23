package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/ququzone/hermes-patch/commands"
)

func main() {
	app := &cli.App{
		Name:    "hermes-patch",
		Version: "v0.0.1",
		Authors: []*cli.Author{
			{
				Name: "IoTeX",
			},
		},
		HelpName:  "hermes-patc",
		Usage:     "IoTeX hermes patch",
		UsageText: "hermes-patch <SUBCOMMAND>",
		Commands:  commands.Commonds(),
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err.Error())
	}
}
