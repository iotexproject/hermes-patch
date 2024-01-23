package commands

import (
	"github.com/urfave/cli/v2"

	"github.com/ququzone/hermes-patch/commands/reward"
)

func Commonds() []*cli.Command {
	return []*cli.Command{
		reward.NewClaimer().Command(),
		NewTransfer().Command(),
	}
}
