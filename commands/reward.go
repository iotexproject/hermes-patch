package commands

import "github.com/urfave/cli/v2"

type Reward struct {
}

func NewReward() *Reward {
	return &Reward{}
}

func (c *Reward) Command() *cli.Command {
	return &cli.Command{
		Name:    "reward",
		Aliases: []string{"r"},
	}
}
