package commands

import (
	"fmt"
	"log"
	"math/big"
	"os"

	"github.com/iotexproject/iotex-address/address"
	"github.com/urfave/cli/v2"

	"github.com/ququzone/hermes-patch/hermes/cmd/dao"
	"github.com/ququzone/hermes-patch/hermes/cmd/distribute"
	"github.com/ququzone/hermes-patch/hermes/util"
)

type Merge struct {
	password string
	previous *big.Int
}

func NewMerge() *Merge {
	return &Merge{}
}

func (c *Merge) Command() *cli.Command {
	return &cli.Command{
		Name:    "merge",
		Aliases: []string{"m"},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "password",
				Aliases:  []string{"p"},
				Usage:    "password file path",
				Required: true,
				Action: func(ctx *cli.Context, s string) error {
					data, err := os.ReadFile(s)
					if err != nil {
						return fmt.Errorf("read password file error: %v", err)
					}
					c.password = string(data)
					if err := os.Remove(s); err != nil {
						return fmt.Errorf("remove password file error: %v", err)
					}
					return nil
				},
			},
			&cli.StringFlag{
				Name:     "previous",
				Aliases:  []string{"pr"},
				Usage:    "previous balance",
				Required: true,
				Action: func(ctx *cli.Context, s string) error {
					previous, ok := new(big.Int).SetString(s, 10)
					if !ok {
						return fmt.Errorf("parse previous error: %s", s)
					}
					c.previous = previous
					return nil
				},
			},
		},
		Action: func(ctx *cli.Context) error {
			err := dao.ConnectDatabase()
			if err != nil {
				log.Fatalf("create database error: %v\n", err)
			}

			notifier, err := distribute.NewNotifier(util.MustFetchNonEmptyParam("LARK_ENDPOINT"), util.MustFetchNonEmptyParam("LARK_KEY"))
			if err != nil {
				log.Fatalf("new notifier error: %v\n", err)
			}

			acc, err := util.ReadAccount(c.password)
			if err != nil {
				log.Fatalf("read account error: %v\n", err)
			}

			sender, err := address.FromString(util.MustFetchNonEmptyParam("SENDER_ADDR"))
			if err != nil {
				log.Fatalf("get sender address error: %v\n", err)
			}

			return distribute.Merge(notifier, acc, sender, c.previous)
		},
	}
}
