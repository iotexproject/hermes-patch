package commands

import (
	"fmt"
	"log"
	"os"

	"github.com/iotexproject/iotex-antenna-go/v2/account"
	"github.com/ququzone/hermes-patch/hermes/cmd/dao"
	"github.com/ququzone/hermes-patch/hermes/cmd/distribute"
	"github.com/ququzone/hermes-patch/hermes/util"
	"github.com/urfave/cli/v2"
)

type Sender struct {
	password string
}

func NewSender() *Sender {
	return &Sender{}
}

func (c *Sender) Command() *cli.Command {
	return &cli.Command{
		Name:    "sender",
		Aliases: []string{"s"},
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

			sender, err := distribute.NewSender(notifier, []account.Account{acc})
			if err != nil {
				log.Fatalf("new notifier error: %v\n", err)
			}
			go sender.Send()

			forever := make(chan bool)
			<-forever

			return nil
		},
	}
}
