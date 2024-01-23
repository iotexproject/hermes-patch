package commands

import (
	"log"

	"github.com/iotexproject/iotex-antenna-go/v2/account"
	"github.com/ququzone/hermes-patch/hermes/cmd/dao"
	"github.com/ququzone/hermes-patch/hermes/cmd/distribute"
	"github.com/ququzone/hermes-patch/hermes/util"
	"github.com/urfave/cli/v2"
)

type Sender struct {
}

func NewSender() *Sender {
	return &Sender{}
}

func (c *Sender) Command() *cli.Command {
	return &cli.Command{
		Name:    "sender",
		Aliases: []string{"s"},
		Action: func(ctx *cli.Context) error {
			err := dao.ConnectDatabase()
			if err != nil {
				log.Fatalf("create database error: %v\n", err)
			}

			notifier, err := distribute.NewNotifier(util.MustFetchNonEmptyParam("LARK_ENDPOINT"), util.MustFetchNonEmptyParam("LARK_KEY"))
			if err != nil {
				log.Fatalf("new notifier error: %v\n", err)
			}

			acc, err := util.ReadAccount()
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
