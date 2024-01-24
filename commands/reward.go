package commands

import (
	"context"
	"fmt"
	"log"
	"syscall"
	"time"

	"github.com/iotexproject/iotex-address/address"
	"github.com/iotexproject/iotex-antenna-go/v2/account"
	"github.com/iotexproject/iotex-antenna-go/v2/iotex"
	"github.com/iotexproject/iotex-proto/golang/iotexapi"
	"github.com/urfave/cli/v2"
	"golang.org/x/term"
	"google.golang.org/grpc"

	"github.com/ququzone/hermes-patch/hermes/cmd/dao"
	"github.com/ququzone/hermes-patch/hermes/cmd/distribute"
	"github.com/ququzone/hermes-patch/hermes/util"
)

type Reward struct {
	password string
}

func NewReward() *Reward {
	return &Reward{}
}

func (c *Reward) Command() *cli.Command {
	return &cli.Command{
		Name:    "reward",
		Aliases: []string{"r"},
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:     "password",
				Aliases:  []string{"P"},
				Usage:    "password",
				Required: true,
				Action: func(ctx *cli.Context, p bool) error {
					if p {
						password, err := term.ReadPassword(int(syscall.Stdin))
						if err != nil {
							return fmt.Errorf("read password error: %v", err)
						}
						c.password = string(password)
					}
					return nil
				},
			},
		},
		Action: func(ctx *cli.Context) error {
			tls := util.MustFetchNonEmptyParam("RPC_TLS")
			endpoint := util.MustFetchNonEmptyParam("IO_ENDPOINT")
			var conn *grpc.ClientConn
			var err error
			if tls == "true" {
				conn, err = iotex.NewDefaultGRPCConn(endpoint)
				if err != nil {
					log.Fatalf("construct grpc connection error: %v\n", err)
				}
			} else {
				conn, err = iotex.NewGRPCConnWithoutTLS(endpoint)
				if err != nil {
					log.Fatalf("construct grpc connection error: %v\n", err)
				}
			}

			defer conn.Close()
			emptyAccount, err := account.NewAccount()
			if err != nil {
				log.Fatalf("new empty account error: %v\n", err)
			}
			client := iotex.NewAuthedClient(iotexapi.NewAPIServiceClient(conn), 1, emptyAccount)

			err = dao.ConnectDatabase()
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

			retry := 0
			for {
				lastEndEpoch, err := distribute.GetLastEndEpoch(client)
				if err != nil {
					log.Printf("get last end epoch error: %v\n", err)
					retry++
					time.Sleep(5 * time.Minute)
					continue
				}
				startEpoch := lastEndEpoch + 1

				resp, err := client.API().GetChainMeta(context.Background(), &iotexapi.GetChainMetaRequest{})
				if err != nil {
					log.Printf("get chain meta error: %v\n", err)
					retry++
					time.Sleep(5 * time.Minute)
					continue
				}
				curEpoch := resp.ChainMeta.Epoch.Num

				endEpoch := startEpoch + 23

				if endEpoch+2 > curEpoch {
					resp, err := client.API().GetChainMeta(context.Background(), &iotexapi.GetChainMetaRequest{})
					if err != nil {
						log.Printf("get chain meta error: %v\n", err)
						retry++
						time.Sleep(5 * time.Minute)
						continue
					}
					curEpoch = resp.ChainMeta.Epoch.Num
					if endEpoch+2-curEpoch > 0 {
						duration := time.Duration(endEpoch + 2 - curEpoch)
						log.Printf("waiting %d hours for next distribute", duration)
						time.Sleep(duration * time.Hour)
						continue
					}
				}

				sender, err := address.FromString(util.MustFetchNonEmptyParam("SENDER_ADDR"))
				if err != nil {
					log.Printf("get sender address error: %v\n", err)
					retry++
					continue
				}

				err = distribute.Reward(notifier, acc, nil, 0, sender)
				if err != nil {
					log.Printf("distribute reward error: %v\n", err)
					notifier.SendMessage(fmt.Sprintf("Send rewards error %v", err))
					retry++
					time.Sleep(5 * time.Minute)
					continue
				}
				retry = 0
			}
		},
	}
}
