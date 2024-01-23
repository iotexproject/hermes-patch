package commands

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/accounts/keystore"
	ethCrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/iotexproject/go-pkgs/crypto"
	"github.com/iotexproject/go-pkgs/hash"
	"github.com/iotexproject/iotex-address/address"
	"github.com/iotexproject/iotex-antenna-go/v2/account"
	"github.com/iotexproject/iotex-antenna-go/v2/iotex"
	"github.com/iotexproject/iotex-proto/golang/iotexapi"
	"github.com/urfave/cli/v2"
	"golang.org/x/term"
)

type Transfer struct {
	grpc      string
	keystore  string
	recipient address.Address
	amount    *big.Int
}

func NewTransfer() *Transfer {
	return &Transfer{}
}

func (c *Transfer) Command() *cli.Command {
	return &cli.Command{
		Name:    "transfer",
		Aliases: []string{"t"},
		Usage:   "hermes-patch transfer [OPTIONS] [RECIPIENT] [AMOUNT]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "grpc",
				Aliases:  []string{"g"},
				Usage:    "gRPC endpoint",
				Required: true,
				Action: func(ctx *cli.Context, r string) error {
					c.grpc = r
					return nil
				},
			},
			&cli.StringFlag{
				Name:     "keystore",
				Aliases:  []string{"k"},
				Usage:    "keystore file",
				Required: true,
				Action: func(ctx *cli.Context, r string) error {
					c.keystore = r
					return nil
				},
			},
		},
		Action: func(ctx *cli.Context) error {
			recipient := ctx.Args().First()
			receiver, err := address.FromString(recipient)
			if err != nil {
				return fmt.Errorf("parse RECIPIENT error: %v", err)
			}
			c.recipient = receiver

			amountArg := ctx.Args().Get(1)
			amount, ok := new(big.Int).SetString(amountArg, 10)
			if !ok {
				return fmt.Errorf("parse AMOUNT error")
			}
			c.amount = amount

			return c.transfer()
		},
	}
}

func (c *Transfer) transfer() error {
	fmt.Println("Enter password:")
	password, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return fmt.Errorf("read password error: %v", err)
	}
	acc, err := readKeystore(c.keystore, string(password))
	if err != nil {
		return fmt.Errorf("read keystore error: %v", err)
	}

	conn, err := iotex.NewDefaultGRPCConn(c.grpc)
	if err != nil {
		return fmt.Errorf("new grpc connection error: %v", err)
	}
	client := iotex.NewAuthedClient(iotexapi.NewAPIServiceClient(conn), 1, acc)

	return transfer(client, c.recipient, c.amount)
}

func readKeystore(path, password string) (account.Account, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read keystore error: %v", err)
	}
	key, err := keystore.DecryptKey(data, string(password))
	if err != nil {
		return nil, fmt.Errorf("decrypt keystore error: %v", err)
	}
	pk, err := crypto.BytesToPrivateKey(ethCrypto.FromECDSA(key.PrivateKey))
	if err != nil {
		return nil, fmt.Errorf("decrypt private key error: %v", err)
	}
	return account.PrivateKeyToAccount(pk)
}

func transfer(c iotex.AuthedClient, recipient address.Address, amount *big.Int) error {
	ctx := context.Background()
	hash, err := c.Transfer(recipient, amount).Call(ctx)
	if err != nil {
		return err
	}

	err = checkActionReceipt(c, hash)
	if err != nil {
		return err
	}
	fmt.Printf("successfully transfer %s rewards to %s\n", amount.String(), recipient.String())
	return nil
}

func checkActionReceipt(c iotex.AuthedClient, hash hash.Hash256) error {
	time.Sleep(5 * time.Second)
	var resp *iotexapi.GetReceiptByActionResponse
	var err error
	for i := 0; i < 120; i++ {
		resp, err = c.API().GetReceiptByAction(context.Background(), &iotexapi.GetReceiptByActionRequest{
			ActionHash: hex.EncodeToString(hash[:]),
		})
		if err != nil {
			if strings.Contains(err.Error(), "code = NotFound") {
				time.Sleep(2 * time.Second)
				continue
			}
			return err
		}
		if resp.ReceiptInfo.Receipt.Status != 1 {
			return fmt.Errorf("action %x check receipt failed", hash)
		}
		return nil
	}
	fmt.Printf("action %x check receipt not found\n", hash)
	return err
}
