package reward

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/keystore"
	ethCrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/iotexproject/go-pkgs/crypto"
	"github.com/iotexproject/go-pkgs/hash"
	"github.com/iotexproject/iotex-proto/golang/iotexapi"
	"github.com/iotexproject/iotex-proto/golang/protocol"
	"github.com/urfave/cli/v2"

	"github.com/iotexproject/iotex-address/address"
	"github.com/iotexproject/iotex-antenna-go/v2/account"
	"github.com/iotexproject/iotex-antenna-go/v2/iotex"
)

type Claimer struct {
	grpc      string
	interval  uint64
	recipient address.Address
}

func NewClaimer() *Claimer {
	return &Claimer{}
}

func (c *Claimer) Command() *cli.Command {
	return &cli.Command{
		Name:    "claim",
		Aliases: []string{"c"},
		Usage:   "hermes-patch claim [OPTIONS] [RECIPIENT]",
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
			&cli.Uint64Flag{
				Name:     "interval",
				Aliases:  []string{"i"},
				Usage:    "claim interval with seconds, default interval is 30 mins. 0 means one-time claim.",
				Required: true,
				Action: func(ctx *cli.Context, i uint64) error {
					c.interval = i
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

			password, err := os.ReadFile("./pass")
			if err != nil {
				return fmt.Errorf("read password error: %v", err)
			}

			data, err := os.ReadFile("./key")
			if err != nil {
				return fmt.Errorf("read keystore error: %v", err)
			}
			key, err := keystore.DecryptKey(data, string(password))
			if err != nil {
				return fmt.Errorf("decrypt keystore error: %v", err)
			}
			pk, err := crypto.BytesToPrivateKey(ethCrypto.FromECDSA(key.PrivateKey))
			if err != nil {
				return fmt.Errorf("decrypt private key error: %v", err)
			}
			acc, err := account.PrivateKeyToAccount(pk)
			if err != nil {
				return fmt.Errorf("private key to account error: %v", err)
			}

			return c.claim(acc)
		},
	}
}

func (c *Claimer) claim(acc account.Account) error {
	conn, err := iotex.NewDefaultGRPCConn(c.grpc)
	if err != nil {
		return fmt.Errorf("new grpc connection error: %v", err)
	}
	client := iotex.NewAuthedClient(iotexapi.NewAPIServiceClient(conn), 1, acc)

	if c.interval == 0 {
		err := claimAndTransfer(client, c.recipient)
		if err != nil {
			return fmt.Errorf("claim and transfer rewards error: %v", err)
		}
		return nil
	}
	for {
		err := claimAndTransfer(client, c.recipient)
		if err != nil {
			return fmt.Errorf("claim and transfer rewards error: %v", err)
		}
		time.Sleep(time.Duration(c.interval) * time.Second)
	}
}

func claimAndTransfer(c iotex.AuthedClient, recipient address.Address) error {
	unclaimedBalance, err := getUnclaimedBalance(c)
	if err != nil {
		return err
	}
	minAmount, _ := new(big.Int).SetString("500000000000000000", 10)
	if unclaimedBalance.Cmp(minAmount) <= 0 {
		fmt.Println("Rewards too small")
		return nil
	}

	err = claim(c, unclaimedBalance)
	if err != nil {
		return err
	}

	acc, err := c.API().GetAccount(context.Background(), &iotexapi.GetAccountRequest{
		Address: c.Account().Address().String(),
	})
	if err != nil {
		return err
	}
	balance, _ := new(big.Int).SetString(acc.AccountMeta.Balance, 10)

	err = transfer(c, recipient, new(big.Int).Sub(balance, minAmount))
	if err != nil {
		return err
	}

	return nil
}

func getUnclaimedBalance(c iotex.AuthedClient) (*big.Int, error) {
	request := &iotexapi.ReadStateRequest{
		ProtocolID: []byte(protocol.RewardingProtocolID),
		MethodName: []byte(protocol.ReadUnclaimedBalanceMethodName),
		Arguments:  [][]byte{[]byte(c.Account().Address().String())},
	}
	response, err := c.API().ReadState(context.Background(), request)
	if err != nil {
		return nil, err
	}
	unclaimedBlance, ok := big.NewInt(0).SetString(string(response.Data), 10)
	if !ok {
		return nil, errors.New("failed to convert string to big int")
	}
	return unclaimedBlance, nil
}

func claim(c iotex.AuthedClient, unclaimedBalance *big.Int) error {
	ctx := context.Background()
	hash, err := c.ClaimReward(unclaimedBalance).Call(ctx)
	if err != nil {
		return err
	}

	err = checkActionReceipt(c, hash)
	if err != nil {
		return err
	}
	fmt.Printf("successfully claim rewards %s\n", unclaimedBalance.String())
	return nil
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
