package dao

import (
	"fmt"
	"math/big"

	"github.com/jinzhu/gorm"

	"github.com/ququzone/hermes-patch/hermes/cmd/key"
)

// DropRecord drop record model
type DropRecord struct {
	gorm.Model

	EndEpoch     uint64
	DelegateName string `gorm:"type:varchar(100)"`
	Voter        string `gorm:"type:varchar(41)"`
	Index        uint64
	Amount       string `gorm:"type:varchar(50)"`
	Status       string `gorm:"type:varchar(15);index:idx_drop_records_status"`
	Hash         string `gorm:"type:varchar(64)"`
	Signature    string `gorm:"type:text"`
	ErrorMessage string `gorm:"type:text"`
}

// TableName table name of DropRecord
func (DropRecord) TableName() string {
	return "drop_records"
}

// Save insert or update drop record
func (t DropRecord) Save(tx *gorm.DB) error {
	if tx == nil {
		tx = db
	}

	if t.Signature == "" {
		signature, err := key.Sign(fmt.Sprintf("%s,%s,%s", t.DelegateName, t.Amount, t.Status), privateKey)
		if err != nil {
			return err
		}
		t.Signature = signature
	}

	if t.ID == 0 {
		var count uint64
		err := tx.Model(&DropRecord{}).Where("`end_epoch` = ? and `delegate_name` = ? and `voter` = ?", t.EndEpoch, t.DelegateName, t.Voter).Count(&count).Error
		if err != nil {
			return err
		}
		if count > 0 {
			return nil
		}

		return tx.Create(&t).Error
	}
	return tx.Save(&t).Error
}

// Verify verify signature
func (t *DropRecord) Verify() error {
	return key.Verify(fmt.Sprintf("%s,%s,%s", t.DelegateName, t.Amount, t.Status), t.Signature, publicKey)
}

// FindNewDropRecordByLimit find by limit
func FindNewDropRecordByLimit(limit int32) (result []DropRecord, err error) {
	err = db.Limit(limit).Where("status = ?", "new").Find(&result).Error
	return
}

func FindVotersByStatus(status string) (result []string, err error) {
	stmt, _ := db.DB().Prepare("select distinct voter from drop_records where status = '" + status + "'")
	rows, err := stmt.Query()
	if err != nil {
		return
	}
	for rows.Next() {
		var row string
		if err = rows.Scan(&row); err != nil {
			return
		}
		result = append(result, row)
	}
	return
}

func FindByVoterAndStatus(voter, status string) (result []DropRecord, err error) {
	err = db.Where("voter = ? and status = ?", voter, status).Find(&result).Error
	return
}

func FindByStatus(status string) (result []DropRecord, err error) {
	err = db.Where("status = ?", status).Find(&result).Error
	return
}

func SumByEndEpoch(endEpoch uint64) (*big.Int, uint64, error) {
	var result float64

	stmt, _ := db.DB().Prepare("select sum(amount) as total from drop_records where end_epoch = ?")
	err := stmt.QueryRow(endEpoch).Scan(&result)
	if err != nil {
		return nil, 0, err
	}
	sum, _ := big.NewFloat(result).Int(nil)

	var maxEndEpoch uint64
	stmt, _ = db.DB().Prepare("select max(end_epoch) as end_epoch from drop_records")
	err = stmt.QueryRow().Scan(&maxEndEpoch)
	if err != nil {
		return nil, 0, err
	}

	return sum, maxEndEpoch, err
}

type SmallRecord struct {
	gorm.Model

	EndEpoch     uint64
	SentEpoch    uint64
	DelegateName string `gorm:"type:varchar(100);index:idx_small_records_delegate_name"`
	Voter        string `gorm:"type:varchar(41);index:idx_small_records_voter"`
	Amount       string `gorm:"type:varchar(50)"`
	Status       string `gorm:"type:varchar(15);index:idx_small_records_status"`
	Hash         string `gorm:"type:varchar(64)"`
	Signature    string `gorm:"type:text"`
	ErrorMessage string `gorm:"type:text"`
}

func FindSmallByVoterAndStatus(voter, delegate, status string, endEpoch uint64) (result []SmallRecord, err error) {
	err = db.Where("voter = ? and delegate_name = ? and status = ? and end_epoch <> ?", voter, delegate, status, endEpoch).Find(&result).Error
	return
}

func FindPendingSmalls(voter, delegate string, endEpoch uint64) (result []SmallRecord, err error) {
	err = db.Where("voter = ? and delegate_name = ? and end_epoch <> ?", voter, delegate, endEpoch).Find(&result).Error
	return
}

// Save insert or update small record
func (t SmallRecord) Save(tx *gorm.DB) error {
	if tx == nil {
		tx = db
	}

	if t.Signature == "" {
		signature, err := key.Sign(fmt.Sprintf("%s,%s,%s", t.DelegateName, t.Amount, t.Status), privateKey)
		if err != nil {
			return err
		}
		t.Signature = signature
	}

	if t.ID == 0 {
		var count uint64
		err := tx.Model(&SmallRecord{}).Where("`end_epoch` = ? and `delegate_name` = ? and `voter` = ?", t.EndEpoch, t.DelegateName, t.Voter).Count(&count).Error
		if err != nil {
			return err
		}
		if count > 0 {
			return nil
		}

		return tx.Create(&t).Error
	}
	return tx.Save(&t).Error
}

// Verify verify signature
func (t *SmallRecord) Verify() error {
	return key.Verify(fmt.Sprintf("%s,%s,%s", t.DelegateName, t.Amount, t.Status), t.Signature, publicKey)
}

type SmallRecordBak struct {
	gorm.Model

	EndEpoch     uint64
	SentEpoch    uint64
	DelegateName string `gorm:"type:varchar(100);index:idx_small_records_delegate_name"`
	Voter        string `gorm:"type:varchar(41);index:idx_small_records_voter"`
	Amount       string `gorm:"type:varchar(50)"`
	Status       string `gorm:"type:varchar(15);index:idx_small_records_status"`
	Hash         string `gorm:"type:varchar(64)"`
	Signature    string `gorm:"type:text"`
	ErrorMessage string `gorm:"type:text"`
}

func BakCompletedRecord() error {
	tx := Transaction()
	err := tx.Exec("INSERT INTO small_record_baks SELECT * FROM small_records WHERE status =?", "completed").Error
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Exec("DELETE FROM small_records WHERE status =?", "completed").Error
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit().Error
}

type Account struct {
	gorm.Model

	Address  string `gorm:"type:varchar(41);index:idx_all_addresses_address"`
	Transfer uint8
	Contract uint8
}

// Save insert or update small record
func (t Account) Save(tx *gorm.DB) error {
	if tx == nil {
		tx = db
	}

	var count uint64
	err := tx.Model(&Account{}).Where("`address` = ?", t.Address).Count(&count).Error
	if err != nil {
		return err
	}
	if count > 0 {
		return nil
	}
	return tx.Create(&t).Error
}

func FindAccount(address string) (*Account, error) {
	var account Account
	err := db.Model(&Account{}).Where("`address` = ?", address).First(&account).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	return &account, err
}
