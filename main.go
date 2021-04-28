package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

	driver "github.com/SAP/go-hdb/driver"
	"github.com/revolveyao/hdb"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

type BC_MSG_LOG struct {
	MSG_ID      string `gorm:"primaryKey"`
	ACTION_NAME string
	MSG_BYTES   driver.NullLob `gorm:"type:bytes"`
}

func main() {
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold: time.Second,
			LogLevel:      logger.Info, // Log level
		},
	)

	db, err := gorm.Open(hdb.New(hdb.Config{
		DriverName: "hdb",
		DSN:        os.Getenv("GOHDBDSN"),
	}), &gorm.Config{
		Logger: newLogger,
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   "SAPJAVA1.",
			SingularTable: true,
			NoLowerCase:   true,
		},
		DryRun:                                   false,
		PrepareStmt:                              false,
		AllowGlobalUpdate:                        false,
		DisableAutomaticPing:                     false,
		DisableForeignKeyConstraintWhenMigrating: false,
	})
	if err != nil {
		panic(err)
	}
	var msg BC_MSG_LOG
	msg.MSG_BYTES.Lob = driver.NewLob(new(bytes.Buffer), new(bytes.Buffer))
	db.Select([]string{"MSG_ID", "ACTION_NAME", "MSG_BYTES"}).Where("MSG_ID = ?", "00505683-b621-1eeb-a8fb-24069a1521d4").Find(&msg)
	fmt.Println(msg.ACTION_NAME)
}
