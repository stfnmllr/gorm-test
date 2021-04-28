package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"

	"database/sql"

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

func initDB(dsn, schemaName, tableName string) {
	db, err := sql.Open("hdb", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.Exec(fmt.Sprintf("drop schema %s cascade", driver.Identifier(schemaName))) // ignore error

	if _, err := db.Exec(fmt.Sprintf("create schema %s", driver.Identifier(schemaName))); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(fmt.Sprintf("create table %s.%s (msg_id varchar(50), action_name varchar(50), msg_bytes blob)", driver.Identifier(schemaName), driver.Identifier(tableName))); err != nil {
		log.Fatal(err)
	}

	stmt, err := db.Prepare(fmt.Sprintf("insert into %s.%s values(?, ?, ?)", schemaName, tableName))
	if err != nil {
		log.Fatal(err)
	}

	nullLob := &driver.NullLob{Lob: new(driver.Lob)}
	nullLob.Lob.SetReader(bytes.NewBuffer([]byte("action 1")))

	if _, err := stmt.Exec("00505683-b621-1eeb-a8fb-24069a1521d4", "A1", nullLob); err != nil {
		log.Fatal(err)
	}
}

const (
	schemaName = "SAPJAVA1"
	tableName  = "BC_MSG_LOG"
)

func main() {
	dsn := os.Getenv("GOHDBDSN")
	initDB(dsn, schemaName, tableName)

	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold: time.Second,
			LogLevel:      logger.Info, // Log level
		},
	)

	db, err := gorm.Open(hdb.New(hdb.Config{
		DriverName: "hdb",
		DSN:        dsn,
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
