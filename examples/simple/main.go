package main

import (
	"context"
	"encoding/json"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/snapflowio/cdc"
	"github.com/snapflowio/cdc/config"
	"github.com/snapflowio/cdc/message/format"
	"github.com/snapflowio/cdc/publication"
	"github.com/snapflowio/cdc/replication"
	"github.com/snapflowio/cdc/slot"
)

func main() {
	ctx := context.Background()

	cfg := config.NewConfig(
		config.WithDSN("postgres://user:password@127.0.0.1:5432/postgres"),
		config.WithDebugMode(true),
		config.WithLogLevel(logrus.InfoLevel),

		config.WithPublication(publication.NewConfig(
			publication.WithName("cdc_publication"),
			publication.WithCreateIfNotExists(true),
			publication.WithTables(publication.Tables{
				publication.NewTable("posts",
					publication.WithReplicaIdentity(publication.ReplicaIdentityFull),
				),
			}),
			publication.WithOperations(publication.Operations{
				publication.OperationInsert,
				publication.OperationDelete,
				publication.OperationTruncate,
				publication.OperationUpdate,
			}))),

		// Replication slot configuration
		config.WithSlot(slot.NewConfig(
			slot.WithName("cdc_slot"),
			slot.WithCreateIfNotExists(true),
			slot.WithSlotActivityCheckerInterval(3000),
		)),

		// Heartbeat table
		config.WithHeartbeatTable(publication.NewTable("heartbeat")),
	)

	connector, err := cdc.NewConnector(ctx, *cfg, Handler)
	if err != nil {
		logrus.WithError(err).Error("failed to create connector")
		os.Exit(1)
	}
	defer connector.Close()

	// Start the connector in a goroutine
	go connector.Start(ctx)

	// Wait for connector to be ready
	if err := connector.WaitUntilReady(ctx); err != nil {
		logrus.WithError(err).Error("connector failed to become ready")
		os.Exit(1)
	}

	logrus.Info("CDC connector is ready and listening for changes on the users table")

	select {}
}

func Handler(ctx *replication.ListenerContext) {
	switch msg := ctx.Message.(type) {
	case *format.Insert:
		logrus.WithFields(logrus.Fields{
			"action": msg.WAL2JSON.Action,
			"schema": msg.WAL2JSON.Schema,
			"table":  msg.WAL2JSON.Table,
		}).Info("INSERT event received")

		printWAL2JSON(msg.WAL2JSON)

	case *format.Update:
		logrus.WithFields(logrus.Fields{
			"action": msg.WAL2JSON.Action,
			"schema": msg.WAL2JSON.Schema,
			"table":  msg.WAL2JSON.Table,
		}).Info("UPDATE event received")

		printWAL2JSON(msg.WAL2JSON)

	case *format.Delete:
		logrus.WithFields(logrus.Fields{
			"action": msg.WAL2JSON.Action,
			"schema": msg.WAL2JSON.Schema,
			"table":  msg.WAL2JSON.Table,
		}).Info("DELETE event received")

		printWAL2JSON(msg.WAL2JSON)

	case *format.Snapshot:
		logrus.WithFields(logrus.Fields{
			"schema": msg.Schema,
			"table":  msg.Table,
			"rows":   len(msg.Data),
		}).Debug("SNAPSHOT received")
	}

	if err := ctx.Ack(); err != nil {
		logrus.WithError(err).Error("failed to acknowledge message")
	}
}

func printWAL2JSON(msg *format.WAL2JSONMessage) {
	jsonData, err := json.MarshalIndent(msg, "", "  ")
	if err != nil {
		logrus.WithError(err).Error("failed to marshal wal2json")
		return
	}

	logrus.Info("WAL2JSON format")
	logrus.Info("\n" + string(jsonData))

	logrus.Info("Columns in this change:")
	for _, col := range msg.Columns {
		logrus.WithFields(logrus.Fields{
			"name":    col.Name,
			"type":    col.Type,
			"typeoid": col.TypeOID,
			"value":   col.Value,
		}).Debug("  Column")
	}

	if len(msg.PK) > 0 {
		logrus.Info("Primary key columns:")
		for _, pk := range msg.PK {
			logrus.Infof("  - %s", pk.Name)
		}
	}
}
