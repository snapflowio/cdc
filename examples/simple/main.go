package main

import (
	"context"
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
		config.WithHost("127.0.0.1"),
		config.WithPort(5432),
		config.WithUsername("user"),
		config.WithPassword("password"),
		config.WithDatabase("postgres"),

		config.WithDebugMode(false),
		config.WithLogLevel(logrus.InfoLevel),

		config.WithPublication(publication.NewConfig(
			publication.WithName("cdc_publication"),
			publication.WithCreateIfNotExists(true),
			publication.WithTables(publication.Tables{
				publication.NewTable("users",
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

	// Keep the program running
	select {}
}

func Handler(ctx *replication.ListenerContext) {
	switch msg := ctx.Message.(type) {
	case *format.Insert:
		logrus.WithFields(logrus.Fields{
			"schema": msg.TableNamespace,
			"table":  msg.TableName,
			"data":   msg.Decoded,
		}).Info("INSERT received")

	case *format.Delete:
		logrus.WithFields(logrus.Fields{
			"schema": msg.TableNamespace,
			"table":  msg.TableName,
			"old":    msg.OldDecoded,
		}).Info("DELETE received")

	case *format.Update:
		logrus.WithFields(logrus.Fields{
			"schema": msg.TableNamespace,
			"table":  msg.TableName,
			"new":    msg.NewDecoded,
			"old":    msg.OldDecoded,
		}).Info("UPDATE received")

	case *format.Snapshot:
		logrus.WithFields(logrus.Fields{
			"schema": msg.Schema,
			"table":  msg.Table,
			"data":   msg.Data,
		}).Debug("SNAPSHOT row received")
	}

	if err := ctx.Ack(); err != nil {
		logrus.WithError(err).Error("failed to acknowledge message")
	}
}
