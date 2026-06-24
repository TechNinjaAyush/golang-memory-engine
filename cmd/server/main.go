package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"service-mesg/db"
	"service-mesg/model"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func CheckHealth(c *gin.Context) {

	c.JSON(http.StatusOK, gin.H{"message": "Server is healthy"})
}

func main() {

	ctx := context.Background()

	//conneting to nats server

	fmt.Print("Connecting to nats server...")
	nc, err := nats.Connect(nats.DefaultURL, nats.MaxReconnects(-1), nats.ReconnectWait(2*time.Second))

	if err != nil {
		log.Fatal("Error in connecting nats", err)
	}

	defer nc.Close()

	// creating a jetstream context

	js, err := nc.JetStream()

	if err != nil {
		log.Fatal("Jetstream creation failed", err)
	}

	//  connecting to neo4j database

	driver, err := neo4j.NewDriverWithContext("bolt://localhost:7687", neo4j.BasicAuth("neo4j", "password", ""))

	if err != nil {
		log.Fatal("Error in connecting to neo4j", err)
	}
	db.Driver = driver

	defer driver.Close(ctx)
	err = driver.VerifyConnectivity(ctx)
	if err != nil {
		log.Fatal("Error in verifying neo4j connectivity", err)
	}

	log.Printf("Neo4j connected succesfully...")

	// subscribe
	_, err = js.Subscribe(
		"graph.snapshot",

		func(msg *nats.Msg) {

			var data model.GraphResponse
			err := json.Unmarshal(msg.Data, &data)

			if err != nil {
				log.Println("Processing failed:\n", err)
				return
			}

			// Generate a unique timestamp for this synchronization cycle
			scanTimestamp := time.Now().UnixMilli()

			nodes := data.Elements.Nodes

			for _, n := range nodes {

				db.CreateNode(n, scanTimestamp)

			}

			edges := data.Elements.Edges

			for _, e := range edges {
				db.CreateEdge(e, scanTimestamp)
			}

			// Remove stale nodes and relationships not seen in this sync cycle
			db.CleanupStaleData(scanTimestamp)

			msg.Ack()
		},
		nats.Durable("graph-snapshot-consumer"),
		nats.AckWait(30*time.Second),
		nats.MaxDeliver(5),
		nats.ManualAck(),
	)

	if err != nil {
		log.Fatal("Subscription failed:", err)
	}

	fmt.Println("Listening for messages...")

	r := gin.Default()

	r.Run(":3000")

	// Keep running
	select {}

}
