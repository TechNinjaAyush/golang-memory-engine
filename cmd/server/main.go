package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"service-mesg/db"
	"service-mesg/model"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func CheckHealth(c *gin.Context) {

	c.JSON(http.StatusOK, gin.H{"message": "Server is healthy"})
}

func main() {

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	r := gin.Default()

	server := &http.Server{

		Addr:    ":3000",
		Handler: r,
	}

	go func() {

		if err := r.Run(":3000"); err != nil {
			log.Fatalf("Error in listening on port:%v", err)
		}

	}()
	fmt.Print("Connecting to nats server...")

	natsURL := os.Getenv("NATS_URL")
	log.Printf("nats url is %s", natsURL)

	var nc *nats.Conn

	for {
		var err error

		nc, err = nats.Connect(
			natsURL,
			nats.MaxReconnects(-1),
			nats.ReconnectWait(10*time.Second),
			nats.RetryOnFailedConnect(true),
		)

		if err == nil {
			log.Println("NATS connected successfully")
			break
		}

		log.Printf("Error in connecting nats: %v", err)
		log.Println("Retrying NATS connection in 5 seconds...")
		time.Sleep(5 * time.Second)
	}

	// creating a jetstream context

	js, err := nc.JetStream()

	if err != nil {
		log.Fatal("Jetstream creation failed", err)
	}

	//  connecting to neo4j database

	driver, err := neo4j.NewDriverWithContext(os.Getenv("NEO4J_URL"), neo4j.BasicAuth(os.Getenv("NEO4J_USERNAME"), os.Getenv("NEO4J_PASSWORD"), ""))

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

	r.GET("/healthz", CheckHealth)
	r.GET("/readyz", CheckHealth)

	<-sigch

	defer cancel()

	if err := server.Shutdown(ctx); err != nil {

		log.Fatalf("failed to shutdown server %v", err)
	}

	log.Println("Server is shutdown with gracefully...")

}
