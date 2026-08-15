package main

import (
	"context"
	"encoding/json"
	"errors"
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

const (
	graphSnapshotSubject  = "graph.snapshot"
	graphSnapshotStream   = "GRAPH_SNAPSHOTS"
	graphSnapshotConsumer = "graph-snapshot-consumer"
)

func CheckHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"message": "Server is healthy"})
}

func ensureGraphStream(ctx context.Context, js nats.JetStreamContext) error {
	_, err := js.StreamInfo(graphSnapshotStream, nats.Context(ctx))
	if err == nil {
		return nil
	}

	if !errors.Is(err, nats.ErrStreamNotFound) {
		return err
	}

	_, err = js.AddStream(
		&nats.StreamConfig{
			Name:      graphSnapshotStream,
			Subjects:  []string{graphSnapshotSubject},
			Retention: nats.LimitsPolicy,
			Storage:   nats.FileStorage,
		},
		nats.Context(ctx),
	)
	return err
}

func main() {

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, syscall.SIGINT, syscall.SIGTERM)

	r := gin.Default()

	server := &http.Server{
		Addr:    ":3000",
		Handler: r,
	}

	r.GET("/healthz", CheckHealth)
	r.GET("/readyz", CheckHealth)

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Error in listening on port: %v", err)
		}
	}()

	fmt.Println("Connecting to nats server...")

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

	defer nc.Close()

	// Creating a JetStream context
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal("Jetstream creation failed: ", err)
	}

	for {
		streamCtx, streamCancel := context.WithTimeout(context.Background(), 30*time.Second)
		err = ensureGraphStream(streamCtx, js)
		streamCancel()

		if err == nil {
			log.Printf("JetStream stream %s is ready for subject %s", graphSnapshotStream, graphSnapshotSubject)
			break
		}

		log.Printf("JetStream stream setup failed: %v. Retrying in 5 seconds...", err)
		time.Sleep(5 * time.Second)
	}

	// Connecting to Neo4j
	driver, err := neo4j.NewDriverWithContext(
		os.Getenv("NEO4J_URL"),
		neo4j.BasicAuth(
			os.Getenv("NEO4J_USERNAME"),
			os.Getenv("NEO4J_PASSWORD"),
			"",
		),
	)

	if err != nil {
		log.Fatal("Error in creating neo4j driver: ", err)
	}

	db.Driver = driver

	// Use a separate context only for Neo4j connectivity verification.
	verifyCtx, verifyCancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer verifyCancel()

	err = driver.VerifyConnectivity(verifyCtx)
	if err != nil {
		log.Fatal("Error in verifying neo4j connectivity: ", err)
	}

	log.Println("Neo4j connected successfully...")

	// Subscribe to graph snapshots with retry loop and timeout
	for {
		subCtx, subCancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err = js.Subscribe(
			graphSnapshotSubject,

			func(msg *nats.Msg) {

				var data model.GraphResponse

				err := json.Unmarshal(msg.Data, &data)
				if err != nil {
					log.Println("Processing failed:", err)
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

				err = msg.Ack()
				if err != nil {
					log.Println("Message ACK failed:", err)
					return
				}

				log.Println("Graph snapshot processed successfully")
			},

			nats.BindStream(graphSnapshotStream),
			nats.Durable(graphSnapshotConsumer),
			nats.AckWait(30*time.Second),
			nats.MaxDeliver(5),
			nats.ManualAck(),
			nats.Context(subCtx),
		)
		subCancel()

		if err == nil {
			log.Println("Subscribed to graph.snapshot successfully")
			break
		}

		log.Printf("Subscription failed: %v. Retrying in 5 seconds...", err)
		time.Sleep(5 * time.Second)
	}

	fmt.Println("Listening for messages...")

	// Wait for SIGINT/SIGTERM
	<-sigch

	log.Println("Shutdown signal received...")

	// Create shutdown context only when shutting down.
	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("failed to shutdown server: %v", err)
	}

	// Close Neo4j driver
	closeCtx, closeCancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer closeCancel()

	if err := driver.Close(closeCtx); err != nil {
		log.Printf("Error closing Neo4j driver: %v", err)
	}

	log.Println("Server shutdown gracefully")
}
