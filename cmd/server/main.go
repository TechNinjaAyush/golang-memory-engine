package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
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
	graphSnapshotSubject = "graph.snapshot"
)

func CheckHealth(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"message": "Server is healthy"})
}

func handleGraphSnapshot(msg *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var data model.GraphResponse

	err := json.Unmarshal(msg.Data, &data)
	if err != nil {
		log.Println("Processing failed:", err)
		return
	}

	// Generate a unique timestamp for this synchronization cycle
	scanTimestamp := time.Now().UnixMilli()

	nodes := data.Elements.Nodes

	log.Printf("Received graph snapshot with %d nodes and %d edges", len(nodes), len(data.Elements.Edges))

	writtenNodes := 0
	for _, n := range nodes {
		if n.Data.ID == "" {
			log.Println("Skipping node with empty id")
			continue
		}
		if err := db.CreateNode(ctx, n, scanTimestamp); err != nil {
			log.Printf("Error creating node %q: %v", n.Data.ID, err)
			continue
		}
		writtenNodes++
	}

	slog.Info("Node creation done", "count", writtenNodes)

	edges := data.Elements.Edges

	writtenEdges := 0
	for _, e := range edges {
		if e.Data.ID == "" || e.Data.Source == "" || e.Data.Target == "" {
			log.Printf("Skipping edge with missing id/source/target: id=%q source=%q target=%q", e.Data.ID, e.Data.Source, e.Data.Target)
			continue
		}
		if err := db.CreateEdge(ctx, e, scanTimestamp); err != nil {
			log.Printf("Error creating edge %q: %v", e.Data.ID, err)
			continue
		}
		writtenEdges++
	}

	slog.Info("edge creation done", "count", writtenEdges)

	// Remove stale nodes and relationships not seen in this sync cycle
	if err := db.CleanupStaleData(ctx, scanTimestamp); err != nil {
		log.Println("Error cleaning up stale data:", err)
	}

	log.Printf("Graph snapshot processed successfully: wrote %d/%d nodes and %d/%d edges", writtenNodes, len(nodes), writtenEdges, len(edges))
}

func subscribeWithCoreNATS(nc *nats.Conn) error {
	_, err := nc.Subscribe(graphSnapshotSubject, func(msg *nats.Msg) {
		handleGraphSnapshot(msg)
	})
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

	err = subscribeWithCoreNATS(nc)
	if err != nil {
		log.Fatal("Core NATS subscription failed: ", err)
	}
	log.Printf("Subscribed to %s with core NATS", graphSnapshotSubject)

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
