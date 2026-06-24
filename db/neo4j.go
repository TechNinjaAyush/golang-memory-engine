package db

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"service-mesg/model"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

var Driver neo4j.DriverWithContext

func CreateNode(node model.Node, scanTimestamp int64) {

	// Convert Pods struct to a slice of maps so the Neo4j driver can parse it natively
	var podsList []map[string]interface{}
	for _, p := range node.Data.Pod {
		podMap := map[string]interface{}{
			"name":          p.Name,
			"status":        p.Status,
			"statusMessage": p.StatusMessage,
		}
		podsList = append(podsList, podMap)
	}

	// Use FOREACH to iterate over the pods list safely (even if empty)
	// and create a separate :Pod node for each pod, linking it to the :Service
	query := `
	MERGE (n:Service {id: $id})
	SET n.app = $app,
		n.namespace = $namespace,
		n.cluster = $cluster,
		n.nodeType = $nodeType,
		n.lastSeen = $scanTimestamp
		
	FOREACH (pod IN $pods |
		MERGE (p:Pod {name: pod.name, namespace: $namespace, cluster: $cluster})
		SET p.status = pod.status,
			p.statusMessage = pod.statusMessage,
			p.lastSeen = $scanTimestamp
		MERGE (n)-[hp:HAS_POD]->(p)
		SET hp.lastSeen = $scanTimestamp
	)
	`

	params := map[string]interface{}{
		"id":            node.Data.ID,
		"app":           node.Data.App,
		"namespace":     node.Data.Namespace,
		"cluster":       node.Data.Cluster,
		"nodeType":      node.Data.NodeType,
		"pods":          podsList,
		"scanTimestamp": scanTimestamp,
	}

	session := Driver.NewSession(
		context.Background(),
		neo4j.SessionConfig{},
	)

	defer session.Close(context.Background())

	_, err := session.Run(
		context.Background(),
		query,
		params,
	)

	if err != nil {
		log.Println("Error creating node:", err)
		return
	}

}

func CreateEdge(edge model.Edge, scanTimestamp int64) {

	// Serialize the nested Traffic struct into a JSON string
	// Neo4j only accepts primitives, so nested objects must be stringified.
	trafficJSON, err := json.Marshal(edge.Data.Traffic)
	if err != nil {
		log.Println("Error marshalling edge traffic:", err)
		trafficJSON = []byte("{}")
	}

	// MATCH the previously created Service nodes
	// MERGE the relationship with its unique ID
	// SET all the edge properties
	query := `
	MATCH (source:Service {id: $sourceId})
	MATCH (target:Service {id: $targetId})
	MERGE (source)-[r:TRAFFIC_TO {id: $id}]->(target)
	SET r.destPrincipal = $destPrincipal,
		r.sourcePrincipal = $sourcePrincipal,
		r.isMTLS = $isMTLS,
		r.responseTime = $responseTime,
		r.throughput = $throughput,
		r.traffic = $traffic,
		r.lastSeen = $scanTimestamp
	`

	params := map[string]interface{}{
		"id":              edge.Data.ID,
		"sourceId":        edge.Data.Source,
		"targetId":        edge.Data.Target,
		"destPrincipal":   edge.Data.DestPrincipal,
		"sourcePrincipal": edge.Data.SourcePrincipal,
		"isMTLS":          edge.Data.IsMTLS,
		"responseTime":    edge.Data.ResponseTime,
		"throughput":      edge.Data.Throughput,
		"traffic":         string(trafficJSON),
		"scanTimestamp":   scanTimestamp,
	}

	session := Driver.NewSession(
		context.Background(),
		neo4j.SessionConfig{},
	)
	defer session.Close(context.Background())

	_, err = session.Run(
		context.Background(),
		query,
		params,
	)

	if err != nil {
		log.Println("Error creating edge:", err)
		return
	}

}

// CleanupStaleData removes nodes and relationships that were not updated during the current sync cycle.
// It is safe for repeated executions (idempotent) and will cleanly remove stale elements to keep
// the Neo4j graph strictly synchronized with the Kubernetes state.
func CleanupStaleData(scanTimestamp int64) {
	// 1. Delete nodes that are stale (which will also DETACH their relationships)
	nodeCleanupQuery := `
	MATCH (n)
	WHERE n.lastSeen < $scanTimestamp
	DETACH DELETE n
	`

	// 2. Delete relationships that are stale, even if their attached nodes are still active.
	// This ensures that removed links (e.g. TRAFFIC_TO edges no longer present) are properly cleaned up.
	edgeCleanupQuery := `
	MATCH ()-[r]->()
	WHERE r.lastSeen < $scanTimestamp
	DELETE r
	`

	params := map[string]interface{}{
		"scanTimestamp": scanTimestamp,
	}

	session := Driver.NewSession(
		context.Background(),
		neo4j.SessionConfig{},
	)
	defer session.Close(context.Background())

	startTime := time.Now()

	// Execute node cleanup
	nodeResult, err := session.Run(context.Background(), nodeCleanupQuery, params)
	if err != nil {
		log.Println("Error cleaning up stale nodes:", err)
		return
	}
	nodeSummary, err := nodeResult.Consume(context.Background())
	if err != nil {
		log.Println("Error consuming node cleanup summary:", err)
		return
	}
	deletedNodes := nodeSummary.Counters().NodesDeleted()

	// Execute relationship cleanup
	edgeResult, err := session.Run(context.Background(), edgeCleanupQuery, params)
	if err != nil {
		log.Println("Error cleaning up stale edges:", err)
		return
	}
	edgeSummary, err := edgeResult.Consume(context.Background())
	if err != nil {
		log.Println("Error consuming edge cleanup summary:", err)
		return
	}
	deletedEdges := edgeSummary.Counters().RelationshipsDeleted()

	duration := time.Since(startTime).Milliseconds()

	log.Printf("Cleanup completed in %d ms. Deleted %d stale nodes and %d stale relationships.", duration, deletedNodes, deletedEdges)
}
