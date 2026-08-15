package db

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"service-mesg/model"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

var Driver neo4j.DriverWithContext

func CreateNode(ctx context.Context, node model.Node, scanTimestamp int64) error {
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

	session := Driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	_, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, err
		}
		return result.Consume(ctx)
	})
	return err
}

func CreateEdge(ctx context.Context, edge model.Edge, scanTimestamp int64) error {
	// Serialize the nested Traffic struct into a JSON string
	// Neo4j only accepts primitives, so nested objects must be stringified.
	trafficJSON, err := json.Marshal(edge.Data.Traffic)
	if err != nil {
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

	session := Driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	summary, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, query, params)
		if err != nil {
			return nil, err
		}
		return result.Consume(ctx)
	})
	if err != nil {
		return err
	}

	counters := summary.(neo4j.ResultSummary).Counters()
	if counters.RelationshipsCreated() == 0 && counters.PropertiesSet() == 0 {
		return fmt.Errorf("edge %q was not written because source %q or target %q was not found", edge.Data.ID, edge.Data.Source, edge.Data.Target)
	}
	return nil
}

// CleanupStaleData removes nodes and relationships that were not updated during the current sync cycle.
// It is safe for repeated executions (idempotent) and will cleanly remove stale elements to keep
// the Neo4j graph strictly synchronized with the Kubernetes state.
func CleanupStaleData(ctx context.Context, scanTimestamp int64) error {
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

	session := Driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)

	startTime := time.Now()

	// Execute node cleanup
	summary, err := session.ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		nodeResult, err := tx.Run(ctx, nodeCleanupQuery, params)
		if err != nil {
			return nil, err
		}
		nodeSummary, err := nodeResult.Consume(ctx)
		if err != nil {
			return nil, err
		}

		edgeResult, err := tx.Run(ctx, edgeCleanupQuery, params)
		if err != nil {
			return nil, err
		}
		edgeSummary, err := edgeResult.Consume(ctx)
		if err != nil {
			return nil, err
		}

		return []int{
			nodeSummary.Counters().NodesDeleted(),
			edgeSummary.Counters().RelationshipsDeleted(),
		}, nil
	})
	if err != nil {
		return err
	}

	deleted := summary.([]int)

	duration := time.Since(startTime).Milliseconds()

	log.Printf("Cleanup completed in %d ms. Deleted %d stale nodes and %d stale relationships.", duration, deleted[0], deleted[1])
	return nil
}
