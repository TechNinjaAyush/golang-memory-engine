package model

type GraphResponse struct {
	Timestamp int64    `json:"timestamp"`
	Duration  int      `json:"duration"`
	GraphType string   `json:"graphType"`
	Elements  Elements `json:"elements"`
}

type Elements struct {
	Nodes []Node `json:"nodes"`
	Edges []Edge `json:"edges"`
}

type Node struct {
	Data NodeData `json:"data"`
}

type NodeData struct {
	ID           string        `json:"id"`
	NodeType     string        `json:"nodeType"`
	Cluster      string        `json:"cluster"`
	Namespace    string        `json:"namespace"`
	App          string        `json:"app"`
	DestServices []DestService `json:"destServices,omitempty"`
	Traffic      []Traffic     `json:"traffic,omitempty"`
	HealthData   interface{}   `json:"healthData"`
	IsRoot       bool          `json:"isRoot,omitempty"`
	Pod          []Pods        `json:"pod,omitempty"`
}

type Pods struct {
	Name          string       `json:"name"`
	Container     []Containers `json:"container"`
	StatusMessage string       `json:"statusMessage,omitempty"`
	Status        string       `json:"status"`
}

type Containers struct {
	ContainerName string `json:"containerName,omitempty"`
	Logs          string `json:"logs,omitempty"`
	Status        string `json:"status,omitempty"`
	Reason        string `json:"reason,omitempty"`
	ExitCode      int32  `json:"exitCode,omitempty"`
}

type DestService struct {
	Cluster   string `json:"cluster"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type Traffic struct {
	Protocol string            `json:"protocol"`
	Rates    map[string]string `json:"rates"`
}

type Edge struct {
	Data EdgeData `json:"data"`
}

type EdgeData struct {
	ID              string      `json:"id"`
	Source          string      `json:"source"`
	Target          string      `json:"target"`
	DestPrincipal   string      `json:"destPrincipal"`
	SourcePrincipal string      `json:"sourcePrincipal"`
	IsMTLS          string      `json:"isMTLS"`
	ResponseTime    string      `json:"responseTime,omitempty"`
	Throughput      string      `json:"throughput,omitempty"`
	Traffic         EdgeTraffic `json:"traffic"`
}

type EdgeTraffic struct {
	Protocol  string                    `json:"protocol"`
	Rates     map[string]string         `json:"rates"`
	Responses map[string]ResponseDetail `json:"responses"`
}

type ResponseDetail struct {
	Flags map[string]string `json:"flags"`
	Hosts map[string]string `json:"hosts"`
}
