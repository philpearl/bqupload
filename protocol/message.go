package protocol

import "github.com/philpearl/plenc/plenccodec"

// ConnectionDescriptor describes a connection to a BigQuery table. It is sent
// at the start of each connection to a BQUpload server.
type ConnectionDescriptor struct {
	ProjectID  string                `json:"projectID,omitempty" plenc:"1"`
	DataSetID  string                `json:"dataSetID,omitempty" plenc:"2"`
	TableName  string                `json:"tableName,omitempty" plenc:"3"`
	Descriptor plenccodec.Descriptor `json:"descriptor,omitempty" plenc:"4"`
}
