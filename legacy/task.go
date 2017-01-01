package dispatcher

// Arguments types:
// bool
// string
// int int8 int16 int32 int64
// uint uint8 uint16 uint32 uint64
// float32 float64

type TaskArgument struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

type Task struct {
	UUID       string                 `json:"uuid"`
	Name       string                 `json:"name"`
	RoutingKey string                 `json:"routing_key"`
	Exchange   string                 `json:"exchange"`
	Args       []TaskArgument         `json:"args"`
	Headers    map[string]interface{} `json:"headers"` // needed for AMQP
}
