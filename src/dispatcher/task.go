package project_1

type TaskArgument struct {
	Type  string
	Value interface{}
}

type Task struct {
	UUID           string
	Name           string
	RoutingKey     string
	Args           []TaskArgument
	Headers        map[string]interface{} // needed for AMQP
}

