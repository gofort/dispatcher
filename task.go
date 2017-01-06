package dispatcher

// Task is a task which can be send to AMQP.
// Workers receive this tasks and handles them via parsing their arguments.
// You can pass exchange and routing key to task if you want, they will be used in publish function.
type Task struct {
	UUID       string                 `json:"uuid"`
	Name       string                 `json:"name"`
	RoutingKey string                 `json:"-"`
	Args       []TaskArgument         `json:"args"`
	Headers    map[string]interface{} `json:"headers"`
}

// TaskArgument is an argument which will be passed to function.
// For example, task with such arguments will call the following function:
//
// Arguments:
//  t := []TaskArgument{
//	  TaskArgument{
//	  	Type: "int",
//	  	Value: 3,
//	  },
//	  TaskArgument{
//	  	Type: "string",
//	  	Value: "I am a string",
//	  },
//  }
//
// Function:
//   func (myInt int, myAwesomeString string) error {}
//
// Types which can be used: bool, string, int int8 int16 int32 int64, uint uint8 uint16 uint32 uint64, float32 float64
type TaskArgument struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}
