package tasks

import (
	"fmt"
	"github.com/gofort/dispatcher"
)

func CompoundTask() *dispatcher.Task {
	return &dispatcher.Task{
		Name: "compound",
		Args: []dispatcher.TaskArgument{
			{
				Type:  "int",
				Value: 1,
			},
			{
				Type:  "int",
				Value: 1,
			},
		},
	}
}

func CompoundTaskConfig() dispatcher.TaskConfig {
	return dispatcher.TaskConfig{
		TimeoutSeconds: 30,
		Function: func(first, second int) {
			fmt.Println(first, " ", second)
		},
	}
}
