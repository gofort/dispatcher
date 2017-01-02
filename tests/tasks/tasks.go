package tasks

import (
	"fmt"
	"github.com/gofort/dispatcher"
	"github.com/satori/go.uuid"
	"time"
)

func Test1Task() *dispatcher.Task {

	t := dispatcher.Task{
		Name: "test_1",
		UUID: uuid.NewV4().String(),
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

	return &t
}

func Test1TaskConfig() dispatcher.TaskConfig {
	return dispatcher.TaskConfig{
		TimeoutSeconds: 30,
		Function: func(first, second int) {
			fmt.Println("Test 1 task started")
			time.Sleep(time.Second * 3)
			fmt.Println("Test 1 task result: ", first, " ", second)
		},
	}
}

func Test2Task() *dispatcher.Task {
	t := dispatcher.Task{
		Name: "test_2",
		UUID: uuid.NewV4().String(),
		Args: []dispatcher.TaskArgument{
			{
				Type:  "int",
				Value: 2,
			},
			{
				Type:  "int",
				Value: 2,
			},
		},
	}
	return &t
}

func Test2TaskConfig() dispatcher.TaskConfig {
	return dispatcher.TaskConfig{
		TimeoutSeconds: 30,
		Function: func(first, second int) {
			fmt.Println("Test 2 task started")
			time.Sleep(time.Second * 4)
			fmt.Println("Test 2 task result: ", first, " ", second)
		},
	}
}

func Test3Task() *dispatcher.Task {
	t := dispatcher.Task{
		Name: "test_3",
		UUID: uuid.NewV4().String(),
		Args: []dispatcher.TaskArgument{
			{
				Type:  "int",
				Value: 2,
			},
			{
				Type:  "int",
				Value: 2,
			},
		},
	}
	return &t
}

func Test3TaskConfig() dispatcher.TaskConfig {
	return dispatcher.TaskConfig{
		TimeoutSeconds: 30,
		Function: func(first, second int) {
			fmt.Println("Test 2 task started")
			time.Sleep(time.Second * 7)
			fmt.Println("Test 2 task result: ", first, " ", second)
		},
	}
}
