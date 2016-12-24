package project_1

type WorkerConfig struct {
	Limit        int // Parallel tasks limit
	Exchange     string // dispatcher
	ExchangeType string // direct
	DefaultQueue string // scalet_backup, scalet_management, scalet_create
	BindingKey   string // If no routing key in task - this will be used, it is like default routing key for all tasks
	Tasks        map[string]TaskConfig
}

type TaskConfig struct {
	Timeout int64 // in seconds
	Function interface{}
}


