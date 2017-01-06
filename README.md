# Dispatcher

[![Go Report Card](https://goreportcard.com/badge/github.com/gofort/dispatcher)](https://goreportcard.com/report/github.com/gofort/dispatcher)
[![GoDoc](https://img.shields.io/badge/godoc-reference-blue.svg)](https://godoc.org/github.com/gofort/dispatcher)
[![TravisCI](https://travis-ci.org/gofort/dispatcher.svg?branch=master)](https://travis-ci.org/gofort/dispatcher)

### Overview

Dispatcher is an asynchronous task queue/job queue based on distributed message passing.
Dispatcher can send tasks to queue and execute them asynchronously on different servers.

Goals:
1. Reconnection ability and its configuration
2. Graceful quit (stop all workers, wait until all tasks will be finished, close connection)
4. Ability to create as many workers as we wish from only one connection
3. Simplicity
5. Ability to configure timeouts for tasks
6. Ability to limit number of parallel task for every worker

Non goals:
1. Handling results of executed tasks

### Example

```go



```

### Special thanks
1. [Richard Knop](https://github.com/RichardKnop) for his Machinery project which helped a lot in creating Dispatcher.
