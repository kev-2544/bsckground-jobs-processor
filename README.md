
# Background Job Processor

A concurrent background job processing system built with Go.

The project demonstrates worker pools, buffered channels, job tracking,
timeouts, graceful shutdown, automatic retries, exponential backoff,
and a dead letter queue.

## Architecture

```text
              Job Producer
                   |
                   v
            Buffered Job Queue
                   |
          +--------+--------+
          |        |        |
          v        v        v
       Worker 1 Worker 2 Worker 3
          |        |        |
          +--------+--------+
                   |
             Job Processing
                   |
          +--------+--------+
          |                 |
          v                 v
      Results          Retry Logic
                            |
                     Exponential Backoff
                            |
                     Maximum Attempts
                            |
                    +-------+-------+
                    |               |
                    v               v
                Completed      Dead Letter
                                  Queue
```

## Features

- Concurrent worker pool using goroutines.
- Buffered job queue using Go channels.
- Configurable worker count and queue capacity.
- Thread-safe job tracking using sync.RWMutex.
- Job status and attempt tracking.
- Per-attempt processing timeouts using context.
- Graceful shutdown on termination signals.
- Automatic retries for failed jobs.
- Exponential backoff between attempts.
- Dead Letter Queue for jobs that exhaust their attempts.
- Unit and integration tests.
- Race detector compatibility.
- Queue benchmark.

## Requirements

- Go installed on your machine.
- No external dependencies.

## Run

```powershell
go run .
```

## Test

```powershell
go test -v ./...
```

## Race Detection

```powershell
go test -race ./...
```

## Benchmark

```powershell
go test -bench=. -benchmem ./internal/queue
```

## Configuration

The demo configuration is defined in `main.go`.

| Setting | Value |
|---|---:|
| Worker count | 3 |
| Queue capacity | 10 |
| Job timeout | 3 seconds |
| Maximum attempts | 3 |
| Base retry delay | 500 milliseconds |

## Job Lifecycle

A job moves through these states:

- Pending
- Processing
- Retrying
- Completed
- Dead Letter

## Retry Behaviour

Failed jobs are retried up to the configured maximum number of attempts.

The retry delay increases exponentially:

`delay = baseBackoff * 2^(attempt - 1)`

Jobs that exhaust all attempts are sent to the Dead Letter Queue.

## Technologies

- Go
- Goroutines
- Channels
- sync.WaitGroup
- sync.RWMutex
- context
- time

## Project Scope

This is an in-memory educational implementation.

Jobs are not persisted to disk. Jobs and dead letters are lost when the application exits. The retry mechanism is synchronous within each worker, meaning a worker waits during its backoff period.

A production implementation would require additional considerations such as persistent storage, recovery after restarts, distributed workers, and more advanced retry scheduling.
