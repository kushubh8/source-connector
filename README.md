# Source Connector Framework 

## Problem Statement

Design a Source Connector Framework that moves records from external systems into Kafka.

Teams currently write a producer per feed, each with its own position tracking, retries, and serialization. Your framework replaces that: a user onboards a feed by writing a config file. They write no code.

## Integrations

Two integrations are needed, and they work in opposite directions:

- **Postgres (pull):** The framework reads a table, decides when to read, and can resume from where it stopped.
- **HTTP endpoint (push):** The framework listens and callers push payloads to it. Callers decide when data arrives, and there is nothing to resume from.

## Requirements

1. **Config is the only interface a user has.** Its format, shape, and validation are your design.
2. **Postgres:** Read the table incrementally and publish the rows, resuming after a restart.
3. **HTTP:** Publish what callers push. You decide what a caller is told, and what happens when Kafka cannot keep up.
4. **One shared pipeline:** Both integrations share one path once a record has arrived — transforms, serialization, error handling, publishing, and progress tracking.
5. **Transforms are an ordered, config-driven chain.** A transform returns a record, or nothing, in which case the record is dropped.
6. **Serialization supports any data format — JSON and Avro at a minimum — chosen per feed.**
7. **Progress and guarantees:** The framework can stop or crash at any point, including between reading a record and publishing it. Decide what progress it tracks and when it records it, and state what each integration therefore guarantees.
8. **Failure handling:** Failures vary — the database goes away, Kafka rejects a write, a record is malformed. Decide how the framework tells apart the ones worth another attempt, and what control a feed's config has over what happens to a record it cannot process.
9. **Extensibility:** Adding a third kind of source, such as a file drop or a message queue, should not require changes to the pipeline.

## Constraints

- No controller layer is required. No REST API, no web server, no framework — this is a plain in-process domain library.
- Use in-memory data structures for all state. No database, no persistence, no ORM. Postgres, the callers, and Kafka are all simulated, and the stand-ins should be able to fail on demand.
- A driver entry point is provided, where you can write and run your tests/scenarios.
- Code should be modular and extendable.

## Notes

- The problem is deliberately open-ended. There is no starter code and no prescribed package layout, class names, or method signatures — deciding those is part of the exercise.
- The project ships with nothing but an empty driver entry point. Create whatever packages, modules, and classes you see fit, and use the driver to run scenarios and print results. See `INSTRUCTIONS.md` in the project for the exact file to start from.
- If you prefer proper unit tests over driver-class scenarios, you are welcome to add them alongside the driver.
- State any assumptions you make — the shape of the config, how positions are recorded, retry and backoff values, what "acknowledged" means, etc. — in code comments or a short `README.md`.
- No Avro library and no network are available, so write stand-ins for them rather than the real thing.

---

## High-Level Components

The following is only the high-level view of the implementation; keep the actual code structure as simple as the assessment allows.

```text
                    Feed Configuration
                            |
                     Config Validation
                            |
              +-------------+-------------+
              |                           |
       Postgres Source                HTTP Source
          (Pull)                        (Push)
              |                           |
              +-------------+-------------+
                            |
                    Shared Pipeline
                            |
                    Ordered Transforms
                            |
                       Serializer
                     (JSON / Avro)
                            |
                     Kafka Publisher

       Postgres Source ---- Progress / Checkpoint
       Shared Pipeline ---- Error / Retry Handling
```

### Component responsibilities

- **Config:** Defines a feed and validates source, topic, format, and transform names.
- **Postgres source:** Reads records incrementally using the saved position.
- **HTTP source:** Accepts caller-pushed records; has no source checkpoint.
- **Shared pipeline:** Runs the same processing path for records from either source.
- **Transforms:** Execute in configured order; a transform may return no record to drop it.
- **Serializer:** Converts records to the configured format (JSON or an Avro stand-in).
- **Kafka publisher:** Simulates publishing and can fail on demand.
- **Progress/checkpoint:** Tracks the Postgres source position after a successful terminal outcome.
- **Retry/error handling:** Distinguishes retryable failures from permanent failures and applies the feed's policy.

## Go Project Instructions

The provided environment uses **Go 1.22** and the standard library. The project layout shown in `INSTRUCTIONS.md` includes:

```text
main.go                 # Driver entry point
<your packages>/        # Create packages under the assignment module
*_test.go               # Optional tests, next to the code they cover
go.mod                  # Existing module definition (module name: assignment)
tools/hrreport/         # IDE test reporter — do not modify
```

### Build, run, and test

Run these commands from the project root:

```bash
gofmt -w .
go build ./...
go run .
go test ./...
```

To run the IDE test reporter:

```bash
go run ./tools/hrreport
```

You can also use the IDE's **Run** and **Test** buttons.

**Important:** Keep the existing `go.mod` and module name. Do not modify `tools/hrreport/`.
