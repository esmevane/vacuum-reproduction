# Sqlx vacuum issue reproduction

This is a reproduction repo that demonstrates a few different attempts to create a sqlite memory database and then vacuum its contents into a file.

1. An attempt with `sqlx` using a shared cache connection, which silently fails.
2. An attempt with `sqlx` with a shared cache pool with a connection limit of 1, which silently fails.
3. An attempt with `rusqlite` as a direct attempt, which succeeds.

## Usage

The reproduction is done with three functions that make a temporary directory, which can be run directly or with tests.

To run directly:

```sh
cargo run
```

To run via tests:

```sh
cargo test
```
