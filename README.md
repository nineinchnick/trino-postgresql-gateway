# trino-postgresql-gateway

A gateway/proxy for using any [PostgreSQL](https://www.postgresql.org/) client
to connect to and run SQL queries in a [Trino](https://trino.io) cluster.

It is an experiment to test Trino access in programs that do not support
the official Trino drivers, like [JDBC](https://trino.io/docs/current/installation/jdbc.html).

It does not implement 100% client functionality and it is using
[the Trino Go client](https://github.com/trinodb/trino-go-client), which itself
is not complete yet.

## Usage

Since there are no releases yet, it needs to be built before running, so
a working installation of Go >=1.18 is required:

```bash
go run github.com/nineinchnick/trino-postgresql-gateway -listen localhost:5432 -target localhost:8080
```

## Authentication

There's no authentication for PostgreSQL clients.

Trino authentication has to be specified in the `-target` option.
