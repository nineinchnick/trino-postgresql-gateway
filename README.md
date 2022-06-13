# trino-postgresql-gateway

A gateway/proxy for using any [PostgreSQL](https://www.postgresql.org/) client
to connect to and run SQL queries in a [Trino](https://trino.io) cluster.

It is an experiment to test Trino access in programs that do not support
the official Trino drivers, like [JDBC](https://trino.io/docs/current/installation/jdbc.html).

It does not implement 100% client functionality and it is using
[the Trino Go client](https://github.com/trinodb/trino-go-client), which itself
is not complete yet.

It does NOT parse PostgreSQL queries and expects ANSII SQL queries supported by
Trino. To fully support PostgreSQL queries, it would be better to implement it
as a [PostgreSQL Foreign Data Wrapper (FDW)](https://www.postgresql.org/docs/current/ddl-foreign-data.html)

## Usage

Start the server, pointing it to a Trino cluster:
```bash
./trino-postgresql-gateway -target http://user@localhost:8080
```

Then connect to it using any PostgreSQL client, like `psql`:
```bash
psql -h localhost -U aaa -c 'select * from tpch.tiny.nation limit 2'
```

To run the latest version directly from the `main` branch:

```bash
go run github.com/nineinchnick/trino-postgresql-gateway -listen localhost:5432 -target http://localhost:8080
```

## Limitations

### Authentication

There's no authentication for PostgreSQL clients.

Trino authentication has to be specified in the `-target` option.

### Data types

Only basic data types are supported, like `VARCHAR`, `INTEGER` and `DOUBLE`.

### Multiple queries

Simple queries are not being split, so if multiple queries are send together
they'll fail to execute.

### Prepared statements

Prepared statements are supported, but PostgreSQL uses positional placeholders
like `$1`, which are replaced with `?` using a plain regular expression.
This means it might break queries that contain strings like this in literal
values or quoted identifiers.

### Canceling queries

Trino queries execute with a fixed timeout of 1 minute and cannot be cancelled.

### Error reporting

Trino query errors are not decoded into PostgreSQL errors, so only an error
message is returned, without details like the number of query line and column
where an error ocurred.

### Metadata

Most PostgreSQL tools that support schema discovery (like listing tables)
do so by querying the PostgreSQL's [system catalogs](https://www.postgresql.org/docs/current/catalogs.html).
They're not available in Trino. To read metadata, query
the `information_schema` schema.
