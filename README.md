# Goliat
[![codecov](https://codecov.io/gh/filcuc/goliat/graph/badge.svg?token=NRTHurmebQ)](https://codecov.io/gh/filcuc/goliat)
[![Go Reference](https://pkg.go.dev/badge/github.com/filcuc/goliat.svg)](https://pkg.go.dev/github.com/filcuc/goliat)

`goliat` is a small, low-level Go wrapper around the SQLite C API. It provides direct access to SQLite features (including BLOB operations and prepared statements) while keeping the implementation lightweight and easy to inspect or fork.

The project aims to be simple, hackable, and easy to adapt to your needs. The full implementation is under 1000 lines of Go code. A primary advantage of `goliat` is direct access to SQLite's BLOB functions and built-in support for custom datatype binding.

## Threading

A `goliat` database connection is intended to be used by a single goroutine. If you need concurrent access, create multiple connections and manage them yourself (for example, with a connection pool or a simple "create-on-demand" strategy). Connection management is intentionally left to the caller to keep the library minimal.

## Features

- Open and close SQLite database connections.
- Execute SQL statements with parameter binding.
- Query data using iterators.
- Manage prepared statements.
- Read and write BLOBs; supports `io.Reader`, `io.ReaderAt`, and `io.Seeker`.
- Custom struct serialization/deserialization via small interface methods.
- Resource cleanup integration using `runtime.AddCleanup` (Go 1.24).

## Installation

Make sure SQLite is available on your system, then add `goliat` to your module:

```bash
go get github.com/filcuc/goliat
```

## Usage

### Opening a database

```go
db, err := goliat.Open(":memory:")
if err != nil {
    log.Fatalf("failed to open database: %v", err)
}
defer db.Close()
```

### Executing SQL statements

```go
err = db.Exec("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)")
if err != nil {
    log.Fatalf("failed to execute statement: %v", err)
}
```

### Querying a single row

```go
var bar string
err = db.QueryRow("SELECT bar FROM foo WHERE bar = ?", "baz").Scan(&bar)
if err != nil {
    log.Fatalf("query failed: %v", err)
}
fmt.Println(bar) // expects "baz"
```

### Querying multiple rows

```go
rows, err := db.Query("SELECT bar FROM foo WHERE bar = ?", "baz")
if err != nil {
    log.Fatalf("query failed: %v", err)
}
defer rows.Close()

for rows.Next() {
    var bar string
    if err := rows.Scan(&bar); err != nil {
        log.Fatalf("scan failed: %v", err)
    }
    fmt.Println(bar)
}
```

### Working with BLOBs

```go
blob, err := db.BlobOpen(goliat.DatabaseNameMain, "users", "profile_picture", 1, goliat.BlobOpenFlagsReadOnly)
if err != nil {
    log.Fatalf("failed to open BLOB: %v", err)
}
defer blob.Close()

reader := goliat.NewBlobReader(blob)
defer reader.Close()

buf := make([]byte, 1024)
n, err := reader.Read(buf)
if err != nil && err != io.EOF {
    log.Fatalf("failed to read BLOB: %v", err)
}
fmt.Printf("read %d bytes from BLOB\n", n)
```

## Custom struct serialization / deserialization

`goliat` lets you store and retrieve complex types by implementing `ToSQLiteValue` and `FromSQLiteValue` on your types. The example below shows a minimal approach.

```go
type CustomTypeTestStruct struct {
    field1 string
    field2 string
}

// ToSQLiteValue converts the struct to a SQLite-compatible value.
func (c *CustomTypeTestStruct) ToSQLiteValue() (result goliat.BindValue) {
    result.SetText(fmt.Sprintf("%s;%s", c.field1, c.field2))
    return
}

// FromSQLiteValue converts a SQLite value back to the struct.
func (c *CustomTypeTestStruct) FromSQLiteValue(value goliat.ColumnValue) error {
    text, err := value.Text()
    if err != nil {
        return err
    }
    parts := strings.Split(text, ";")
    if len(parts) != 2 {
        return fmt.Errorf("expected 2 parts, got %d", len(parts))
    }
    c.field1 = parts[0]
    c.field2 = parts[1]
    return nil
}
```

For a complete example showing database operations with custom types, see the [Custom Types Example](examples/custom_types/main.go).

## Error handling

Errors returned by the library use the `DatabaseError` type, which includes an SQLite error code and a descriptive message. Example:

```go
if err != nil {
    if dbErr, ok := err.(*goliat.DatabaseError); ok {
        fmt.Printf("SQLite Error [%d]: %s\n", dbErr.Code, dbErr.Message)
    } else {
        log.Fatalf("unexpected error: %v", err)
    }
}
```

## Contributing

We welcome contributions! Before contributing, please:

1. Read our [Contributing Guidelines](CONTRIBUTING.md)
2. Sign our [Developer Certificate of Origin](DCO.md)
3. Add yourself to the [Contributors](CONTRIBUTORS.md) list

⚠️ Important: All contributions must be submitted with GPG-signed commits and include agreement to potential future relicensing of the code.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full process and requirements.

## License

Goliat is licensed under the Apache License 2.0. This is a permissive license that lets you:

- Use this library in proprietary (closed-source) software
- Modify the library for your needs
- Distribute the library as part of your software

The main requirements are:
- Include a copy of the Apache 2.0 license
- State that you're using Goliat (attribution)
- If you modify Goliat's source code, clearly indicate your changes
- Preserve any existing copyright notices

You don't need to:
- Make your entire application open source
- Pay any fees or royalties
- Ask for permission to use in commercial projects

For the complete terms, see the [LICENSE](LICENSE) file.