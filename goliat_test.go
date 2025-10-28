// Copyright 2025 Filippo Cucchetto
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package goliat_test

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/filcuc/goliat"
	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	db, err := goliat.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
}

func TestExecFailure(t *testing.T) {
	db, err := goliat.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Exec("foo")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestExecSuccess(t *testing.T) {
	db, err := goliat.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Exec("CREATE TABLE foo (bar)")
	if err != nil {
		t.Fatal(err)
	}
}

func TestPrepareFailure(t *testing.T) {
	db, err := goliat.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Prepare("foo")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPrepareSuccess(t *testing.T) {
	db, err := goliat.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Prepare("CREATE TABLE foo (bar)")
	if err != nil {
		t.Fatal(err)
	}
}

func TestLastInsertedId(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	stmt, err := db.Prepare("CREATE TABLE foo (bar)")
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())

	stmt, err = db.Prepare("INSERT INTO foo (bar) VALUES (1)")
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())
}

func TestStatementStep(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	stmt, err := db.Prepare("CREATE TABLE User (name TEXT)")
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())

	stmt, err = db.Prepare("INSERT INTO User (name) VALUES ('foo')")
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())

	stmt, err = db.Prepare("SELECT name from User")
	assert.NoError(t, err)
	assert.Equal(t, goliat.ROW, stmt.Step())
	assert.Equal(t, goliat.DONE, stmt.Step())
}

func TestStatementBindAndColumn(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	err = db.Exec("CREATE TABLE User (name TEXT, age INTEGER, height REAL, picture BLOB, nilBlob BLOB, verified INTEGER)")
	assert.NoError(t, err)

	err = db.Exec("INSERT INTO User (name, age, height, picture, nilBlob, verified) VALUES (?, ?, ?, ?, ?, ?)",
		"foo", 1, 1.1, []byte{0, 1, 2, 3}, nil, true)
	assert.NoError(t, err)

	row := db.QueryRow("SELECT name, age, height, picture, nilBlob, verified from User")
	assert.NotNil(t, row)

	var name string
	var age int
	var height float64
	var picture []byte
	var nilBlob []byte
	var verified bool
	assert.NoError(t, row.Scan(&name, &age, &height, &picture, &nilBlob, &verified))
	assert.Equal(t, "foo", name)
	assert.Equal(t, 1, age)
	assert.Equal(t, 1.1, height)
	assert.Equal(t, []byte{0, 1, 2, 3}, picture)
	assert.Nil(t, nilBlob)
}

func TestBlobOpen(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	stmt, err := db.Prepare("CREATE TABLE User (name TEXT, picture BLOB)")
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())

	stmt, err = db.Prepare("INSERT INTO User (name, picture) VALUES (?, ?)")
	expectedData := []byte{0, 1, 2, 3}
	stmt.Bind("foo", expectedData)
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())

	blob, err := db.BlobOpen(goliat.DatabaseNameMain, "User", "picture", 1, goliat.BlobOpenFlagsReadOnly)
	assert.NoError(t, err)
	assert.NotNil(t, blob)
	defer blob.Close()

	assert.Equal(t, len(expectedData), blob.Bytes())

	actualData, err := blob.Read(0, len(expectedData))
	assert.NoError(t, err)
	assert.Equal(t, expectedData, actualData)
}

func TestBlobOpenError(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	_, err = db.BlobOpen(goliat.DatabaseNameMain, "User", "picture", 1, goliat.BlobOpenFlagsReadOnly)
	assert.Error(t, err)
}

func TestBlobReader(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	stmt, err := db.Prepare("CREATE TABLE User (name TEXT, picture BLOB)")
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())

	stmt, err = db.Prepare("INSERT INTO User (name, picture) VALUES (?, ?)")
	expectedData := []byte{0, 1, 2, 3}
	stmt.Bind("foo", expectedData)
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())

	blob, err := db.BlobOpen(goliat.DatabaseNameMain, "User", "picture", 1, goliat.BlobOpenFlagsReadOnly)
	assert.NoError(t, err)
	defer blob.Close()

	reader := goliat.NewBlobReader(blob)
	defer reader.Close()

	actualData, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, expectedData, actualData)
}

func TestBlobReaderSeek(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	stmt, err := db.Prepare("CREATE TABLE User (name TEXT, picture BLOB)")
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())

	stmt, err = db.Prepare("INSERT INTO User (name, picture) VALUES (?, ?)")
	expectedData := []byte{0, 1, 2, 3}
	stmt.Bind("foo", expectedData)
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())

	blob, err := db.BlobOpen(goliat.DatabaseNameMain, "User", "picture", 1, goliat.BlobOpenFlagsReadOnly)
	assert.NoError(t, err)
	defer blob.Close()

	reader := goliat.NewBlobReader(blob)
	defer reader.Close()

	offset, err := reader.Seek(1, io.SeekStart)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), offset)
	actualData, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, expectedData[1:], actualData)
}

func TestBlobReadAt(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	stmt, err := db.Prepare("CREATE TABLE User (name TEXT, picture BLOB)")
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())

	stmt, err = db.Prepare("INSERT INTO User (name, picture) VALUES (?, ?)")
	expectedData := []byte{0, 1, 2, 3}
	stmt.Bind("foo", expectedData)
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())

	blob, err := db.BlobOpen(goliat.DatabaseNameMain, "User", "picture", 1, goliat.BlobOpenFlagsReadOnly)
	assert.NoError(t, err)
	defer blob.Close()

	reader := goliat.NewBlobReader(blob)
	defer reader.Close()

	offset, err := reader.Seek(1, io.SeekStart)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), offset)
	actualData := make([]byte, 2)
	_, err = reader.ReadAt(actualData, 1)
	assert.NoError(t, err)
	assert.Equal(t, expectedData[1:3], actualData)
}

func TestBlobClose(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	stmt, err := db.Prepare("CREATE TABLE User (name TEXT, picture BLOB)")
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())

	stmt, err = db.Prepare("INSERT INTO User (name, picture) VALUES (?, ?)")
	expectedData := []byte{0, 1, 2, 3}
	stmt.Bind("foo", expectedData)
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())

	blob, err := db.BlobOpen(goliat.DatabaseNameMain, "User", "picture", 1, goliat.BlobOpenFlagsReadOnly)
	assert.NoError(t, err)
	defer blob.Close()
}

func TestLastInsertRowId(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	stmt, err := db.Prepare("CREATE TABLE foo (bar)")
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())

	stmt, err = db.Prepare("INSERT INTO foo (bar) VALUES (?)")
	stmt.Bind("baz")
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())
	assert.Equal(t, int64(1), db.LastInsertRowId())
}

func TestQueryApi(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	err = db.Exec("CREATE TABLE foo (bar)")
	assert.NoError(t, err)

	err = db.Exec("INSERT INTO foo (bar) VALUES (?)", "baz")
	assert.NoError(t, err)

	rows, err := db.Query("SELECT bar FROM foo WHERE bar = ?", "baz")
	assert.NoError(t, err)

	for rows.Next() {
		var bar string
		err = rows.Scan(&bar)
		assert.NoError(t, err)
		assert.Equal(t, "baz", bar)
	}
}

type CustomTypeTestSruct struct {
	field1 string
	field2 string
}

func (c *CustomTypeTestSruct) ToSQLiteValue() (result goliat.BindValue) {
	result.SetText(fmt.Sprintf("%s;%s", c.field1, c.field2))
	return
}

func (c *CustomTypeTestSruct) FromSQLiteValue(value goliat.ColumnValue) error {
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

func TestCustomTypesHandling(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	err = db.Exec("CREATE TABLE foo (bar TEXT)")
	assert.NoError(t, err)

	expectedStruct := &CustomTypeTestSruct{
		field1: "foo",
		field2: "bar",
	}

	err = db.Exec("INSERT INTO foo (bar) VALUES (?)", expectedStruct)
	assert.NoError(t, err)

	rows, err := db.Query("SELECT bar FROM foo WHERE bar = ?", expectedStruct)
	assert.NoError(t, err)
	assert.True(t, rows.Next())

	var actualStruct CustomTypeTestSruct
	err = rows.Scan(&actualStruct)
	assert.NoError(t, err)
	assert.Equal(t, expectedStruct.field1, actualStruct.field1)
	assert.Equal(t, expectedStruct.field2, actualStruct.field2)
}

func TestQueryRow(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	err = db.Exec("CREATE TABLE foo (bar TEXT)")
	assert.NoError(t, err)
	err = db.Exec("INSERT INTO foo (bar) VALUES (?)", "baz")
	assert.NoError(t, err)

	var bar string
	err = db.QueryRow("SELECT bar FROM foo WHERE bar = ?", "baz").Scan(&bar)
	assert.NoError(t, err)
	assert.Equal(t, "baz", bar)
}

func TestChanges(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	err = db.Exec("CREATE TABLE foo (bar TEXT)")
	assert.NoError(t, err)

	stmt, err := db.Prepare("INSERT INTO foo (bar) VALUES (?)")
	assert.NoError(t, err)
	stmt.Bind("baz")
	assert.NoError(t, err)
	assert.Equal(t, goliat.DONE, stmt.Step())
	assert.Equal(t, int64(1), db.Changes())
}

func TestTransactionRollback(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	err = db.Exec("CREATE TABLE foo (bar TEXT)")
	assert.NoError(t, err)

	tx, err := db.BeginTransaction()
	assert.NoError(t, err)
	err = db.Exec("INSERT INTO foo (bar) VALUES (?)", "baz")
	assert.NoError(t, err)

	err = tx.Rollback()
	assert.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM foo WHERE bar = ?", "baz").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestTransactionCommit(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	err = db.Exec("CREATE TABLE foo (bar TEXT)")
	assert.NoError(t, err)

	tx, err := db.BeginTransaction()
	assert.NoError(t, err)
	err = db.Exec("INSERT INTO foo (bar) VALUES (?)", "baz")
	assert.NoError(t, err)

	err = tx.Commit()
	assert.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM foo WHERE bar = ?", "baz").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestQueryRowShouldErrorNotFoundIfNoResult(t *testing.T) {
	db, err := goliat.Open(":memory:")
	assert.NoError(t, err)
	defer db.Close()

	err = db.Exec("CREATE TABLE foo (bar TEXT)")
	assert.NoError(t, err)

	var bar string
	err = db.QueryRow("SELECT bar FROM foo WHERE bar = ?", "nonexistent").Scan(&bar)
	assert.Error(t, err)
	assert.ErrorIs(t, err, goliat.ErrNoRows)
}
