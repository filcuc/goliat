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

package goliat

/*
#cgo CFLAGS: -I.
#cgo LDFLAGS: -lm
#include "sqlite3.h"
#include <stdlib.h>
const sqlite3_destructor_type transient = (sqlite3_destructor_type)-1;
const sqlite3_destructor_type staticv   = (sqlite3_destructor_type)0;
const int sqlite3_datatype_integer = SQLITE_INTEGER;
const int sqlite3_datatype_text    = SQLITE_TEXT;
const int sqlite3_datatype_blob    = SQLITE_BLOB;
const int sqlite3_datatype_float    = SQLITE_FLOAT;
const int sqlite3_datatype_null    = SQLITE_NULL;
*/
import "C"

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"unsafe"
)

// ErrorCode represents a SQLite result code.
type ErrorCode int

const (
	OK         ErrorCode = C.SQLITE_OK         // 0
	ERROR      ErrorCode = C.SQLITE_ERROR      // 1
	INTERNAL   ErrorCode = C.SQLITE_INTERNAL   // 2
	PERM       ErrorCode = C.SQLITE_PERM       // 3
	ABORT      ErrorCode = C.SQLITE_ABORT      // 4
	BUSY       ErrorCode = C.SQLITE_BUSY       // 5
	LOCKED     ErrorCode = C.SQLITE_LOCKED     // 6
	NOMEM      ErrorCode = C.SQLITE_NOMEM      // 7
	READONLY   ErrorCode = C.SQLITE_READONLY   // 8
	INTERRUPT  ErrorCode = C.SQLITE_INTERRUPT  // 9
	IOERR      ErrorCode = C.SQLITE_IOERR      // 10
	CORRUPT    ErrorCode = C.SQLITE_CORRUPT    // 11
	NOTFOUND   ErrorCode = C.SQLITE_NOTFOUND   // 12
	FULL       ErrorCode = C.SQLITE_FULL       // 13
	CANTOPEN   ErrorCode = C.SQLITE_CANTOPEN   // 14
	PROTOCOL   ErrorCode = C.SQLITE_PROTOCOL   // 15
	EMPTY      ErrorCode = C.SQLITE_EMPTY      // 16
	SCHEMA     ErrorCode = C.SQLITE_SCHEMA     // 17
	TOOBIG     ErrorCode = C.SQLITE_TOOBIG     // 18
	CONSTRAINT ErrorCode = C.SQLITE_CONSTRAINT // 19
	MISMATCH   ErrorCode = C.SQLITE_MISMATCH   // 20
	MISUSE     ErrorCode = C.SQLITE_MISUSE     // 21
	NOLFS      ErrorCode = C.SQLITE_NOLFS      // 22
	AUTH       ErrorCode = C.SQLITE_AUTH       // 23
	FORMAT     ErrorCode = C.SQLITE_FORMAT     // 24
	RANGE      ErrorCode = C.SQLITE_RANGE      // 25
	NOTADB     ErrorCode = C.SQLITE_NOTADB     // 26
	NOTICE     ErrorCode = C.SQLITE_NOTICE     // 27
	WARNING    ErrorCode = C.SQLITE_WARNING    // 28
	ROW        ErrorCode = C.SQLITE_ROW        // 100
	DONE       ErrorCode = C.SQLITE_DONE       // 101
)

type ZeroBlob struct {
	Size uint64
}

type DatabaseError struct {
	Code    ErrorCode
	Message string
}

func (e *DatabaseError) Error() string {
	return fmt.Sprintf("%d: %s", e.Code, e.Message)
}

func newDatabaseError(code ErrorCode, message string) *DatabaseError {
	return &DatabaseError{Code: code, Message: message}
}

var ErrNoRows = io.EOF

type stringHandle struct {
	ptr *C.char
}

func (h *stringHandle) close() error {
	if h.ptr == nil {
		return nil
	}
	C.free(unsafe.Pointer(h.ptr))
	h.ptr = nil
	return nil
}

type databaseString struct {
	h *stringHandle
}

func (h *databaseString) Close() error {
	return h.h.close()
}

func newDatabaseString(s string) *databaseString {
	result := &databaseString{h: &stringHandle{C.CString(s)}}
	runtime.AddCleanup(result, func(h *stringHandle) {
		h.close()
	}, result.h)
	return result
}

type connectionHandle struct {
	ptr *C.sqlite3
}

func (h *connectionHandle) Close() error {
	if h.ptr == nil {
		return nil
	}
	ec := C.sqlite3_close_v2(h.ptr)
	if ec != C.SQLITE_OK {
		return newDatabaseError(ErrorCode(ec), "failed to close database")
	}
	h.ptr = nil
	return nil
}

type statementHandle struct {
	ptr *C.sqlite3_stmt
}

func (h *statementHandle) close() error {
	if h.ptr == nil {
		return nil
	}
	C.sqlite3_finalize(h.ptr)
	h.ptr = nil
	return nil
}

type Statement struct {
	db *Connection
	h  *statementHandle
}

func (h *Statement) Close() error {
	return h.h.close()
}

func newDatabaseStatement(db *Connection, handle *statementHandle) *Statement {
	result := &Statement{db: db, h: handle}
	runtime.AddCleanup(result, func(h *statementHandle) {
		h.close()
	}, result.h)
	return result
}

func (s *Statement) Reset() error {
	ec := C.sqlite3_reset(s.h.ptr)
	if ec != C.SQLITE_OK {
		return s.db.newDatabaseError()
	}
	return nil
}

func (s *Statement) ClearBindings() error {
	ec := C.sqlite3_clear_bindings(s.h.ptr)
	if ec != C.SQLITE_OK {
		return s.db.newDatabaseError()
	}
	return nil
}

type Connection struct {
	h *connectionHandle
}

func newDatabaseConnection(handle *connectionHandle) *Connection {
	result := &Connection{h: handle}
	runtime.AddCleanup(result, func(h *connectionHandle) {
		h.Close()
	}, result.h)
	return result
}

// Open opens a SQLite database file (creates it if it doesn’t exist)
func Open(filename string) (*Connection, error) {
	cfilename := newDatabaseString(filename)
	defer cfilename.Close()

	handle := connectionHandle{ptr: nil}
	if ec := C.sqlite3_open(cfilename.h.ptr, &handle.ptr); ec != C.SQLITE_OK {
		return nil, &DatabaseError{
			Code:    ErrorCode(ec),
			Message: "failed to open database",
		}
	}

	return newDatabaseConnection(&handle), nil
}

// Close closes the SQLite database
func (d *Connection) Close() error {
	return d.h.Close()
}

func (d *Connection) LastErrorCode() ErrorCode {
	return ErrorCode(C.sqlite3_errcode(d.h.ptr))
}

func (d *Connection) LastErrorMessage() string {
	return C.GoString(C.sqlite3_errmsg(d.h.ptr))
}

func (d *Connection) newDatabaseError() *DatabaseError {
	return newDatabaseError(d.LastErrorCode(), d.LastErrorMessage())
}

func (d *Connection) Changes() int64 {
	return int64(C.sqlite3_changes64(d.h.ptr))
}

func (d *Connection) LastInsertRowId() int64 {
	return int64(C.sqlite3_last_insert_rowid(d.h.ptr))
}

func (d *Connection) Exec(sql string, values ...any) error {
	stmt, err := d.Prepare(sql)
	if err != nil {
		return err
	}
	defer stmt.Close()
	err = stmt.Bind(values...)
	if err != nil {
		return err
	}
	for {
		switch stmt.Step() {
		case DONE:
			return nil
		case ROW:
			continue
		default:
			return d.newDatabaseError()
		}
	}
}

type QueryIterator struct {
	stmt *Statement
	err  error
	done bool
}

func (d *Connection) Query(sql string, args ...any) (*QueryIterator, error) {
	stmt, err := d.Prepare(sql)
	if err != nil {
		return nil, err
	}

	if len(args) > 0 {
		if err := stmt.Bind(args...); err != nil {
			stmt.Close()
			return nil, err
		}
	}

	result := &QueryIterator{stmt: stmt}

	runtime.AddCleanup(result, func(h *Statement) {
		h.Close()
	}, result.stmt)

	return result, nil
}

func (r *QueryIterator) Next() bool {
	if r.done || r.err != nil {
		return false
	}

	ec := r.stmt.Step()

	switch ec {
	case ROW:
		return true
	case DONE:
		r.done = true
		return false
	default:
		r.err = newDatabaseError(ErrorCode(ec), "failed to step query")
		r.done = true
		return false
	}
}

func (r *QueryIterator) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if !r.done {
		return r.stmt.Column(dest...)
	}
	return ErrNoRows
}

func (r *QueryIterator) Close() error {
	return r.stmt.Close()
}

type QueryRowResult struct {
	iterator *QueryIterator
	err      error
}

func (d *Connection) QueryRow(sql string, args ...any) *QueryRowResult {
	iterator, err := d.Query(sql, args...)
	return &QueryRowResult{iterator: iterator, err: err}
}

func (r *QueryRowResult) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	defer r.iterator.Close()
	if r.iterator.Next() {
		return r.iterator.Scan(dest...)
	}
	if r.iterator.err != nil {
		return r.iterator.err
	}
	return ErrNoRows
}

func (d *Connection) Prepare(sql string) (*Statement, error) {
	sqlRaw := newDatabaseString(sql)
	defer sqlRaw.Close()

	handle := statementHandle{ptr: nil}

	ec := C.sqlite3_prepare_v2(d.h.ptr, sqlRaw.h.ptr, -1, &handle.ptr, nil)
	if ec != C.SQLITE_OK {
		return nil, d.newDatabaseError()
	}

	return newDatabaseStatement(d, &handle), nil
}

func (s *Statement) Step() ErrorCode {
	return ErrorCode(C.sqlite3_step(s.h.ptr))
}

func (s *Statement) BindCount() int {
	return int(C.sqlite3_bind_parameter_count(s.h.ptr))
}

func (s *Statement) ColumnCount() int {
	return int(C.sqlite3_column_count(s.h.ptr))
}

type BindValue struct {
	value any
}

func (b *BindValue) SetText(value string) {
	b.value = value
}

func (b *BindValue) SetInt(value int) {
	b.value = value
}

func (b *BindValue) SetInt64(value int64) {
	b.value = value
}

func (b *BindValue) SetFloat64(value float64) {
	b.value = value
}

func (b *BindValue) SetBlob(value []byte) {
	b.value = value
}

func (b *BindValue) SetNull() {
	b.value = nil
}

func (b *BindValue) SetZeroBlob(size uint64) {
	b.value = ZeroBlob{Size: size}
}

type BindHandler interface {
	ToSQLiteValue() BindValue
}

func (stmt *Statement) Bind(values ...any) error {
	if len(values) != stmt.BindCount() {
		return fmt.Errorf("wrong number of values %d != %d", len(values), stmt.BindCount())
	}
	for i, value := range values {
		index := i + 1
		err := stmt.BindValue(index, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (stmt *Statement) BindValue(index int, value any) error {
	switch v := value.(type) {
	case bool:
		C.sqlite3_bind_int(stmt.h.ptr, C.int(index), C.int(boolToInt(v)))
	case int:
		C.sqlite3_bind_int(stmt.h.ptr, C.int(index), C.int(v))
	case int64:
		C.sqlite3_bind_int64(stmt.h.ptr, C.int(index), C.sqlite3_int64(int64(v)))
	case float64:
		C.sqlite3_bind_double(stmt.h.ptr, C.int(index), C.double(v))
	case []byte:
		if len(v) == 0 {
			C.sqlite3_bind_blob(stmt.h.ptr, C.int(index), nil, 0, C.transient)
		} else {
			C.sqlite3_bind_blob(stmt.h.ptr, C.int(index), unsafe.Pointer(&v[0]), C.int(len(v)), C.transient)
		}
	case string:
		cvalue := newDatabaseString(v)
		defer cvalue.Close()
		C.sqlite3_bind_text(stmt.h.ptr, C.int(index), cvalue.h.ptr, -1, C.transient)
	case nil:
		C.sqlite3_bind_null(stmt.h.ptr, C.int(index))
	case ZeroBlob:
		C.sqlite3_bind_zeroblob64(stmt.h.ptr, C.int(index), C.sqlite3_uint64(v.Size))
	default:
		if handler, ok := value.(BindHandler); ok {
			return stmt.BindValue(index, handler.ToSQLiteValue().value)
		}
		return fmt.Errorf("unknown type %T", value)
	}

	return nil
}

type ColumnValue struct {
	datatype int
	stmt     *Statement
	index    int
}

func (c ColumnValue) IsNull() bool {
	return c.datatype == int(C.sqlite3_datatype_null)
}

func (c ColumnValue) IsInteger() bool {
	return c.datatype == int(C.sqlite3_datatype_integer)
}

func (c ColumnValue) IsFloat() bool {
	return c.datatype == int(C.sqlite3_datatype_float)
}

func (c ColumnValue) IsText() bool {
	return c.datatype == int(C.sqlite3_datatype_text)
}

func (c ColumnValue) IsBlob() bool {
	return c.datatype == int(C.sqlite3_datatype_blob)
}

func (c ColumnValue) ToFloat() (float64, error) {
	if !c.IsFloat() {
		return 0, fmt.Errorf("not a float")
	}
	return float64(C.sqlite3_column_double(c.stmt.h.ptr, C.int(c.index))), nil
}

func (c ColumnValue) Integer() (int64, error) {
	if !c.IsInteger() {
		return 0, fmt.Errorf("not an integer")
	}
	return int64(C.sqlite3_column_int64(c.stmt.h.ptr, C.int(c.index))), nil
}

func (c ColumnValue) Text() (string, error) {
	if c.IsNull() {
		return "", nil
	}
	if !c.IsText() {
		return "", fmt.Errorf("not a text")
	}
	textPtr := C.sqlite3_column_text(c.stmt.h.ptr, C.int(c.index))
	if textPtr != nil {
		return C.GoString((*C.char)(unsafe.Pointer(textPtr))), nil
	} else {
		return "", nil
	}
}

func (c ColumnValue) Blob() ([]byte, error) {
	if c.IsNull() {
		return nil, nil
	}
	if !c.IsBlob() {
		return nil, fmt.Errorf("not a blob")
	}
	return C.GoBytes(unsafe.Pointer(C.sqlite3_column_blob(c.stmt.h.ptr, C.int(c.index))), C.sqlite3_column_bytes(c.stmt.h.ptr, C.int(c.index))), nil
}

type ColumnHandler interface {
	FromSQLiteValue(value ColumnValue) error
}

func (stmt *Statement) Column(values ...any) error {
	if len(values) != stmt.ColumnCount() {
		return fmt.Errorf("wrong number of values %d != %d", len(values), stmt.ColumnCount())
	}

	for i, value := range values {
		err := stmt.columnValue(i, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (stmt *Statement) columnDatatype(i int) int {
	return int(C.sqlite3_column_type(stmt.h.ptr, C.int(i)))
}

func (stmt *Statement) columnValue(i int, value any) error {
	switch v := value.(type) {
	case *bool:
		intVal := C.sqlite3_column_int(stmt.h.ptr, C.int(i))
		*v = intVal != 0
	case *int:
		*v = int(C.sqlite3_column_int(stmt.h.ptr, C.int(i)))
	case *int64:
		*v = int64(C.sqlite3_column_int64(stmt.h.ptr, C.int(i)))
	case *string:
		textPtr := C.sqlite3_column_text(stmt.h.ptr, C.int(i))
		if textPtr != nil {
			*v = C.GoString((*C.char)(unsafe.Pointer(textPtr)))
		} else {
			*v = ""
		}
	case *[]byte:
		data := C.sqlite3_column_blob(stmt.h.ptr, C.int(i))
		dataLen := C.sqlite3_column_bytes(stmt.h.ptr, C.int(i))
		if dataLen == 0 {
			*v = nil
		} else {
			*v = C.GoBytes(unsafe.Pointer(data), dataLen)
		}
	case *float32:
		return fmt.Errorf("use float64 instead of float32 %T", value)
	case *float64:
		*v = float64(C.sqlite3_column_double(stmt.h.ptr, C.int(i)))
	default:
		if handler, ok := value.(ColumnHandler); ok {
			return handler.FromSQLiteValue(ColumnValue{
				datatype: stmt.columnDatatype(i),
				stmt:     stmt,
				index:    i,
			})
		} else {
			return fmt.Errorf("unsupported type %T", value)
		}
	}

	return nil
}

type blobHandle struct {
	ptr *C.sqlite3_blob
}

func (b *blobHandle) Close() {
	if b.ptr == nil {
		return
	}
	C.sqlite3_blob_close(b.ptr)
	b.ptr = nil
}

type Blob struct {
	db *Connection
	h  *blobHandle
}

func newDatabaseBlob(db *Connection, ptr *C.sqlite3_blob) *Blob {
	result := &Blob{db: db, h: &blobHandle{ptr}}
	runtime.AddCleanup(result, func(h *blobHandle) {
		h.Close()
	}, result.h)
	return result
}

func (b *Blob) Close() {
	b.h.Close()
}

type DatabaseName string

const DatabaseNameMain = DatabaseName("main")
const DatabaseNameTemp = DatabaseName("temp")

type BlobOpenFlags int

const BlobOpenFlagsReadOnly = BlobOpenFlags(0)
const BlobOpenFlagsReadWrite = BlobOpenFlags(1)

func (d *Connection) BlobOpen(databaseName DatabaseName, tableName string, columnName string, rowId int64, flags BlobOpenFlags) (*Blob, error) {
	databaseNameRaw := newDatabaseString(string(databaseName))
	defer databaseNameRaw.Close()
	tableNameRaw := newDatabaseString(tableName)
	defer tableNameRaw.Close()
	columnNameRaw := newDatabaseString(columnName)
	defer columnNameRaw.Close()
	rowIdRaw := C.sqlite3_int64(rowId)
	flagsRaw := C.int(int(flags))

	blob := newDatabaseBlob(d, nil)
	ec := C.sqlite3_blob_open(d.h.ptr, databaseNameRaw.h.ptr, tableNameRaw.h.ptr, columnNameRaw.h.ptr, rowIdRaw, flagsRaw, &blob.h.ptr)
	if ec != C.SQLITE_OK {
		return nil, d.newDatabaseError()
	}
	return blob, nil
}

func (b *Blob) Bytes() int {
	return int(C.sqlite3_blob_bytes(b.h.ptr))
}

func (b *Blob) Read(offset int, length int) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}

	if offset+length > b.Bytes() {
		return nil, newDatabaseError(ErrorCode(C.SQLITE_ERROR), fmt.Sprintf("offset %d + length %d = %d exceed blob size %d", offset, length, offset+length, b.Bytes()))
	}

	offsetRaw := C.int(offset)
	lengthRaw := C.int(length)

	result := make([]byte, length)
	ec := C.sqlite3_blob_read(b.h.ptr, unsafe.Pointer(&result[0]), lengthRaw, offsetRaw)
	var err error = nil
	if ec != C.SQLITE_OK {
		err = b.db.newDatabaseError()
	}
	return result, err
}

type BlobReader struct {
	blob   *Blob
	offset int
}

func NewBlobReader(blob *Blob) *BlobReader {
	result := &BlobReader{blob: blob, offset: 0}
	runtime.AddCleanup(result, func(h *Blob) {
		h.Close()
	}, result.blob)
	return result
}

func (r *BlobReader) Read(p []byte) (int, error) {
	if r.blob == nil || r.blob.h == nil || r.blob.h.ptr == nil {
		return 0, newDatabaseError(ErrorCode(C.SQLITE_MISUSE), "invalid blob handle")
	}

	blobSize := r.blob.Bytes()
	if r.offset >= blobSize {
		return 0, io.EOF
	}

	// Determine how much to read
	toRead := len(p)
	if r.offset+toRead > blobSize {
		toRead = blobSize - r.offset
	}

	data, err := r.blob.Read(r.offset, toRead)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}

	copy(p, data)
	r.offset += toRead

	if r.offset >= blobSize {
		return toRead, io.EOF
	}

	return toRead, nil
}

// Close closes the underlying blob.
func (r *BlobReader) Close() error {
	if r.blob != nil {
		r.blob.Close()
		r.blob = nil
	}
	return nil
}

// ReadAt implements io.ReaderAt.
// It reads len(p) bytes from the given absolute offset in the blob.
func (r *BlobReader) ReadAt(p []byte, off int64) (int, error) {
	if r.blob == nil || r.blob.h == nil || r.blob.h.ptr == nil {
		return 0, newDatabaseError(ErrorCode(C.SQLITE_MISUSE), "invalid blob handle")
	}

	blobSize := r.blob.Bytes()
	if int(off) >= blobSize {
		return 0, io.EOF
	}

	toRead := len(p)
	if int(off)+toRead > blobSize {
		toRead = blobSize - int(off)
	}

	data, err := r.blob.Read(int(off), toRead)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}

	copy(p, data)
	if int(off)+toRead >= blobSize {
		return toRead, io.EOF
	}
	return toRead, nil
}

// Seek implements io.Seeker.
// It sets the offset for the next Read or Write to offset, interpreted according to whence.
func (r *BlobReader) Seek(offset int64, whence int) (int64, error) {
	if r.blob == nil || r.blob.h == nil || r.blob.h.ptr == nil {
		return 0, newDatabaseError(ErrorCode(C.SQLITE_MISUSE), "invalid blob handle")
	}

	blobSize := r.blob.Bytes()
	var newOffset int

	switch whence {
	case io.SeekStart:
		newOffset = int(offset)
	case io.SeekCurrent:
		newOffset = r.offset + int(offset)
	case io.SeekEnd:
		newOffset = blobSize + int(offset)
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if newOffset < 0 || newOffset > blobSize {
		return 0, fmt.Errorf("seek out of range: %d", newOffset)
	}

	r.offset = newOffset
	return int64(r.offset), nil
}

type Transaction struct {
	db       *Connection
	finished bool
}

func (d *Connection) BeginTransaction() (*Transaction, error) {
	err := d.Exec("BEGIN TRANSACTION;")
	if err != nil {
		return nil, err
	}

	result := &Transaction{db: d, finished: false}

	runtime.AddCleanup(result, func(db *Connection) {
		if !result.finished {
			db.Exec("ROLLBACK;")
		}
	}, result.db)

	return result, nil
}

func (t *Transaction) Commit() error {
	t.finished = true
	return t.db.Exec("COMMIT;")
}

func (t *Transaction) Rollback() error {
	t.finished = true
	return t.db.Exec("ROLLBACK;")
}
