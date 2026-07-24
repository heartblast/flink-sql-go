package changelog

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/heartblast/flink-sql-go/flinksqlgateway"
)

var materializerColumns = []flinksqlgateway.ColumnInfo{
	{Name: "id", LogicalType: flinksqlgateway.LogicalType{Type: "BIGINT"}},
	{Name: "region", LogicalType: flinksqlgateway.LogicalType{Type: "STRING"}},
	{Name: "value", LogicalType: flinksqlgateway.LogicalType{Type: "STRING"}},
}

func TestMaterializerInsertUpdateDeleteCompositeKey(t *testing.T) {
	materializer, err := NewMaterializer(PrimaryKey("id", "region"), Columns(materializerColumns), MaxRows(10))
	if err != nil {
		t.Fatalf("NewMaterializer() error = %v", err)
	}
	if err := materializer.Apply(changeRow(flinksqlgateway.RowInsert, `1`, `"us"`, `"old"`)); err != nil {
		t.Fatalf("Apply(insert us) error = %v", err)
	}
	if err := materializer.Apply(changeRow(flinksqlgateway.RowInsert, `1`, `"eu"`, `"other"`)); err != nil {
		t.Fatalf("Apply(insert eu) error = %v", err)
	}
	if err := materializer.Apply(changeRow(flinksqlgateway.RowUpdateBefore, `1`, `"us"`, `"old"`)); err != nil {
		t.Fatalf("Apply(update before) error = %v", err)
	}
	if err := materializer.Apply(changeRow(flinksqlgateway.RowUpdateAfter, `1`, `"us"`, `"new"`)); err != nil {
		t.Fatalf("Apply(update after) error = %v", err)
	}
	if err := materializer.Apply(changeRow(flinksqlgateway.RowDelete, `1`, `"eu"`, `"other"`)); err != nil {
		t.Fatalf("Apply(delete) error = %v", err)
	}

	snapshot := materializer.Snapshot()
	if len(snapshot) != 1 || snapshot[0].Kind != flinksqlgateway.RowInsert || string(snapshot[0].Fields[2]) != `"new"` {
		t.Fatalf("Snapshot() = %+v", snapshot)
	}
	snapshot[0].Fields[2][1] = 'X'
	second := materializer.Snapshot()
	if string(second[0].Fields[2]) != `"new"` {
		t.Fatalf("snapshot mutated internal state: %s", second[0].Fields[2])
	}

	if err := materializer.Apply(changeRow(flinksqlgateway.RowDelete, `1`, `"us"`, `"new"`)); err != nil {
		t.Fatalf("Apply(delete us) error = %v", err)
	}
	if err := materializer.Apply(changeRow(flinksqlgateway.RowInsert, `1`, `"us"`, `"reinserted"`)); err != nil {
		t.Fatalf("Apply(reinsert) error = %v", err)
	}
	if got := materializer.Snapshot(); len(got) != 1 || string(got[0].Fields[2]) != `"reinserted"` {
		t.Fatalf("Snapshot() after reinsert = %+v", got)
	}
}

func TestMaterializerValidationAndUpdateOrder(t *testing.T) {
	if _, err := NewMaterializer(); !errors.Is(err, ErrPrimaryKeyRequired) {
		t.Fatalf("NewMaterializer() error = %v", err)
	}
	if _, err := NewMaterializer(PrimaryKey("id")); !errors.Is(err, ErrColumnsRequired) {
		t.Fatalf("missing Columns error = %v", err)
	}
	if _, err := NewMaterializer(PrimaryKey("missing"), Columns(materializerColumns)); !errors.Is(err, ErrPrimaryKeyMissing) {
		t.Fatalf("missing primary-key column error = %v", err)
	}
	if _, err := NewMaterializer(PrimaryKey("id", "id")); !errors.Is(err, ErrPrimaryKeyRequired) {
		t.Fatalf("duplicate PrimaryKey error = %v", err)
	}

	materializer, err := NewMaterializer(PrimaryKey("id"), Columns(materializerColumns), MaxRows(1))
	if err != nil {
		t.Fatalf("NewMaterializer() error = %v", err)
	}
	if err := materializer.Apply(changeRow(flinksqlgateway.RowInsert, `1`, `"us"`, `"one"`)); err != nil {
		t.Fatalf("Apply(insert) error = %v", err)
	}
	if err := materializer.Apply(changeRow(flinksqlgateway.RowInsert, `2`, `"us"`, `"two"`)); !errors.Is(err, ErrMaxRows) {
		t.Fatalf("max rows error = %v", err)
	}
	if err := materializer.Apply(changeRow(flinksqlgateway.RowUpdateAfter, `1`, `"us"`, `"new"`)); !errors.Is(err, ErrUpdateOrder) {
		t.Fatalf("UPDATE_AFTER order error = %v", err)
	}
	if err := materializer.Apply(changeRow(flinksqlgateway.RowUpdateBefore, `1`, `"us"`, `"mismatch"`)); !errors.Is(err, ErrUpdateOrder) {
		t.Fatalf("UPDATE_BEFORE mismatch error = %v", err)
	}
	if err := materializer.Apply(changeRow(flinksqlgateway.RowKind("FUTURE"), `1`, `"us"`, `"one"`)); !errors.Is(err, ErrUnsupportedRowKind) {
		t.Fatalf("unknown row kind error = %v", err)
	}

	nullKey := changeRow(flinksqlgateway.RowInsert, `null`, `"us"`, `"none"`)
	if err := materializer.Apply(nullKey); !errors.Is(err, ErrPrimaryKeyMissing) {
		t.Fatalf("NULL key error = %v", err)
	}
	unbound := flinksqlgateway.Row{Kind: flinksqlgateway.RowInsert, Fields: []json.RawMessage{}}
	if err := materializer.Apply(unbound); !errors.Is(err, ErrPrimaryKeyMissing) {
		t.Fatalf("unbound key error = %v", err)
	}
}

func TestMaterializerRollbackAndAtomicUpdate(t *testing.T) {
	materializer, err := NewMaterializer(PrimaryKey("id"), Columns(materializerColumns), MaxRows(10))
	if err != nil {
		t.Fatalf("NewMaterializer() error = %v", err)
	}
	inserted := changeRow(flinksqlgateway.RowInsert, `1`, `"us"`, `"old"`)
	before := changeRow(flinksqlgateway.RowUpdateBefore, `1`, `"us"`, `"old"`)
	after := changeRow(flinksqlgateway.RowUpdateAfter, `1`, `"us"`, `"new"`)
	if err := materializer.Apply(inserted); err != nil {
		t.Fatalf("Apply(insert) error = %v", err)
	}
	if err := materializer.Apply(before); err != nil {
		t.Fatalf("Apply(update before) error = %v", err)
	}
	if materializer.PendingUpdates() != 1 || materializer.RollbackPending() != 1 || materializer.PendingUpdates() != 0 {
		t.Fatalf("pending updates were not rolled back")
	}
	if err := materializer.ApplyUpdate(before, after); err != nil {
		t.Fatalf("ApplyUpdate() error = %v", err)
	}
	if got := materializer.Snapshot(); len(got) != 1 || string(got[0].Fields[2]) != `"new"` {
		t.Fatalf("Snapshot() = %+v", got)
	}
}

func TestMaterializerCanonicalKeyAndBoundedOrderIndex(t *testing.T) {
	materializer, err := NewMaterializer(PrimaryKey("region"), Columns(materializerColumns), MaxRows(1))
	if err != nil {
		t.Fatalf("NewMaterializer() error = %v", err)
	}
	if err := materializer.Apply(changeRow(flinksqlgateway.RowInsert, `1`, `"\u0075s"`, `"value"`)); err != nil {
		t.Fatalf("Apply(insert) error = %v", err)
	}
	if err := materializer.Apply(changeRow(flinksqlgateway.RowDelete, `1`, `"us"`, `"value"`)); err != nil {
		t.Fatalf("Apply(delete canonical key) error = %v", err)
	}
	for index := 0; index < 3000; index++ {
		key := json.RawMessage(`"key"`)
		row := flinksqlgateway.Row{Kind: flinksqlgateway.RowInsert, Fields: []json.RawMessage{json.RawMessage(`1`), key, json.RawMessage(`"value"`)}}
		if err := materializer.Apply(row); err != nil {
			t.Fatalf("Apply(insert %d) error = %v", index, err)
		}
		row.Kind = flinksqlgateway.RowDelete
		if err := materializer.Apply(row); err != nil {
			t.Fatalf("Apply(delete %d) error = %v", index, err)
		}
	}
	if len(materializer.order) > 1025 {
		t.Fatalf("order index retained %d entries", len(materializer.order))
	}
}

func changeRow(kind flinksqlgateway.RowKind, id, region, value string) flinksqlgateway.Row {
	return flinksqlgateway.Row{
		Kind: kind,
		Fields: []json.RawMessage{
			json.RawMessage(id),
			json.RawMessage(region),
			json.RawMessage(value),
		},
	}
}
