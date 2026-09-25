package eventlog

import (
	"context"
	"testing"
)

func TestSmokeAppendReplay(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	var positions []Position
	for i := 0; i < 5; i++ {
		p, err := s.Append(ctx, []byte{byte(i)})
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		positions = append(positions, p)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := Open(dir, DefaultOptions())
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	var got []byte
	err = s2.Replay(ctx, Beginning, func(r Record) error {
		got = append(got, r.Data...)
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("want 5 records, got %d: %v", len(got), got)
	}
	for i := range got {
		if got[i] != byte(i) {
			t.Fatalf("record %d = %d", i, got[i])
		}
	}
	r, err := s2.Read(ctx, positions[2])
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if r.Sequence != 2 || string(r.Data) != "\x02" {
		t.Fatalf("unexpected record: %+v", r)
	}
	t.Logf("replay OK, report=%+v", s2.LastRecovery())
}
