package logstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestMetaVersionRules 版本化元数据的接受/升级/拒绝规则。
func TestMetaVersionRules(t *testing.T) {
	writeMeta := func(dir, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("older version upgraded", func(t *testing.T) {
		dir := t.TempDir()
		writeMeta(dir, `{"version":0}`)
		l, err := Open(dir, Options{})
		if err != nil {
			t.Fatal(err)
		}
		info := l.RecoveryInfo()
		t.Logf("meta action=%s version=%d (规则: 旧版本接受并升级)", info.MetaAction, info.FormatVersion)
		if info.MetaAction != "upgraded" || info.FormatVersion != formatVersionCurrent {
			t.Fatalf("unexpected: %+v", info)
		}
		l.Close()
	})

	t.Run("newer version rejected", func(t *testing.T) {
		dir := t.TempDir()
		writeMeta(dir, `{"version":99}`)
		_, err := Open(dir, Options{})
		if !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("got %v, want ErrUnsupportedVersion", err)
		}
		t.Logf("newer version rejected: %v", err)
	})

	t.Run("corrupt meta rejected", func(t *testing.T) {
		dir := t.TempDir()
		writeMeta(dir, `{"version":`) // 不完整 JSON，模拟写一半崩溃
		_, err := Open(dir, Options{})
		if !errors.Is(err, ErrMetaCorrupt) {
			t.Fatalf("got %v, want ErrMetaCorrupt", err)
		}
		t.Logf("corrupt meta rejected: %v", err)
	})

	t.Run("empty meta rejected", func(t *testing.T) {
		dir := t.TempDir()
		writeMeta(dir, "")
		_, err := Open(dir, Options{})
		if !errors.Is(err, ErrMetaCorrupt) {
			t.Fatalf("got %v, want ErrMetaCorrupt", err)
		}
		t.Logf("empty meta rejected: %v", err)
	})

	t.Run("fresh dir created", func(t *testing.T) {
		dir := t.TempDir()
		l, err := Open(dir, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if info := l.RecoveryInfo(); info.MetaAction != "created" {
			t.Fatalf("action=%s want created", info.MetaAction)
		}
		l.Close()
	})
}
