package logstore

import (
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentWritersAndReaders 并发追加与读取：
// 所有确认的写入必须不重不漏、内容可校验，读取结果不受并发污染。
func TestConcurrentWritersAndReaders(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{MaxSegmentBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	const writers = 8
	const perWriter = 50
	var mu sync.Mutex
	written := map[uint64]string{} // offset -> payload

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				p := fmt.Sprintf("w%02d-%03d", w, i)
				off, err := l.Append([]byte(p))
				if err != nil {
					t.Errorf("append: %v", err)
					return
				}
				mu.Lock()
				written[off] = p
				mu.Unlock()
			}
		}(w)
	}
	// 并发重放：读到的每条记录都必须与写入内容一致。
	stop := make(chan struct{})
	var rwg sync.WaitGroup
	for r := 0; r < 2; r++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				l.Replay(0, func(rec Record) error {
					mu.Lock()
					want, ok := written[rec.Offset]
					mu.Unlock()
					if !ok {
						t.Errorf("replay: unknown offset %d", rec.Offset)
						return fmt.Errorf("stop")
					}
					if string(rec.Payload) != want {
						t.Errorf("replay: offset %d got %q want %q",
							rec.Offset, rec.Payload, want)
					}
					return nil
				})
			}
		}()
	}
	wg.Wait()
	close(stop)
	rwg.Wait()

	if len(written) != writers*perWriter {
		t.Fatalf("confirmed writes=%d want %d", len(written), writers*perWriter)
	}
	// 最终一致性：重放全部记录，逐一比对。
	seen := 0
	err = l.Replay(0, func(rec Record) error {
		mu.Lock()
		want := written[rec.Offset]
		mu.Unlock()
		if string(rec.Payload) != want {
			t.Fatalf("offset %d: got %q want %q", rec.Offset, rec.Payload, want)
		}
		seen++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("concurrent test done: %d records, nextOffset=%d, replayed=%d",
		len(written), l.NextOffset(), seen)
	if seen != len(written) {
		t.Fatalf("replayed %d want %d", seen, len(written))
	}
}
