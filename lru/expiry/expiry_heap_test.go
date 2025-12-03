package expiry

import (
	"maps"
	"runtime"
	"slices"
	"testing"
	"time"
)

func TestExpiryTracker(t *testing.T) {
	baseTime := time.Now()

	type instruction struct {
		preAdvance    time.Duration
		pushVal       int64
		pushExp       time.Time
		removeVal     int64
		remove        bool
		expExpired    []int64
		discardHandle []int64
	}

	for _, tbl := range []struct {
		name   string
		instrs []instruction
	}{
		{
			name: "simple_insert_3_no_advance",
			instrs: []instruction{
				{
					preAdvance:    0,
					pushVal:       1,
					pushExp:       baseTime.Add(time.Hour),
					expExpired:    nil,
					remove:        false,
					discardHandle: []int64{1},
				}, {
					preAdvance:    0,
					pushVal:       2,
					pushExp:       baseTime.Add(time.Hour),
					expExpired:    nil,
					remove:        false,
					discardHandle: []int64{2},
				}, {
					preAdvance: 0,
					pushVal:    3,
					pushExp:    baseTime.Add(time.Hour),
					expExpired: nil,
					remove:     false,
				},
			},
		}, {
			name: "insert_3_advance_expire_1",
			instrs: []instruction{
				{
					preAdvance:    0,
					pushVal:       1,
					pushExp:       baseTime.Add(time.Hour),
					expExpired:    nil,
					remove:        false,
					discardHandle: []int64{1},
				}, {
					preAdvance:    0,
					pushVal:       2,
					pushExp:       baseTime.Add(time.Hour),
					expExpired:    nil,
					remove:        false,
					discardHandle: []int64{2},
				}, {
					preAdvance: 0,
					pushVal:    3,
					pushExp:    baseTime.Add(time.Minute * 10),
					expExpired: nil,
					remove:     false,
				}, {
					preAdvance: time.Minute * 20,
					expExpired: []int64{3},
				},
			},
		}, {
			name: "mix_add_expire_2",
			instrs: []instruction{
				{
					preAdvance: 0,
					pushVal:    1,
					pushExp:    baseTime.Add(time.Hour),
					expExpired: nil,
					remove:     false,
				}, {
					preAdvance: time.Minute * 3,
					pushVal:    2,
					pushExp:    baseTime.Add(time.Minute * 5),
					expExpired: nil,
					remove:     false,
				}, {
					preAdvance: time.Minute * 3,
					pushVal:    3,
					pushExp:    baseTime.Add(time.Minute * 10),
					expExpired: []int64{2},
					remove:     false,
				}, {
					preAdvance: time.Minute * 20,
					expExpired: []int64{3},
				},
			},
		}, {
			name: "mix_add_5_remove_expire_2",
			instrs: []instruction{
				{
					preAdvance: 0,
					pushVal:    1,
					pushExp:    baseTime.Add(time.Hour),
					expExpired: nil,
					remove:     false,
				}, {
					preAdvance: time.Minute * 3,
					pushVal:    2,
					pushExp:    baseTime.Add(time.Minute * 5),
					expExpired: nil,
					remove:     false,
				}, {
					preAdvance: time.Minute * 3,
					pushVal:    3,
					pushExp:    baseTime.Add(time.Minute * 10),
					expExpired: []int64{2},
					remove:     false,
				}, {
					preAdvance: time.Minute * 20,
					pushVal:    4,
					pushExp:    baseTime.Add(time.Hour * 10),
					expExpired: []int64{3},
					removeVal:  1,
					remove:     true,
				}, {
					preAdvance: time.Minute * 3,
					pushVal:    5,
					pushExp:    baseTime.Add(time.Minute * 45),
					expExpired: nil,
					removeVal:  4,
					remove:     true,
				},
			},
		}, {
			name: "mix_add_5_remove_expire_2_discard_handle",
			instrs: []instruction{
				{
					preAdvance: 0,
					pushVal:    1,
					pushExp:    baseTime.Add(time.Hour),
					expExpired: nil,
					remove:     false,
				}, {
					preAdvance:    time.Minute * 3,
					pushVal:       2,
					pushExp:       baseTime.Add(time.Minute * 5),
					expExpired:    nil,
					remove:        false,
					discardHandle: []int64{1},
				}, {
					preAdvance: time.Minute * 3,
					pushVal:    3,
					pushExp:    baseTime.Add(time.Minute * 10),
					expExpired: []int64{2},
					remove:     false,
				}, {
					preAdvance: time.Minute * 20,
					pushVal:    4,
					pushExp:    baseTime.Add(time.Hour * 10),
					expExpired: []int64{3},
					removeVal:  1,
					remove:     true,
				}, {
					preAdvance: time.Minute * 3,
					pushVal:    5,
					pushExp:    baseTime.Add(time.Minute * 45),
					expExpired: nil,
					removeVal:  4,
					remove:     true,
				}, {
					preAdvance: time.Hour * 20,
					expExpired: []int64{1, 5},
					removeVal:  5,
					remove:     true,
				},
			},
		},
	} {
		t.Run(tbl.name, func(t *testing.T) {
			now := baseTime
			handles := map[int64]*EntryHandle[int64]{}
			e := ExpiryTracker[int64]{}
			for i, instr := range tbl.instrs {
				now = now.Add(instr.preAdvance)
				expExpired := make(map[int64]struct{}, len(instr.expExpired))
				for _, expVal := range instr.expExpired {
					expExpired[expVal] = struct{}{}
				}
				for expVal := range e.PopAllExpired(now) {
					if _, expected := expExpired[expVal]; !expected {
						t.Errorf("step %d: %s: value %d expired unexpectedly", i, now, expVal)
					}
					delete(expExpired, expVal)
				}
				if len(expExpired) > 0 {
					t.Errorf("step %d: %s: expected values to expire: %v",
						i, now, slices.Collect(maps.Keys(expExpired)))
				}
				if !instr.pushExp.IsZero() {
					handles[instr.pushVal] = e.Push(instr.pushExp, instr.pushVal)
				}
				e.Remove(&EntryHandle[int64]{})
				if instr.remove {
					e.Remove(handles[instr.removeVal])
					// don't remove from handles so we can exercise double-removing
				}
				for _, v := range instr.discardHandle {
					handles[v] = nil
					delete(handles, v)
				}
				// Trigger a GC so discarded handles get gc'd
				runtime.GC()
			}
		})
	}

}
