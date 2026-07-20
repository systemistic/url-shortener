package cache

import (
	"fmt"
	"sync"
	"testing"
)

func TestLRUPutGet(t *testing.T) {
	c := NewLRU[string, int](2)
	c.Put("a", 1)
	c.Put("b", 2)

	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a) = %d,%v want 1,true", v, ok)
	}
	if _, ok := c.Get("missing"); ok {
		t.Fatal("Get(missing) = true, want false")
	}
	if c.Len() != 2 {
		t.Fatalf("Len = %d, want 2", c.Len())
	}
}

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	tests := []struct {
		name    string
		ops     func(c *LRU[string, int])
		evicted string
		kept    []string
	}{
		{
			name:    "insertion order",
			ops:     func(c *LRU[string, int]) {},
			evicted: "a",
			kept:    []string{"b", "c"},
		},
		{
			name: "get promotes",
			ops: func(c *LRU[string, int]) {
				c.Get("a") // a becomes MRU, b is now LRU
			},
			evicted: "b",
			kept:    []string{"a", "c"},
		},
		{
			name: "put update promotes",
			ops: func(c *LRU[string, int]) {
				c.Put("a", 10)
			},
			evicted: "b",
			kept:    []string{"a", "c"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewLRU[string, int](2)
			c.Put("a", 1)
			c.Put("b", 2)
			tt.ops(c)
			c.Put("c", 3) // triggers eviction

			if _, ok := c.Get(tt.evicted); ok {
				t.Errorf("%q should have been evicted", tt.evicted)
			}
			for _, k := range tt.kept {
				if _, ok := c.Get(k); !ok {
					t.Errorf("%q should have been kept", k)
				}
			}
		})
	}
}

func TestLRUUpdateExistingDoesNotEvict(t *testing.T) {
	c := NewLRU[string, int](2)
	c.Put("a", 1)
	c.Put("b", 2)
	c.Put("a", 3) // update, not insert
	if c.Len() != 2 {
		t.Fatalf("Len = %d, want 2", c.Len())
	}
	if v, _ := c.Get("a"); v != 3 {
		t.Fatalf("Get(a) = %d, want 3", v)
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatal("b should not have been evicted by an update")
	}
}

func TestLRUDelete(t *testing.T) {
	c := NewLRU[string, int](4)
	c.Put("a", 1)
	if !c.Delete("a") {
		t.Fatal("Delete(a) = false, want true")
	}
	if c.Delete("a") {
		t.Fatal("second Delete(a) = true, want false")
	}
	if _, ok := c.Get("a"); ok {
		t.Fatal("a still present after Delete")
	}
	if c.Len() != 0 {
		t.Fatalf("Len = %d, want 0", c.Len())
	}
}

func TestLRUCapacityOne(t *testing.T) {
	c := NewLRU[int, string](1)
	c.Put(1, "one")
	c.Put(2, "two")
	if _, ok := c.Get(1); ok {
		t.Fatal("1 should have been evicted")
	}
	if v, ok := c.Get(2); !ok || v != "two" {
		t.Fatalf("Get(2) = %q,%v want two,true", v, ok)
	}
}

func TestLRUConcurrentAccess(t *testing.T) {
	c := NewLRU[string, int](64)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 500 {
				k := fmt.Sprintf("k%d", (g*500+i)%100)
				c.Put(k, i)
				c.Get(k)
				if i%10 == 0 {
					c.Delete(k)
				}
			}
		}()
	}
	wg.Wait()
	if c.Len() > 64 {
		t.Fatalf("Len = %d exceeds capacity 64", c.Len())
	}
}
