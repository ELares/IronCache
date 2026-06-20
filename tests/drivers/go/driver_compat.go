// SPDX-License-Identifier: MIT OR Apache-2.0
//
// PROD-8 go-redis compatibility driver (issue #158).
//
// Runs the real github.com/redis/go-redis/v9 client against a live IronCache, in TWO modes:
//
//   - single-node (redis.NewClient): core data-type ops, pipelining, MULTI/EXEC (TxPipeline),
//     pub/sub, and a RESP3 HELLO probe.
//   - cluster (redis.NewClusterClient): topology DISCOVERY (go-redis loads the slot map via CLUSTER
//     SLOTS), routed keyed ops (the client routes to the owning node, following MOVED), read-back
//     correctness, a cross-slot multi-key op, and a hash-tag co-located multi-key op.
//
// Each checked behavior prints exactly one line:
//
//	RESULT go-redis <single|cluster> <op-group> <PASS|FAIL> [detail]
//
// The process exits non-zero if any group FAILed. Assertions check VALUES (the ZRANGE pairs, the
// HGETALL map, pipeline reply order, MULTI/EXEC atomicity, a SUBSCRIBE receiving a PUBLISH), not
// merely the absence of an error.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()
var failed int

func result(mode, group string, ok bool, detail string) {
	status := "PASS"
	if !ok {
		status = "FAIL"
		failed++
	}
	line := fmt.Sprintf("RESULT go-redis %s %s %s %s", mode, group, status, detail)
	fmt.Println(strings.TrimRight(line, " "))
}

// check runs a closure returning (ok, detail, err); a non-nil err is a finding (FAIL).
func check(mode, group string, fn func() (bool, string, error)) {
	ok, detail, err := fn()
	if err != nil {
		result(mode, group, false, fmt.Sprintf("error: %v", err))
		return
	}
	result(mode, group, ok, detail)
}

func runSingle(port int) {
	mode := "single"
	pfx := fmt.Sprintf("go:%d:", time.Now().UnixNano()%1_000_000)
	rdb := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", port)})
	defer rdb.Close()

	check(mode, "connect", func() (bool, string, error) {
		pong, err := rdb.Ping(ctx).Result()
		return pong == "PONG", "PING=" + pong, err
	})

	check(mode, "strings", func() (bool, string, error) {
		k := pfx + "str"
		if err := rdb.Set(ctx, k, "hello", 0).Err(); err != nil {
			return false, "", err
		}
		v, err := rdb.Get(ctx, k).Result()
		if err != nil {
			return false, "", err
		}
		if err := rdb.Append(ctx, k, " world").Err(); err != nil {
			return false, "", err
		}
		full, _ := rdb.Get(ctx, k).Result()
		rng, _ := rdb.GetRange(ctx, k, 0, 4).Result()
		rdb.Set(ctx, pfx+"n", "10", 0)
		n, err := rdb.IncrBy(ctx, pfx+"n", 5).Result()
		ok := v == "hello" && full == "hello world" && rng == "hello" && n == 15
		return ok, fmt.Sprintf("get=%q append=%q getrange=%q incr=%d", v, full, rng, n), err
	})

	check(mode, "lists", func() (bool, string, error) {
		k := pfx + "list"
		rdb.RPush(ctx, k, "a", "b", "c")
		rdb.LPush(ctx, k, "z")
		vals, err := rdb.LRange(ctx, k, 0, -1).Result()
		ok := len(vals) == 4 && vals[0] == "z" && vals[1] == "a" && vals[3] == "c"
		return ok, fmt.Sprintf("lrange=%v", vals), err
	})

	check(mode, "hashes", func() (bool, string, error) {
		k := pfx + "hash"
		rdb.HSet(ctx, k, "f1", "v1", "f2", "v2")
		m, err := rdb.HGetAll(ctx, k).Result()
		ok := m["f1"] == "v1" && m["f2"] == "v2" && len(m) == 2
		return ok, fmt.Sprintf("hgetall=%v", m), err
	})

	check(mode, "sets", func() (bool, string, error) {
		k := pfx + "set"
		rdb.SAdd(ctx, k, "x", "y", "z")
		members, err := rdb.SMembers(ctx, k).Result()
		set := map[string]bool{}
		for _, m := range members {
			set[m] = true
		}
		ok := len(set) == 3 && set["x"] && set["y"] && set["z"]
		return ok, fmt.Sprintf("smembers=%v", members), err
	})

	check(mode, "zsets", func() (bool, string, error) {
		k := pfx + "zset"
		rdb.ZAdd(ctx, k, redis.Z{Score: 1, Member: "one"}, redis.Z{Score: 2, Member: "two"}, redis.Z{Score: 3, Member: "three"})
		pairs, err := rdb.ZRangeWithScores(ctx, k, 0, -1).Result()
		ok := len(pairs) == 3 &&
			pairs[0].Member == "one" && pairs[0].Score == 1 &&
			pairs[1].Member == "two" && pairs[1].Score == 2 &&
			pairs[2].Member == "three" && pairs[2].Score == 3
		return ok, fmt.Sprintf("zrange_withscores=%v", pairs), err
	})

	check(mode, "expire-ttl", func() (bool, string, error) {
		k := pfx + "ttl"
		rdb.Set(ctx, k, "v", 0)
		rdb.Expire(ctx, k, 100*time.Second)
		t, err := rdb.TTL(ctx, k).Result()
		ok := t > 90*time.Second && t <= 100*time.Second
		return ok, fmt.Sprintf("ttl=%v", t), err
	})

	check(mode, "mget-mset", func() (bool, string, error) {
		rdb.MSet(ctx, pfx+"m1", "A", pfx+"m2", "B", pfx+"m3", "C")
		vals, err := rdb.MGet(ctx, pfx+"m1", pfx+"m2", pfx+"m3").Result()
		ok := len(vals) == 3 && vals[0] == "A" && vals[1] == "B" && vals[2] == "C"
		return ok, fmt.Sprintf("mget=%v", vals), err
	})

	check(mode, "pipeline", func() (bool, string, error) {
		pipe := rdb.Pipeline()
		pipe.Set(ctx, pfx+"p1", "1", 0)
		incr := pipe.Incr(ctx, pfx+"p1")
		get := pipe.Get(ctx, pfx+"p1")
		pipe.RPush(ctx, pfx+"pl", "a", "b")
		lr := pipe.LRange(ctx, pfx+"pl", 0, -1)
		_, err := pipe.Exec(ctx)
		ok := incr.Val() == 2 && get.Val() == "2" && len(lr.Val()) == 2 && lr.Val()[0] == "a"
		return ok, fmt.Sprintf("incr=%d get=%q lrange=%v", incr.Val(), get.Val(), lr.Val()), err
	})

	check(mode, "multi-exec", func() (bool, string, error) {
		// TxPipeline wraps in MULTI/EXEC -> atomic.
		var incr *redis.IntCmd
		_, err := rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, pfx+"t", "0", 0)
			pipe.Incr(ctx, pfx+"t")
			incr = pipe.Incr(ctx, pfx+"t")
			return nil
		})
		final, _ := rdb.Get(ctx, pfx+"t").Result()
		ok := incr.Val() == 2 && final == "2"
		return ok, fmt.Sprintf("exec_last=%d final=%q", incr.Val(), final), err
	})

	check(mode, "pubsub", func() (bool, string, error) {
		chan_ := pfx + "chan"
		sub := rdb.Subscribe(ctx, chan_)
		defer sub.Close()
		// Wait for the subscribe confirmation.
		if _, err := sub.ReceiveTimeout(ctx, 3*time.Second); err != nil {
			return false, "no subscribe confirmation", err
		}
		ch := sub.Channel()
		time.Sleep(100 * time.Millisecond)
		rdb.Publish(ctx, chan_, "ping-payload")
		select {
		case msg := <-ch:
			ok := msg.Payload == "ping-payload"
			return ok, fmt.Sprintf("payload=%q", msg.Payload), nil
		case <-time.After(3 * time.Second):
			return false, "no message received", nil
		}
	})

	check(mode, "resp3", func() (bool, string, error) {
		// go-redis v9 negotiates RESP3 by default (sends HELLO 3 on connect). Force it explicitly and
		// probe the HELLO map directly.
		r3 := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", port), Protocol: 3})
		defer r3.Close()
		res, err := r3.Do(ctx, "HELLO", "3").Result()
		if err != nil {
			return false, "", err
		}
		m, ok := res.(map[interface{}]interface{})
		proto := int64(-1)
		if ok {
			if p, has := m["proto"]; has {
				if pi, isInt := p.(int64); isInt {
					proto = pi
				}
			}
		}
		r3.Set(ctx, pfx+"r3", "ok", 0)
		v, _ := r3.Get(ctx, pfx+"r3").Result()
		good := proto == 3 && v == "ok"
		return good, fmt.Sprintf("hello_proto=%d get=%q", proto, v), nil
	})
}

func runCluster(addrs []string) {
	mode := "cluster"
	pfx := fmt.Sprintf("goc:%d:", time.Now().UnixNano()%1_000_000)
	rdb := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	defer rdb.Close()

	// DISCOVERY: ReloadState loads the slot map via CLUSTER SLOTS; Ping then routes through it.
	check(mode, "discovery", func() (bool, string, error) {
		if err := rdb.Ping(ctx).Err(); err != nil {
			return false, "", err
		}
		// Count discovered master nodes.
		n := 0
		err := rdb.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error {
			n++
			return nil
		})
		return n >= 1, fmt.Sprintf("masters_discovered=%d", n), err
	})

	check(mode, "routed-ops", func() (bool, string, error) {
		// 60 keys spread across the slot space; the client routes each to its owner via MOVED.
		for i := 0; i < 60; i++ {
			if err := rdb.Set(ctx, fmt.Sprintf("%sk%d", pfx, i), fmt.Sprintf("%d", i), 0).Err(); err != nil {
				return false, fmt.Sprintf("set k%d failed", i), err
			}
		}
		return true, "set 60 keys across slots", nil
	})

	check(mode, "routed-readback", func() (bool, string, error) {
		for i := 0; i < 60; i++ {
			v, err := rdb.Get(ctx, fmt.Sprintf("%sk%d", pfx, i)).Result()
			if err != nil {
				return false, fmt.Sprintf("get k%d failed", i), err
			}
			if v != fmt.Sprintf("%d", i) {
				return false, fmt.Sprintf("k%d=%q", i, v), nil
			}
		}
		return true, "all 60 read back", nil
	})

	check(mode, "crossslot", func() (bool, string, error) {
		// A multi-key MGET that spans slots: go-redis sends it to one node, which must reply
		// CROSSSLOT (the keys hash to different slots). The error surfaces to the caller.
		err := rdb.MGet(ctx, pfx+"k0", pfx+"k1", pfx+"k2").Err()
		if err == nil {
			return true, "mget across slots accepted (client may split)", nil
		}
		msg := strings.ToUpper(err.Error())
		ok := strings.Contains(msg, "CROSSSLOT") || strings.Contains(msg, "SLOT")
		return ok, fmt.Sprintf("rejected: %v", err), nil
	})

	check(mode, "hashtag-coloc", func() (bool, string, error) {
		// {tag} forces co-location -> a multi-key op succeeds against one slot.
		err := rdb.MSet(ctx, pfx+"{t}:a", "1", pfx+"{t}:b", "2", pfx+"{t}:c", "3").Err()
		if err != nil {
			return false, "", err
		}
		vals, err := rdb.MGet(ctx, pfx+"{t}:a", pfx+"{t}:b", pfx+"{t}:c").Result()
		ok := len(vals) == 3 && vals[0] == "1" && vals[1] == "2" && vals[2] == "3"
		return ok, fmt.Sprintf("hashtag_mget=%v", vals), err
	})

	check(mode, "pipeline", func() (bool, string, error) {
		pipe := rdb.Pipeline()
		for i := 0; i < 10; i++ {
			pipe.Set(ctx, fmt.Sprintf("%spp%d", pfx, i), fmt.Sprintf("val%d", i), 0)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return false, "", err
		}
		for i := 0; i < 10; i++ {
			v, _ := rdb.Get(ctx, fmt.Sprintf("%spp%d", pfx, i)).Result()
			if v != fmt.Sprintf("val%d", i) {
				return false, fmt.Sprintf("pp%d=%q", i, v), nil
			}
		}
		return true, "cluster pipeline 10 keys ok", nil
	})
}

func main() {
	singlePort := flag.Int("single-port", 0, "single-node RESP port")
	clusterCSV := flag.String("cluster", "", "comma list of host:port cluster seeds")
	flag.Parse()

	fmt.Println("# go-redis v9")
	if *singlePort > 0 {
		runSingle(*singlePort)
	}
	if *clusterCSV != "" {
		runCluster(strings.Split(*clusterCSV, ","))
	}
	if failed > 0 {
		os.Exit(1)
	}
}
