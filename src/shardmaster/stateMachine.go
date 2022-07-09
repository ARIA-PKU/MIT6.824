package shardmaster

import (
	"sort"
)

type ConfigStateMachine  interface {
	Join(groups map[int][]string) Err
	Leave(gids []int) Err
	Move(shard, gid int) Err
	Query(num int) (Config, Err)
}

type MemoryConfigStateMachine struct {
	Configs []Config
}

func NewMemoryConfigStateMachine() *MemoryConfigStateMachine {
	cf := &MemoryConfigStateMachine{make([]Config, 1)}
	cf.Configs[0] = DefaultConfig()
	return cf
}

// Join(servers) -- add a set of groups (gid -> server-list mapping).
func (cf *MemoryConfigStateMachine) Join(groups map[int][]string) Err {
	lastConfig := cf.Configs[len(cf.Configs) - 1]
	newConfig := Config{
		Num:	len(cf.Configs),
		Shards:	lastConfig.Shards,
		Groups:	deepCopy(lastConfig.Groups),
	}
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}
	g2s := Group2Shards(newConfig)
	for {
		source, target := GetGidWithMaxShards(g2s), GetGidWithMinShards(g2s)
		if source != 0 && len(g2s[source]) - len(g2s[target]) <= 1 {
			break
		}
		g2s[target] = append(g2s[target], g2s[source][0])
		g2s[source] = g2s[source][1:]
	}
	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}


// Leave(gids) -- delete a set of groups.
func (cf *MemoryConfigStateMachine) Leave(gids []int) Err {
	lastConfig := cf.Configs[len(cf.Configs) - 1]
	newConfig := Config{
		Num:	len(cf.Configs),
		Shards:	lastConfig.Shards,
		Groups:	deepCopy(lastConfig.Groups),
	}
	g2s := Group2Shards(newConfig)
	orphanShards := make([]int, 0)
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}

		if shards, ok := g2s[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(g2s, gid)
		}
	}

	var newShards [NShards]int

	if len(newConfig.Groups) != 0 {
		for _, shard := range orphanShards {
			target := GetGidWithMinShards(g2s)
			g2s[target] = append(g2s[target], shard)
		}

		for gid, shards := range g2s {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	newConfig.Shards = newShards
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}


// Move(shard, gid) -- hand off one shard from current owner to gid.
func (cf *MemoryConfigStateMachine) Move(shard, gid int) Err {
	lastConfig := cf.Configs[len(cf.Configs) - 1]
	newConfig := Config{
		Num:	len(cf.Configs),
		Shards:	lastConfig.Shards,
		Groups:	deepCopy(lastConfig.Groups),
	}
	newConfig.Shards[shard] = gid
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

// Query(num) -> fetch Config # num, or latest config if num==-1.
func (cf *MemoryConfigStateMachine) Query(num int) (Config, Err) {
	if num < 0 || num >= len(cf.Configs) {
		return cf.Configs[len(cf.Configs) - 1], OK
	}
	return cf.Configs[num], OK
}

// find mapping from groups to shards
func Group2Shards(config Config) map[int][]int {
	g2s := make(map[int][]int)
	for gid := range config.Groups {
		g2s[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		g2s[gid] = append(g2s[gid], shard)
	}
	return g2s
}


func GetGidWithMinShards(g2s map[int][]int) int {
	var keys []int
	for k := range g2s {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	index, min := -1, NShards + 1
	for _, gid := range keys {
		if gid != 0 && len(g2s[gid]) < min {
			index, min = gid, len(g2s[gid])
		}
	}
	return index
}

func GetGidWithMaxShards(g2s map[int][]int) int {
	// left shards by operation Leave will be in position 0
	if shards, ok := g2s[0]; ok && len(shards) > 0 {
		return 0
	}

	var keys []int
	for k := range g2s {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	index, max := -1, -1
	for _, gid := range keys {
		if len(g2s[gid]) > max {
			index, max = gid, len(g2s[gid])
		}
	}
	return index
}
