package main

import (
	"sync"
)

var (
	SETs  = map[string]string{}
	HSETs = map[string]map[string]string{}
)

var smutex, hmutex = sync.RWMutex{}, sync.RWMutex{}

var Handlers = map[string]func([]Value) Value{
	"PING":    ping,
	"SET":     set,
	"GET":     get,
	"HGET":    hget,
	"HGETALL": hgetall,
	"HSET":    hset,
}

func ping(args []Value) Value {
	if len(args) == 0 {
		return Value{typ: "string", str: "PONG"}
	}
	return Value{typ: "string", str: args[0].bulk}
}

func get(args []Value) Value {
	key := args[0].bulk

	smutex.RLock()
	val, ok := SETs[key]
	smutex.RUnlock()

	if ok {
		return Value{typ: "string", str: val}
	}

	return Value{typ: "null"}
}

func set(args []Value) Value {
	key := args[0].bulk
	val := args[1].bulk

	smutex.Lock()
	SETs[key] = val
	smutex.Unlock()

	return Value{typ: "string", str: "OK"}
}

func hset(args []Value) Value {
	if len(args) != 3 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hset' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	hmutex.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	hmutex.Unlock()

	return Value{typ: "string", str: "OK"}
}

func hgetall(args []Value) Value {
	if len(args) != 1 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := args[0].bulk

	hmutex.RLock()
	value, ok := HSETs[hash]
	hmutex.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	res := Value{typ: "array", array: make([]Value, 0, len(value)*2)}

	for k, val := range value {
		res.array = append(res.array, Value{typ: "bulk", bulk: k})
		res.array = append(res.array, Value{typ: "bulk", bulk: val})
	}

	return res
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: "error", str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := args[0].bulk
	key := args[1].bulk

	hmutex.RLock()
	value, ok := HSETs[hash][key]
	hmutex.RUnlock()

	if !ok {
		return Value{typ: "null"}
	}

	return Value{typ: "bulk", bulk: value}
}
