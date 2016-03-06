package nosql

import (
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
	. "github.com/polaris1119/config"
	"github.com/polaris1119/goutils"
)

// TODO:redis 操作出错，报警？

var redisConfig map[string]string

func init() {
	var err error
	redisConfig, err = ConfigFile.GetSection("redis")
	if err != nil {
		log.Println("config parse redis section error:", err)
		return
	}

	KeyPrefix = redisConfig["prefix"]
}

var KeyPrefix = ""

type RedisClient struct {
	redis.Conn
	err error
}

func NewRedisClient() *RedisClient {
	connTimeout := time.Duration(goutils.MustInt(redisConfig["conn_timeout"], 0)) * time.Second
	readTimeout := time.Duration(goutils.MustInt(redisConfig["read_timeout"], 0)) * time.Second
	writeTimeout := time.Duration(goutils.MustInt(redisConfig["write_timeout"], 0)) * time.Second

	conn, err := redis.DialTimeout("tcp", redisConfig["host"]+":"+redisConfig["port"], connTimeout, readTimeout, writeTimeout)
	if err != nil {
		return &RedisClient{Conn: conn, err: err}
	}

	if _, err = conn.Do("AUTH", redisConfig["password"]); err != nil {
		return &RedisClient{Conn: conn, err: err}
	}

	return &RedisClient{Conn: conn, err: nil}
}

func (this *RedisClient) SET(key string, val interface{}, expireSeconds int) error {
	if this.err != nil {
		return this.err
	}

	key = KeyPrefix + key

	args := redis.Args{}.Add(key, val)
	if expireSeconds != 0 {
		args.Add("EX").Add(expireSeconds)
	}
	_, err := redis.String(this.Conn.Do("SET", args...))
	return err
}

func (this *RedisClient) GET(key string) string {
	if this.err != nil {
		return ""
	}

	key = KeyPrefix + key

	val, err := redis.String(this.Conn.Do("GET", key))
	if err != nil {
		return ""
	}

	return val
}

func (this *RedisClient) DEL(key string) error {
	if this.err != nil {
		return this.err
	}

	key = KeyPrefix + key

	_, err := redis.Int(this.Conn.Do("DEL", key))

	return err
}

func (this *RedisClient) HSET(key, field, val string) error {
	if this.err != nil {
		return this.err
	}

	key = KeyPrefix + key

	_, err := redis.Int(this.Conn.Do("HSET", key, field, val))
	return err
}

func (this *RedisClient) HGETALL(key string) (map[string]string, error) {
	if this.err != nil {
		return nil, this.err
	}
	key = KeyPrefix + key

	return redis.StringMap(this.Conn.Do("HGETALL", key))
}

func (this *RedisClient) INCR(key string) (int64, error) {
	if this.err != nil {
		return 0, this.err
	}

	key = KeyPrefix + key

	return redis.Int64(this.Conn.Do("INCR", key))
}

func (this *RedisClient) HDEL(key, field string) error {
	if this.err != nil {
		return this.err
	}

	key = KeyPrefix + key

	_, err := redis.Int(this.Conn.Do("HDEL", key, field))

	return err
}

func (this *RedisClient) HSCAN(key string, cursor interface{}, optionArgs ...interface{}) (uint64, map[string]string, error) {
	if this.err != nil {
		return 0, nil, this.err
	}

	key = KeyPrefix + key

	args := redis.Args{}.Add(key, cursor).AddFlat(optionArgs)
	result, err := redis.Values(this.Conn.Do("HSCAN", args...))
	if err != nil {
		return 0, nil, err
	}

	newCursor, err := redis.Uint64(result[0], nil)
	if err != nil {
		return 0, nil, err
	}
	data, err := redis.StringMap(result[1], nil)

	return newCursor, data, err
}

func (this *RedisClient) Close() {
	this.Conn.Close()
}
