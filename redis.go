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

	pool = newPool(redisConfig)
}

var KeyPrefix = ""

type RedisClient struct {
	redis.Conn
	err error

	NoPrefix bool
}

// NewRedisClient 通过 [redis] 配置获取 redis 连接实例
func NewRedisClient() *RedisClient {
	return newRedisClient(redisConfig)
}

// NewRedisClientWithSection 通过传递进来的 section 配置获取 redis 连接实例
func NewRedisClientWithSection(section string) *RedisClient {
	sectionConfig, err := ConfigFile.GetSection(section)
	if err != nil {
		return &RedisClient{err: err}
	}
	return newRedisClient(sectionConfig)
}

var pool *redis.Pool

// NewRedisFromPool 使用连接池（只支持主 redis 实例）
func NewRedisFromPool() *RedisClient {
	return &RedisClient{Conn: pool.Get()}
}

func newRedisClient(configMap map[string]string) *RedisClient {
	conn, err := redisDialTimeout(configMap)
	if err != nil {
		return &RedisClient{err: err}
	}

	return &RedisClient{Conn: conn, err: nil}
}

func redisDialTimeout(configMap map[string]string) (redis.Conn, error) {
	connTimeout := time.Duration(goutils.MustInt(configMap["conn_timeout"], 0)) * time.Second
	readTimeout := time.Duration(goutils.MustInt(configMap["read_timeout"], 0)) * time.Second
	writeTimeout := time.Duration(goutils.MustInt(configMap["write_timeout"], 0)) * time.Second

	conn, err := redis.DialTimeout("tcp", configMap["host"]+":"+configMap["port"], connTimeout, readTimeout, writeTimeout)
	if err != nil {
		return conn, err
	}

	if _, err = conn.Do("AUTH", configMap["password"]); err != nil {
		conn.Close()
		return conn, err
	}

	return conn, nil
}

func newPool(configMap map[string]string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     goutils.MustInt(configMap["max_idle"]),
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redisDialTimeout(configMap)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func (this *RedisClient) SET(key string, val interface{}, expireSeconds int) error {
	if this.err != nil {
		return this.err
	}

	key = this.key(key)

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

	key = this.key(key)

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

	key = this.key(key)

	_, err := redis.Int(this.Conn.Do("DEL", key))

	return err
}

func (this *RedisClient) HSET(key, field, val string) error {
	if this.err != nil {
		return this.err
	}

	key = this.key(key)

	_, err := redis.Int(this.Conn.Do("HSET", key, field, val))
	return err
}

func (this *RedisClient) HGETALL(key string) (map[string]string, error) {
	if this.err != nil {
		return nil, this.err
	}
	key = this.key(key)

	return redis.StringMap(this.Conn.Do("HGETALL", key))
}

func (this *RedisClient) INCR(key string) (int64, error) {
	if this.err != nil {
		return 0, this.err
	}

	key = this.key(key)

	return redis.Int64(this.Conn.Do("INCR", key))
}

func (this *RedisClient) HDEL(key, field string) error {
	if this.err != nil {
		return this.err
	}

	key = this.key(key)

	_, err := redis.Int(this.Conn.Do("HDEL", key, field))

	return err
}

func (this *RedisClient) HSCAN(key string, cursor interface{}, optionArgs ...interface{}) (uint64, map[string]string, error) {
	if this.err != nil {
		return 0, nil, this.err
	}

	key = this.key(key)

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

func (this *RedisClient) key(key string) string {
	if this.NoPrefix {
		return key
	}

	return KeyPrefix + key
}
