package redisconnection

import (
	"github.com/garyburd/redigo/redis"
	"time"
	"log"
	"os"
)

type redisPool interface {
	Get() redis.Conn
	Close() error
	ActiveCount() int
}

type RedisPoolHost struct {
	Origin    redisPool
	Destination  redisPool
}

var RedisPoolConnection *RedisPoolHost

//create new redis connection pool
func RedisConn(orig, dest string) *RedisPoolHost {
	var redisPoolH RedisPoolHost

	poolSize := 10

	redisPoolH.Origin = &redis.Pool{
		MaxIdle:     poolSize,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", orig) },
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	redisPoolH.Destination = &redis.Pool{
		MaxIdle:     poolSize,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", dest) },
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	_, errOrg := redisPoolH.Origin.Get().Do("PING")
	if errOrg != nil {
		log.Fatal("failed to connect redis origin : ", errOrg)
		os.Exit(0)
	}
	_, errDest:= redisPoolH.Destination.Get().Do("PING")
	if errDest != nil {
		log.Fatal("failed to connect redis destination : ", errDest)
		os.Exit(0)
	}

	return &redisPoolH
}
