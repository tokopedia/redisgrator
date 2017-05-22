package handler

import (
	"errors"
	"fmt"
	"time"

	redis "github.com/tokopedia/go-redis-server"
	"github.com/tokopedia/redisgrator/config"
	"github.com/tokopedia/redisgrator/connection"
)

type RedisHandler struct {
	redis.DefaultHandler
	Start time.Time
}

// GET
func (h *RedisHandler) Get(key string) ([]byte, error) {
	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	v, err := origConn.Do("GET", key)
	//for safety handle v nil and v empty string
	if err != nil || v == nil || v == "" {
		v, err = destConn.Do("GET", key)
	} else {
		if config.Cfg.General.SetToDestWhenGet {
			//if keys exist in origin move it too destination
			_, err := destConn.Do("SET", key, v.([]byte))
			if err != nil {
				return nil, errors.New("err when set on get : " + err.Error())
			}
			_, err = origConn.Do("DEL", key)
			if err != nil {
				return nil, errors.New("err when del on get : " + err.Error())
			}
		}
	}

	strv, ok := v.([]byte)
	if ok == false {
		return nil, errors.New("keys not found")
	}
	return strv, nil
}

// SET
func (h *RedisHandler) Set(key string, value []byte) ([]byte, error) {
	destConn := connection.RedisPoolConnection.Destination.Get()

	v, err := destConn.Do("SET", key, value)
	if err != nil {
		return nil, errors.New("err when set : " + err.Error())
	}
	strv, ok := v.(string)
	if ok == false {
		return nil, errors.New("value not string")
	}
	return []byte(strv), nil
}

// INFO
func (h *RedisHandler) Info() ([]byte, error) {
	return []byte(fmt.Sprintf(
		`#Server
		redisgrator 0.0.1
		uptime_in_seconds: %d
		#Stats
		number_of_reads_per_second: %d
		`, int(time.Since(h.Start).Seconds()), 0)), nil
}
