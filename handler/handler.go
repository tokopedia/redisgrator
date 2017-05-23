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
		if err != nil {
			return nil, errors.New("GET : " + err.Error())
		}
	} else {
		if config.Cfg.General.SetToDestWhenGet {
			//if keys exist in origin move it too destination
			_, err := destConn.Do("SET", key, v.([]byte))
			if err != nil {
				return nil, errors.New("GET : err when set on get : " + err.Error())
			}
			_, err = origConn.Do("DEL", key)
			if err != nil {
				return nil, errors.New("GET : err when del on get : " + err.Error())
			}
		}
	}

	strv, ok := v.([]byte)
	if ok == false {
		return nil, errors.New("GET : keys not found")
	}
	return strv, nil
}

// SET
func (h *RedisHandler) Set(key string, value []byte) ([]byte, error) {
	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	v, err := destConn.Do("SET", key, value)
	if err != nil {
		return nil, errors.New("SET : err when set : " + err.Error())
	}
	//could ignore all in origin because set on dest already success
	//del old key in origin
	origConn.Do("DEL", key)

	strv, ok := v.(string)
	if ok == false {
		return nil, errors.New("SET : value not string")
	}
	return []byte(strv), nil
}

// HEXISTS
func (h *RedisHandler) Hexists(key, field string) (int, error) {
	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	v, err := origConn.Do("HEXISTS", key, field)

	//check first is it really not error from origin
	int64v, ok := v.(int64)
	if ok == false {
		return 0, errors.New("value not int from origin")
	}

	//for safety handle v nil and int64v == 0 int
	if err != nil || v == nil || int64v == 0 {
		v, err = destConn.Do("HEXISTS", key, field)
		if err != nil {
			return 0, err
		}
	} else {
		//if this hash is in origin move it to destination
		err = moveHash(key)
		if err != nil {
			return 0, err
		}
	}

	//check first is it really not error from destination
	int64v, ok = v.(int64)
	if ok == false {
		return 0, errors.New("HEXISTS : value not int from destination")
	}
	intv := int(int64v)
	return intv, nil
}

// HGET
func (h *RedisHandler) Hget(key string, value []byte) ([]byte, error) {
	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	v, err := origConn.Do("HGET", key, value)
	//for safety handle v nil and v == ""
	if err != nil || v == nil || v == "" {
		v, err = destConn.Do("HGET", key, value)
		if err != nil {
			return nil, err
		}
	} else {
		if config.Cfg.General.SetToDestWhenGet {
			//if this hash is in origin move it to destination
			err = moveHash(key)
			if err != nil {
				return nil, err
			}
		}
	}
	if err != nil {
		return nil, errors.New("HGET : err when set : " + err.Error())
	}
	bytv, ok := v.([]byte)
	strv := string(bytv)
	if ok == false {
		return nil, errors.New("HGET : value not string")
	}
	return []byte(strv), nil
}

// HSET
func (h *RedisHandler) Hset(key, field string, value []byte) (int, error) {
	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()
	v, err := origConn.Do("EXISTS", key)
	if err != nil {
		return 0, errors.New("HSET : err when check exist in origin : " + err.Error())
	}
	if v.(int64) == 1 {
		//if hash exists move all hash first to destination
		err := moveHash(key)
		if err != nil {
			return 0, err
		}
	}

	v, err = destConn.Do("HSET", key, field, value)
	if err != nil {
		return 0, errors.New("HSET : err when set : " + err.Error())
	}
	int64v, ok := v.(int64)
	intv := int(int64v)
	if ok == false {
		return 0, errors.New("HSET : value not int")
	}
	return intv, nil
}

// SISMEMBER
func (h *RedisHandler) Sismember(set, field string) (int, error) {
	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	v, err := origConn.Do("SISMEMBER", set, field)
	if err != nil || v.(int64) == 0 || v == nil {
		v, err = destConn.Do("SISMEMBER", set, field)
		if err != nil {
			return 0, errors.New("SISMEMBER : err when sismember in destination : " + err.Error())
		}
	} else {
		//move all set
		err := moveSet(set)
		if err != nil {
			return 0, err
		}
	}

	int64v, ok := v.(int64)
	intv := int(int64v)
	if ok == false {
		return 0, errors.New("SISMEMBER : value not int")
	}
	return intv, nil
}

// SADD
func (h *RedisHandler) Sadd(set string, val []byte) (int, error) {
	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	v, err := origConn.Do("EXISTS", set)
	if err != nil {
		return 0, errors.New("SADD : err when check exist in origin : " + err.Error())
	}
	if v.(int64) == 1 {
		//if set exists move all set first to destination
		err := moveSet(set)
		if err != nil {
			return 0, err
		}
	}

	v, err = destConn.Do("SADD", set, val)
	if err != nil {
		return 0, errors.New("SADD : err when check exist in origin : " + err.Error())
	}
	int64v, ok := v.(int64)
	intv := int(int64v)
	if ok == false {
		return 0, errors.New("SISMEMBER : value not int")
	}
	return intv, nil
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

func moveHash(key string) error {
	if config.Cfg.General.MoveHash {
		origConn := connection.RedisPoolConnection.Origin.Get()
		destConn := connection.RedisPoolConnection.Destination.Get()

		v, err := origConn.Do("HGETALL", key)
		if err != nil {
			return err
		}
		//check first is v really array of interface
		arrval, ok := v.([]interface{})
		if ok == true {
			for i, val := range arrval {
				valstr := string(val.([]byte))
				if i%2 == 0 {
					_, err := destConn.Do("HSET", key, valstr, arrval[i+1].([]byte))
					if err != nil {
						return errors.New("err when set on hexist : " + err.Error())
					}
				}
			}
			_, err = origConn.Do("DEL", key)
			if err != nil {
				return errors.New("err when del on hexist : " + err.Error())
			}
		}
	}
	return nil
}

func moveSet(set string) error {
	if config.Cfg.General.MoveSet {
		origConn := connection.RedisPoolConnection.Origin.Get()
		destConn := connection.RedisPoolConnection.Destination.Get()

		v, err := origConn.Do("SMEMBERS", set)
		if err != nil {
			return err
		}
		//check first is v really array of interface
		arrval, ok := v.([]interface{})
		if ok == true {
			for _, val := range arrval {
				valstr := string(val.([]byte))
				//add all members of set to destination
				_, err := destConn.Do("SADD", set, valstr)
				if err != nil {
					return errors.New("err when set on hexist : keys exist as different type : " + err.Error())
				}
			}
			//delete from origin
			_, err = origConn.Do("DEL", set)
			if err != nil {
				return errors.New("err when del on hexist : " + err.Error())
			}
		}
	}
	return nil
}
