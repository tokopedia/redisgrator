package handler

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/eapache/go-resiliency/semaphore"
	rds "github.com/garyburd/redigo/redis"
	redis "github.com/tokopedia/go-redis-server"
	"github.com/tokopedia/redisgrator/config"
	"github.com/tokopedia/redisgrator/connection"
)

type RedisHandler struct {
	redis.DefaultHandler
	Start time.Time
	Sema  *semaphore.Semaphore
}

// GET 2 side
func (h *RedisHandler) Get(key string) ([]byte, error) {
	err := h.Sema.Acquire()
	if err != nil {
		return nil, err
	}
	defer h.Sema.Release()

	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	chOrig := make(chan interface{})
	chDest := make(chan interface{})

	go getUsingChan(origConn, chOrig, key)
	go getUsingChan(destConn, chDest, key)

	// wait completion.
	valOrig := <-chOrig
	valDest := <-chDest

	// default exist value
	valExist := valDest

	if valDest == nil {
		if valOrig != nil {
			if config.Cfg.General.Duplicate {
				go func() {
					//if keys exist in origin move it too destination
					_, err := destConn.Do("SET", key, valOrig.([]byte))
					if err != nil {
						log.Println("SET : " + err.Error())
					}
					return
				}()
			}
			if config.Cfg.General.SetToDestWhenGet && !config.Cfg.General.Duplicate {
				go func() {
					_, err := origConn.Do("DEL", key)
					if err != nil {
						log.Println("DEL : " + err.Error())
					}
					return
				}()
			}
		}
		valExist = valOrig // set exist value
	}

	strv, ok := valExist.([]byte)
	if ok == false {
		if strv == nil {
			return nil, nil
		}
		return nil, errors.New("GET : keys not found")
	}
	return strv, nil
}

// get func channel
func getUsingChan(rcon rds.Conn, ch chan<- interface{}, key string) {
	defer close(ch)

	v, err := rcon.Do("GET", key)
	if err != nil {
		if err != rds.ErrNil {
			log.Println("GET : " + err.Error())
		}
		return
	}
	ch <- v
	return
}

// DEL 2 side
func (h *RedisHandler) Del(key string) (int, error) {
	err := h.Sema.Acquire()
	if err != nil {
		return 0, err
	}
	defer h.Sema.Release()

	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	chOrig := make(chan interface{})
	chDest := make(chan interface{})

	go delUsingChan(origConn, chOrig, key)
	go delUsingChan(destConn, chDest, key)

	// wait completion.
	valOrig := <-chOrig
	valDest := <-chDest

	// default exist value
	valExist := valDest

	if valDest == nil {
		if valOrig == nil {
			return 0, errors.New("DEL : keys not found") //both nil, key not found
		}
		valExist = valOrig // set exist value
	}

	//check first if it is not internal error
	int64v, ok := valExist.(int64)
	if ok == false {
		return 0, errors.New("DEL : value not int from destination")
	}
	intv := int(int64v)
	return intv, nil
}

// del func channel
func delUsingChan(rcon rds.Conn, ch chan<- interface{}, key string) {
	defer close(ch)

	v, err := rcon.Do("DEL", key)
	if err != nil {
		if err != rds.ErrNil {
			log.Println("DEL : " + err.Error())
		}
		return
	}
	ch <- v
	return
}

// SET
func (h *RedisHandler) Set(key string, value []byte) ([]byte, error) {
	err := h.Sema.Acquire()
	if err != nil {
		return nil, err
	}
	defer h.Sema.Release()

	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	v, err := destConn.Do("SET", key, value)
	if err != nil {
		return nil, errors.New("SET : err when set : " + err.Error())
	}

	if config.Cfg.General.Duplicate {
		go func() {
			v, err = origConn.Do("SET", key, value)
			if err != nil {
				log.Println("SET : err when set duplicate: " + err.Error())
			}
			return
		}()
	}
	//could ignore all in origin because set on dest already success
	//del old key in origin
	if !config.Cfg.General.Duplicate {
		go func() {
			origConn.Do("DEL", key)
			return
		}()
	}

	strv, ok := v.(string)
	if ok == false {
		return nil, errors.New("SET : value not string")
	}
	return []byte(strv), nil
}

// HEXISTS 2 side
func (h *RedisHandler) Hexists(key, field string) (int, error) {
	err := h.Sema.Acquire()
	if err != nil {
		return 0, err
	}
	defer h.Sema.Release()

	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	chOrig := make(chan interface{})
	chDest := make(chan interface{})

	go hexistsUsingChan(origConn, chOrig, key, field)
	go hexistsUsingChan(destConn, chDest, key, field)

	// wait completion
	valOrig := <-chOrig
	valDest := <-chDest

	// default exist value
	valExist := valDest

	if valDest == nil || valDest.(int64) == 0 {
		if valOrig != nil && valOrig.(int64) == 1 {
			//if this hash is in origin move it to destination
			go func() {
				err := moveHash(key)
				if err != nil {
					log.Println(err)
				}
				return
			}()
		}
		valExist = valOrig // set exist value
	}

	//check first is it really not error from destination
	int64v, ok := valExist.(int64)
	if ok == false {
		return 0, errors.New("HEXISTS : value not int from destination")
	}
	intv := int(int64v)
	return intv, nil
}

// hexists func channel
func hexistsUsingChan(rcon rds.Conn, ch chan<- interface{}, key, field string) {
	defer close(ch)

	v, err := rcon.Do("HEXISTS", key, field)
	if err != nil {
		if err != rds.ErrNil {
			log.Println("HEXISTS : " + err.Error())
		}
		return
	}
	ch <- v
	return
}

// HGET 2 side
func (h *RedisHandler) Hget(key string, value []byte) ([]byte, error) {
	err := h.Sema.Acquire()
	if err != nil {
		return nil, err
	}
	defer h.Sema.Release()

	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	chOrig := make(chan interface{})
	chDest := make(chan interface{})

	go hgetUsingChan(origConn, chOrig, key, value)
	go hgetUsingChan(destConn, chDest, key, value)

	// wait completion.
	valOrig := <-chOrig
	valDest := <-chDest

	// default exist value
	valExist := valDest

	if valDest == nil {
		if valOrig != nil {
			if config.Cfg.General.SetToDestWhenGet {
				go func() {
					//if this hash is in origin move it to destination
					err := moveHash(key)
					if err != nil {
						log.Println(err)
					}
					return
				}()
			}
		}
		valExist = valOrig // set exist value
	}

	bytv, ok := valExist.([]byte)
	strv := string(bytv)
	if ok == false {
		return nil, nil
	}
	return []byte(strv), nil
}

// hget func channel
func hgetUsingChan(rcon rds.Conn, ch chan<- interface{}, key string, value []byte) {
	defer close(ch)

	v, err := rcon.Do("HGET", key, value)
	if err != nil {
		if err != rds.ErrNil {
			log.Println("HGET : " + err.Error())
		}
		return
	}
	ch <- v
	return
}

// HGETALL 2 side
func (h *RedisHandler) Hgetall(key string) ([]interface{}, error) {
	err := h.Sema.Acquire()
	if err != nil {
		return nil, err
	}
	defer h.Sema.Release()

	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	chOrig := make(chan interface{})
	chDest := make(chan interface{})

	go hgetallUsingChan(origConn, chOrig, key)
	go hgetallUsingChan(destConn, chDest, key)

	// wait completion.
	valOrig := <-chOrig
	valDest := <-chDest

	// default exist value
	valExist := valDest
	var empty []interface{}

	valDestArr, ok := valDest.([]interface{})
	if valDest == nil || !ok || len(valDestArr) == 0 {
		valOrigArr, ok := valOrig.([]interface{})
		if valOrig == nil || !ok || len(valOrigArr) == 0 {
			return empty, errors.New("HGETALL : keys not found")
		} else {
			if config.Cfg.General.SetToDestWhenGet {
				go func() {
					//if this hash is in origin move it to destination
					err := moveHash(key)
					if err != nil {
						log.Println(err)
					}
					return
				}()
			}
		}
		valExist = valOrig // set exist value
	}

	result, ok := valExist.([]interface{})
	if ok == false {
		return empty, errors.New("HGETALL : value not list")
	}
	return result, nil
}

// hgetall func channel
func hgetallUsingChan(rcon rds.Conn, ch chan<- interface{}, key string) {
	defer close(ch)

	v, err := rcon.Do("HGETALL", key)
	if err != nil {
		if err != rds.ErrNil {
			log.Println("HGETALL : " + err.Error())
		}
		return
	}
	ch <- v
	return
}

// HSET
func (h *RedisHandler) Hset(key, field string, value []byte) (int, error) {
	err := h.Sema.Acquire()
	if err != nil {
		return 0, err
	}
	defer h.Sema.Release()

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

	if config.Cfg.General.Duplicate {
		go func() {
			v, err = origConn.Do("HSET", key, field, value)
			if err != nil {
				log.Println("HSET : err when set : " + err.Error())
			}
			return
		}()
	}

	int64v, ok := v.(int64)
	intv := int(int64v)
	if ok == false {
		return 0, errors.New("HSET : value not int")
	}
	return intv, nil
}

// SISMEMBER 2 side
func (h *RedisHandler) Sismember(set, field string) (int, error) {
	err := h.Sema.Acquire()
	if err != nil {
		return 0, err
	}
	defer h.Sema.Release()

	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	chOrig := make(chan interface{})
	chDest := make(chan interface{})

	go sismemberUsingChan(origConn, chOrig, set, field)
	go sismemberUsingChan(destConn, chDest, set, field)

	// wait completion.
	valOrig := <-chOrig
	valDest := <-chDest

	// default exist value
	valExist := valDest

	if valDest == nil || valDest.(int64) == 0 {
		if valOrig == nil || valOrig.(int64) == 0 {
			return 0, nil // both nil, key not found
		} else {
			//move all set
			go func() {
				err := moveSet(set)
				if err != nil {
					log.Println(err)
				}
				return
			}()
		}
		valExist = valOrig
	}

	int64v, ok := valExist.(int64)
	intv := int(int64v)
	if ok == false {
		return 0, errors.New("SISMEMBER : value not int")
	}
	return intv, nil
}

// sismember func channel
func sismemberUsingChan(rcon rds.Conn, ch chan<- interface{}, set, field string) {
	defer close(ch)

	v, err := rcon.Do("SISMEMBER", set, field)
	if err != nil {
		if err != rds.ErrNil {
			log.Println("SISMEMBER : " + err.Error())
		}
		return
	}
	ch <- v
	return
}

// SMEMBERS 2 side
func (h *RedisHandler) Smembers(set string) ([]interface{}, error) {
	err := h.Sema.Acquire()
	if err != nil {
		return nil, err
	}
	defer h.Sema.Release()

	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	chOrig := make(chan interface{})
	chDest := make(chan interface{})

	go smemberUsingChan(origConn, chOrig, set)
	go smemberUsingChan(destConn, chDest, set)

	// wait completion.
	valOrig := <-chOrig
	valDest := <-chDest

	// default exist value
	valExist := valDest
	var empty []interface{}

	valDestArr, ok := valDest.([]interface{})
	if valDest == nil || !ok || len(valDestArr) == 0 {
		valOrigArr, ok := valOrig.([]interface{})
		if valOrig == nil || !ok || len(valOrigArr) == 0 {
			return empty, errors.New("SMEMBERS : keys not found") // both nil, key not found
		} else {
			//move all set
			go func() {
				err := moveSet(set)
				if err != nil {
					log.Println(err)
				}
				return
			}()
		}
		valExist = valOrig // set exist value
	}

	result, ok := valExist.([]interface{})
	if ok == false {
		return empty, errors.New("SMEMBERS : value not list")
	}
	return result, nil
}

// smembers func channel
func smemberUsingChan(rcon rds.Conn, ch chan<- interface{}, set string) {
	defer close(ch)

	v, err := rcon.Do("SMEMBERS", set)
	if err != nil {
		if err != rds.ErrNil {
			log.Println("SMEMBERS : " + err.Error())
		}
		return
	}
	ch <- v
	return
}

// SADD
func (h *RedisHandler) Sadd(set string, val []byte) (int, error) {
	err := h.Sema.Acquire()
	if err != nil {
		return 0, err
	}
	defer h.Sema.Release()

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
	if config.Cfg.General.Duplicate {
		go func() {
			v, err = origConn.Do("SADD", set, val)
			if err != nil {
				log.Println("SADD : err when check exist in origin : " + err.Error())
			}
			return
		}()
	}

	int64v, ok := v.(int64)
	intv := int(int64v)
	if ok == false {
		return 0, errors.New("SADD : value not int")
	}
	return intv, nil
}

// SREM
func (h *RedisHandler) Srem(set string, val []byte) (int, error) {
	err := h.Sema.Acquire()
	if err != nil {
		return 0, err
	}
	defer h.Sema.Release()

	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	v, err := destConn.Do("SREM", set, val)
	if err != nil {
		return 0, errors.New("SREM : err when check exist in origin : " + err.Error())
	}

	if config.Cfg.General.Duplicate {
		go func() {
			v, err = origConn.Do("SREM", set, val)
			if err != nil {
				log.Println("SREM : err when check exist in origin : " + err.Error())
			}
			return
		}()
	}
	int64v, ok := v.(int64)
	intv := int(int64v)
	if ok == false {
		return 0, errors.New("SREM : value not int")
	}
	return intv, nil
}

// SETEX
func (h *RedisHandler) Setex(key string, value int, val string) ([]byte, error) {
	err := h.Sema.Acquire()
	if err != nil {
		return nil, err
	}
	defer h.Sema.Release()

	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	v, err := destConn.Do("SETEX", key, value, val)
	if err != nil {
		return nil, errors.New("SETEX : err when set : " + err.Error())
	}

	if config.Cfg.General.Duplicate {
		go func() {
			v, err = origConn.Do("SETEX", key, value, val)
			if err != nil {
				log.Println("SETEX : err when set duplicate: " + err.Error())
			}
		}()
	}
	//could ignore all in origin because set on dest already success
	//del old key in origin
	if !config.Cfg.General.Duplicate {
		go func() {
			origConn.Do("DEL", key)
			return
		}()
	}

	strv, ok := v.(string)
	if ok == false {
		return nil, errors.New("SETEX : value not string")
	}
	return []byte(strv), nil
}

// EXPIRE
func (h *RedisHandler) Expire(key string, value int) (int, error) {
	err := h.Sema.Acquire()
	if err != nil {
		return 0, err
	}
	defer h.Sema.Release()

	origConn := connection.RedisPoolConnection.Origin.Get()
	destConn := connection.RedisPoolConnection.Destination.Get()

	chOrig := make(chan interface{})
	chDest := make(chan interface{})

	go expireUsingChan(origConn, chOrig, key, value)
	go expireUsingChan(destConn, chDest, key, value)

	// wait completion.
	valOrig := <-chOrig
	valDest := <-chDest

	// default exist value
	valExist := valDest

	if valDest == nil || valDest.(int64) == 0 {
		if valOrig == nil {
			return 0, errors.New("EXPIRE : keys not found") //both nil, key not found
		}
		valExist = valOrig
	}

	int64v, ok := valExist.(int64)
	intv := int(int64v)
	if ok == false {
		return 0, errors.New("EXPIRE : value not int")
	}
	return intv, nil
}

// expire func channel
func expireUsingChan(rcon rds.Conn, ch chan<- interface{}, key string, value int) {
	defer close(ch)

	v, err := rcon.Do("EXPIRE", key, value)
	if err != nil {
		if err != rds.ErrNil {
			log.Println("EXPIRE : " + err.Error())
		}
		return
	}
	ch <- v
	return
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
			if !config.Cfg.General.Duplicate {
				_, err = origConn.Do("DEL", key)
				if err != nil {
					return errors.New("err when del on hexist : " + err.Error())
				}
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
			if !config.Cfg.General.Duplicate {
				//delete from origin
				_, err = origConn.Do("DEL", set)
				if err != nil {
					return errors.New("err when del on hexist : " + err.Error())
				}
			}
		}
	}
	return nil
}
