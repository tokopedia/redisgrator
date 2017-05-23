package config

import (
	"gopkg.in/gcfg.v1"
	"log"
)

type RedisHostCfg struct {
	Origin      string
	Destination string
}

type General struct {
	Port             int
	SetToDestWhenGet bool
	MoveHash         bool
	MoveSet          bool
}

type Config struct {
	General   General
	RedisHost RedisHostCfg
}

var Cfg Config

func ReadConfig(path string) bool {
	err := gcfg.ReadFileInto(&Cfg, path+"config.ini")
	log.Printf("%+v", Cfg)
	if err = Cfg.Validate(); err != nil {
		log.Println("failed read config ", err.Error())
		return false
	}
	if err == nil {
		log.Println("read config from ", path)
		return true
	}
	log.Println("Error : ", err)
	return false
}

func (c *Config) Validate() error {
	//could adding validate to more config
	return nil
}
