package main

import (
	redis "github.com/prima101112/go-redis-server"
	"github.com/tokopedia/redisgrator/src/config"
	"github.com/tokopedia/redisgrator/src/redishandler"
	"github.com/tokopedia/redisgrator/src/redisconnection"
	"log"
	"time"
	"os"
)

func init()  {
	ok := config.ReadConfig("./files/config/")
	if !ok {
		log.Fatal("failed to read config")
		os.Exit(0)
	}
	redisconnection.RedisPoolConnection = redisconnection.RedisConn(config.Cfg.RedisHost.Origin, config.Cfg.RedisHost.Destination)
}

func main() {
	//define redis server handler
	handler := &redishandler.RedisHandler{Start: time.Now()}
	//define default conf
	conf := redis.DefaultConfig().Host("0.0.0.0").Port(config.Cfg.General.Port).Handler(handler)
	//create server with given config
	server, err := redis.NewServer(conf)

	if err != nil {
		log.Println("problem starting redis masquerader server.", err)
	} else {
		log.Printf("starting fake redis server at port :%d\n",config.Cfg.General.Port)
		log.Fatal(server.ListenAndServe())
	}
}
