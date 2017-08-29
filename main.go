package main

import (
	"log"
	"os"
	"time"

	"github.com/google/gops/agent"
	redis "github.com/tokopedia/go-redis-server"
	"github.com/tokopedia/redisgrator/config"
	"github.com/tokopedia/redisgrator/connection"
	"github.com/tokopedia/redisgrator/handler"
)

func init() {
	ok := config.ReadConfig("/etc/")
	if !ok {
		ok = config.ReadConfig("./files/config/")
		if !ok {
			log.Fatal("failed to read config")
			os.Exit(0)
		}
	}
	connection.RedisPoolConnection = connection.RedisConn(config.Cfg.RedisHost.Origin, config.Cfg.RedisHost.Destination)
}

func main() {
	//gops for monitoring
	if err := agent.Listen(nil); err != nil {
		log.Fatal(err)
	}
	//define redis server handler
	handler := &handler.RedisHandler{Start: time.Now(), GuardAsync: make(chan struct{}, config.Cfg.General.MaxGoroutine)}
	//define default conf
	conf := redis.DefaultConfig().Host("0.0.0.0").Port(config.Cfg.General.Port).Handler(handler)
	//create server with given config
	server, err := redis.NewServer(conf)

	if err != nil {
		log.Println("problem starting redis masquerader server.", err)
	} else {
		log.Printf("starting fake redis server at port :%d\n", config.Cfg.General.Port)
		log.Fatal(server.ListenAndServe())
	}
}
