package main

import (
	"log"
	"os"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/eapache/go-resiliency/semaphore"
	"github.com/google/gops/agent"
	redis "github.com/tokopedia/go-redis-server"
	"github.com/tokopedia/redisgrator/config"
	"github.com/tokopedia/redisgrator/connection"
	"github.com/tokopedia/redisgrator/handler"
	logging "gopkg.in/tokopedia/logging.v1"
)

func init() {
	logging.LogInit()
	log.SetFlags(log.LstdFlags | log.Llongfile)
	debug := logging.Debug.Println
	debug("app started")

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
	if err := agent.Listen(agent.Options{
		ShutdownCleanup: true, // automatically closes on os.Interrupt
	}); err != nil {
		log.Fatal(err)
	}

	//define redis server handler
	handler := &handler.RedisHandler{
		Start: time.Now(),
		Sema:  semaphore.New(config.Cfg.General.MaxSema, time.Duration(config.Cfg.General.TimeoutSema)*time.Second),
	}
	//define default conf
	conf := redis.DefaultConfig().Host("0.0.0.0").Port(config.Cfg.General.Port).Handler(handler)
	//create server with given config
	server, err := redis.NewServer(conf)

	if err != nil {
		log.Println("problem starting redis masquerader server.", err)
	} else {
		log.Printf("starting fake redis server at port :%d\n", config.Cfg.General.Port)
		go http.ListenAndServe(":20000", nil)
		log.Fatal(server.ListenAndServe())
	}
}
