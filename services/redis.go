package services

import (
	"github.com/chanmaoganda/go-project-template/config"
	"github.com/redis/go-redis/v9"
)

type RedisProxy struct {
	redis *redis.Client
}

func NewRedisProxy(redisCfg *config.RedisConfig) *RedisProxy {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisCfg.Address,
		// Password: "",
		// DB:       0,
	})
	return &RedisProxy{
		redis: rdb,
	}
}
