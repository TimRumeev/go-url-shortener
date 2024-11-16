package config

import (
	"log"
	"os"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/joho/godotenv"
)

type Config struct {
	Env         string `yaml:"env" env-default:"local"`
	StoragePath string `yaml:"storage_pass" env-required:"true"`
	HTTPServer  `yaml:"http_server"`
}

type HTTPServer struct {
	Address     string        `yaml:"address" env-default:"localhost:8080"`
	Timeout     time.Duration `yaml:"timeout" env-default:"5s"`
	IdleTimeout time.Duration `yaml:"idle_timeout" env-default:"60s"`
}

func MustLoad() *Config {

	err := godotenv.Load("../../.env")
	if err != nil {
		log.Fatalf("unable to read config path: %s", err)
	}

	configPath := os.Getenv("CONFIG_PASS")
	if configPath == "" {
		log.Fatal("CONFIG PASS is not valid")
	}
	//check if file exisst

	if _, err := os.Stat(configPath); os.IsNotExist(err) { //check if file exists
		log.Fatalf("config file does not exists: %s", configPath)
	}

	var cfg Config

	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		log.Fatalf("cleanenv failed to read config: %s", err)
	}

	return &cfg
}
