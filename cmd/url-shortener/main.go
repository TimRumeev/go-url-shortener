package main

import (
	"fmt"

	"ex.com/internal/config"
)

func main() {
	cfg := config.MustLoad()

	fmt.Println(cfg)
}
