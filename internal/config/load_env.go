package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

func LoadEnvVariable() error {
	var path = ".env"

	for range 10 {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			path = "../" + path
		} else {
			break
		}
	}

	if os.Getenv("ENV") == "prod" {
		fmt.Println("Running in production mode")
	} else {
		if err := godotenv.Load(path); err != nil && !os.IsNotExist(err) {
			fmt.Printf("Error loading .env file: %v \n", err)
			return err
		}
		fmt.Println("Running in development mode")
	}

	return nil
}
