package orm

import (
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var default_db *gorm.DB

func Open(databaseURL string) (*gorm.DB, error) {
	db, err := gorm.Open(postgres.Open(databaseURL), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("open gorm postgres connection: %w", err)
	}

	default_db = db
	return db, nil
}

func GetDB() (*gorm.DB, error) {
	if default_db == nil {
		return nil, fmt.Errorf("orm db is not initialized")
	}

	return default_db, nil
}

// SetTestDB injects a DB for use in tests. Not safe for concurrent use.
func SetTestDB(db *gorm.DB) {
	default_db = db
}
