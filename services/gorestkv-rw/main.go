package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gin-gonic/gin"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// KeyValue represents the key-value pair model
type KeyValue struct {
	Key   string `gorm:"primaryKey" json:"key" binding:"required"` // Validation: key is required
	Value string `json:"value" binding:"required"`                 // Validation: value is required
}

var db *gorm.DB

// Initialize the SQLite database and migrate the schema
func initDatabase() {
	var err error
	dataDir := filepath.Join(".", "data")
	os.MkdirAll(dataDir, os.ModePerm)
	db, err = gorm.Open(sqlite.Open("data/keyvaluestore.db"), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect to the database: %v", err)
	}

	// Auto-migrate the KeyValue model
	db.AutoMigrate(&KeyValue{})
}

func main() {
	// Initialize the database
	initDatabase()

	// Create a new gin router
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	// Define the routes
	r.GET("/kv/:key", getValue)
	r.POST("/kv", blindWrite)
	r.DELETE("/kv/:key", deleteValue)

	// Start the server
	log.Printf("Starting restkv-rw at :8080")
	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}

// getValue retrieves the value associated with a given key
func getValue(c *gin.Context) {
	key := c.Param("key")

	var kv KeyValue
	if err := db.First(&kv, "key = ?", key).Error; err != nil {
		// return empty if key not found
		kv.Key = key
		kv.Value = ""
		c.JSON(http.StatusOK, kv)
		return
	}

	c.JSON(http.StatusOK, kv)
}

// deleteValue deletes a key-value pair
func deleteValue(c *gin.Context) {
	key := c.Param("key")

	if err := db.Delete(&KeyValue{}, "key = ?", key).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete key"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Key deleted"})
}

// blindWrite performs an upsert: inserts if the key does not exist, updates if it does
func blindWrite(c *gin.Context) {
	var kv KeyValue
	if err := c.ShouldBindJSON(&kv); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Key and value are required", "details": err.Error()})
		return
	}

	// Upsert logic: First, try to get the existing key
	var existing KeyValue
	if err := db.First(&existing, "key = ?", kv.Key).Error; err != nil {
		// If key doesn't exist, create a new record
		db.Create(&kv)
		c.JSON(http.StatusCreated, kv)
	} else {
		// If key exists, update the value
		existing.Value = kv.Value
		db.Save(&existing)
		c.JSON(http.StatusOK, existing)
	}
}
