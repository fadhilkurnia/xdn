package main

import (
	"log"
	"os"

	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3"
)

// Cors is the additional header on every request to server
func Cors() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Add("Access-Control-Allow-Origin", "*")
		c.Next()
	}
}

func main() {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	log.Printf("Starting service with PID=%d\n", os.Getpid())

	dbPath := "./data/data.db"
	if p := os.Getenv("SQLITE_PATH"); p != "" {
		dbPath = p
	}
	log.Printf("Serving data from %s\n", dbPath)

	r.Use(Cors())

	v1 := r.Group("api/v1")
	{
		v1.POST("/users", PostUser)
		v1.GET("/users", GetUsers)
		v1.GET("/users/:id", GetUser)
		v1.PUT("/users/:id", UpdateUser)
		v1.DELETE("/users/:id", DeleteUser)
	}
	// MockUsers()
	err := r.Run(":8000")
	if err != nil {
		log.Fatalf("failed to start web service - %s\n", err.Error())
	}
}
