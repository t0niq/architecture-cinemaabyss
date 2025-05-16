package main

import (
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"time"
)

func main() {
	go consumeTopic("movie-events")
	go consumeTopic("user-events")
	go consumeTopic("payment-events")

	r := gin.Default()

	r.GET("/api/events/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": true})
	})

	r.POST("/api/events/movie", func(c *gin.Context) {
		var input MovieEvent
		if err := c.ShouldBindJSON(&input); err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid input"})
			return
		}

		event := Event{
			ID:        "movie-" + input.Action,
			Type:      "movie",
			Timestamp: time.Now().UTC(),
			Payload:   input,
		}

		_, _, err := produceToKafka("movie-events", event)
		if err != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "kafka error"})
			return
		}

		c.JSON(http.StatusCreated, EventResponse{Status: "success", Partition: 0, Offset: 0, Event: event})
	})

	// User event handler
	r.POST("/api/events/user", func(c *gin.Context) {
		var input UserEvent
		if err := c.ShouldBindJSON(&input); err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid input"})
			return
		}

		event := Event{
			ID:        "user-" + input.Action,
			Type:      "user",
			Timestamp: input.Timestamp,
			Payload:   input,
		}

		_, _, err := produceToKafka("user-events", event)
		if err != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "kafka error"})
			return
		}

		c.JSON(http.StatusCreated, EventResponse{Status: "success", Partition: 0, Offset: 0, Event: event})
	})

	// Payment event handler
	r.POST("/api/events/payment", func(c *gin.Context) {
		var input PaymentEvent
		if err := c.ShouldBindJSON(&input); err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{Error: "invalid input"})
			return
		}

		event := Event{
			ID:        "payment-" + input.Status,
			Type:      "payment",
			Timestamp: input.Timestamp,
			Payload:   input,
		}

		_, _, err := produceToKafka("payment-events", event)
		if err != nil {
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "kafka error"})
			return
		}

		c.JSON(http.StatusCreated, EventResponse{Status: "success", Partition: 0, Offset: 0, Event: event})
	})

	log.Println("Event service running on :8082")
	r.Run(":8082")
}
