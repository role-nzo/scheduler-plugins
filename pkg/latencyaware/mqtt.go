package latencyaware

import (
	"fmt"
	"os"
	"os/signal"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var (
	broker   = "tcp://broker.hivemq.com:1883" // Replace with your broker address
	clientID = "clientId-KI02s7qxUZ"
	topic    = "test/topic123678"
)

func main() {
	// Create a new MQTT client
	opts := mqtt.NewClientOptions().AddBroker(broker).SetClientID(clientID)

	// Set up the connection lost handler
	opts.OnConnectionLost = func(c mqtt.Client, err error) {
		fmt.Printf("Connection lost: %v\n", err)
	}

	// Set up the message handler
	opts.OnConnect = func(c mqtt.Client) {
		fmt.Println("Connected to broker")
	}

	// Create and start the MQTT client
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	// Subscribe to a topic
	if token := client.Subscribe(topic, 0, func(c mqtt.Client, m mqtt.Message) {
		fmt.Printf("Received message: %s on topic: %s\n", m.Payload(), m.Topic())
	}); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	// Publish messages
	go func() {
		for {
			message := fmt.Sprintf("Hello from Go at %s", time.Now().Format(time.RFC3339))
			token := client.Publish(topic, 0, false, message)
			token.Wait()
			time.Sleep(2 * time.Second) // Wait for 2 seconds before sending the next message
		}
	}()

	// Wait for interrupt signal to gracefully shut down
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan

	// Clean up
	client.Disconnect(250)
	fmt.Println("Client disconnected")
}
