package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	gamelogic "github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	pubsub "github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	connectionString := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Dial returned err: %v", err)
	}
	defer connection.Close()
	fmt.Println("Connection successful")

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("ClientWelcome returned err: %v", err)
	}

	queueName := fmt.Sprintf("%v.%v", routing.PauseKey, userName)
	_, queue, err := pubsub.DeclareAndBind(connection, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.TransientQueue)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	gamestate := gamelogic.NewGameState(userName)
	if err := pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, fmt.Sprintf("pause.%s", userName), routing.PauseKey, pubsub.TransientQueue, handlerPause(gamestate)); err != nil {
		log.Printf("pubsub.SubscribeJSON returned err: %v\n", err)
	}

loop:
	for {
		input := gamelogic.GetInput()
		switch input[0] {
		case "spawn":
			if err := gamestate.CommandSpawn(input); err != nil {
				log.Printf("CommandSpawn returned err: %v", err)
			}
		case "move":
			move, err := gamestate.CommandMove(input)
			if err != nil {
				log.Printf("CommandMove returned err: %v", err)
				continue
			}
			log.Printf("move: %v, executed succesfully", move)
		case "status":
			gamestate.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			break loop
		default:
			fmt.Println("unknown command")
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	handler := func(ps routing.PlayingState) {
		defer fmt.Print("> ")
		fmt.Printf("DEBUG: received PlayingState: %+v\n", ps)
		gs.HandlePause(ps)
	}

	return handler
}
