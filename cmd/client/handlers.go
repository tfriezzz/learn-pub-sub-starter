package main

import (
	"fmt"
	"log"
	"time"

	gamelogic "github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	pubsub "github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		fmt.Printf("DEBUG: received PlayingState: %+v\n", ps)
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)

		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			// outcome, winner := gs.HandleWar(rw gamelogic.RecognitionOfWar)
			err := pubsub.PublishJSON(ch,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+gs.GetUsername(),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				})
			if err != nil {
				log.Printf("PublishJSON returned err %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Println("unknown move outcome")
			return pubsub.NackDiscard
		}
		// 	if outcome == gamelogic.MoveOutComeSafe || outcome == gamelogic.MoveOutcomeMakeWar {
		// 		return pubsub.Ack
		// 	} else {
		// 		return pubsub.NackDiscard
		// 	}
	}
}

func handlerWar(gs *gamelogic.GameState, publishCh *amqp.Channel) func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		warOutcome, winner, loser := gs.HandleWar(rw)

		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			if err := publishGameLog(
				publishCh,
				gs.GetUsername(),
				fmt.Sprintf("%s won a war against %s", winner, loser),
			); err != nil {
				fmt.Printf("couldn't publish GameLog")
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			if err := publishGameLog(
				publishCh,
				gs.GetUsername(),
				fmt.Sprintf("%s won a war against %s", winner, loser),
			); err != nil {
				fmt.Printf("couldn't publish GameLog")
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			if err := publishGameLog(
				publishCh,
				gs.GetUsername(),
				fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser),
			); err != nil {
				fmt.Printf("couldn't publish GameLog")
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Println("outcome not recognized")
			return pubsub.NackDiscard
		}
	}
}

func handlerLogs() func(gamelog routing.GameLog) pubsub.AckType {
	return func(gamelog routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")

		err := gamelogic.WriteLog(gamelog)
		if err != nil {
			fmt.Printf("error writing log: %v\n", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}

func publishGameLog(publishCh *amqp.Channel, username string, msg string) error {
	gameLog := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     msg,
		Username:    username,
	}

	if err := pubsub.PublishGob(
		publishCh,
		routing.ExchangePerilTopic,
		routing.GameLogSlug+"."+username,
		gameLog,
	); err != nil {
		return fmt.Errorf("PublishGob returned err: %v", err)
	}

	return nil
}
