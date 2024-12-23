package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	var mongoURI string
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "%s: MONGO_URI\n", os.Args[0])
		os.Exit(1)
	} else {
		mongoURI = os.Args[1]
	}

	replSetMembers, err := getReplSetMembers(mongoURI)
	if err != nil {
		log.Fatalln(err)
	}

	app := tview.NewApplication()
	go refreshInterval(app)

	flex := tview.NewFlex()
	for _, replSetMember := range replSetMembers {
		name := replSetMember.Name
		mongoClient, err := getMongoClient(fmt.Sprintf("mongodb://%s", name), true)
		if err != nil {
			log.Fatalln(err)
		}
		var oplogCounter OplogCounter
		box := tview.NewBox().SetTitle(name).SetBorder(true).SetDrawFunc(func(screen tcell.Screen, x, y, width, height int) (int, int, int, int) {
			serverStatus, err := getMongoServerStatus(mongoClient)
			if err != nil {
				log.Fatalln(err)
			}
			if err = countMongoOplog(mongoClient, &oplogCounter); err != nil {
				log.Fatalln(err)
			}
			tview.Print(screen, fmt.Sprintf("opcounters.insert: %d", serverStatus.Opcounters.Insert), x+1, y+1, width, tview.AlignLeft, tcell.ColorLime)
			tview.Print(screen, fmt.Sprintf("opcounters.update: %d", serverStatus.Opcounters.Update), x+1, y+2, width, tview.AlignLeft, tcell.ColorLime)
			tview.Print(screen, fmt.Sprintf("opcounters.delete: %d", serverStatus.Opcounters.Delete), x+1, y+3, width, tview.AlignLeft, tcell.ColorLime)
			tview.Print(screen, fmt.Sprintf("opcountersRepl.insert: %d", serverStatus.OpcountersRepl.Insert), x+1, y+5, width, tview.AlignLeft, tcell.ColorLime)
			tview.Print(screen, fmt.Sprintf("opcountersRepl.update: %d", serverStatus.OpcountersRepl.Update), x+1, y+6, width, tview.AlignLeft, tcell.ColorLime)
			tview.Print(screen, fmt.Sprintf("opcountersRepl.delete: %d", serverStatus.OpcountersRepl.Delete), x+1, y+7, width, tview.AlignLeft, tcell.ColorLime)
			tview.Print(screen, fmt.Sprintf("metrics.inserted: %d", serverStatus.Metrics.Document.Inserted), x+1, y+9, width, tview.AlignLeft, tcell.ColorLime)
			tview.Print(screen, fmt.Sprintf("metrics.updated: %d", serverStatus.Metrics.Document.Updated), x+1, y+10, width, tview.AlignLeft, tcell.ColorLime)
			tview.Print(screen, fmt.Sprintf("metrics.deleted: %d", serverStatus.Metrics.Document.Deleted), x+1, y+11, width, tview.AlignLeft, tcell.ColorLime)
			tview.Print(screen, fmt.Sprintf("oplog.rs.insert: %d", oplogCounter.Insert), x+1, y+13, width, tview.AlignLeft, tcell.ColorLime)
			tview.Print(screen, fmt.Sprintf("oplog.rs.update: %d", oplogCounter.Update), x+1, y+14, width, tview.AlignLeft, tcell.ColorLime)
			tview.Print(screen, fmt.Sprintf("oplog.rs.delete: %d", oplogCounter.Delete), x+1, y+15, width, tview.AlignLeft, tcell.ColorLime)
			return 0, 0, 0, 0
		})
		flex.AddItem(box, 0, 1, false)
	}
	if err := app.SetRoot(flex, true).SetFocus(flex).Run(); err != nil {
		panic(err)
	}
}

func refreshInterval(app *tview.Application) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		app.Draw()
	}
}

type (
	ReplSetMember struct {
		Name string `bson:"name"`
	}

	ServerStatus struct {
		Opcounters     OpcountersStats `bson:"opcounters"`
		OpcountersRepl OpcountersStats `bson:"opcountersRepl"`
		Metrics        MetricsStats    `bson:"metrics"`
	}

	OpcountersStats struct {
		Insert uint64 `bson:"insert"`
		Update uint64 `bson:"update"`
		Delete uint64 `bson:"delete"`
	}

	MetricsStats struct {
		Document DocumentStats `bson:"document"`
	}

	DocumentStats struct {
		Deleted  uint64 `bson:"deleted"`
		Inserted uint64 `bson:"inserted"`
		Updated  uint64 `bson:"updated"`
	}

	OplogCounter struct {
		OpcountersStats
		Timestamp primitive.Timestamp
		mutex     sync.Mutex
	}

	Oplog struct {
		Timestamp primitive.Timestamp `bson:"ts" json:"ts"`
		Operation string              `bson:"op" json:"op"`
	}
)

func getReplSetMembers(uri string) ([]ReplSetMember, error) {
	type ReplSetGetStatusResult struct {
		Members []ReplSetMember `json:"members"`
	}

	mongoClient, err := getMongoClient(uri, false)
	if err != nil {
		return nil, err
	}

	var replSetGetStatusResult ReplSetGetStatusResult
	if err = mongoClient.Database("admin").RunCommand(context.Background(), bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&replSetGetStatusResult); err != nil {
		return nil, err
	}
	return replSetGetStatusResult.Members, nil
}

func getMongoClient(uri string, direct bool) (*mongo.Client, error) {
	return mongo.Connect(context.Background(),
		options.Client().
			ApplyURI(uri).
			SetConnectTimeout(5*time.Second).
			SetTimeout(30*time.Second).SetDirect(direct),
	)
}

func getMongoServerStatus(client *mongo.Client) (*ServerStatus, error) {
	var serverStatus ServerStatus
	if err := client.Database("admin").RunCommand(context.Background(), bson.D{{Key: "serverStatus", Value: 1}, {Key: "recordStats", Value: 0}}).Decode(&serverStatus); err != nil {
		return nil, err
	}
	return &serverStatus, nil
}

func countMongoOplog(client *mongo.Client, counter *OplogCounter) error {
	counter.mutex.Lock()
	defer counter.mutex.Unlock()

	cursor, err := client.Database("local").Collection("oplog.rs").Find(context.Background(), bson.M{
		"ts": bson.M{"$gt": counter.Timestamp},
	})
	if err != nil {
		return err
	}
	for cursor.Next(context.Background()) {
		var result Oplog
		if err = cursor.Decode(&result); err != nil {
			return err
		}
		switch result.Operation {
		case "i":
			counter.Insert += 1
		case "u":
			counter.Update += 1
		case "d":
			counter.Delete += 1
		}
		counter.Timestamp = result.Timestamp
	}
	return cursor.Err()
}
