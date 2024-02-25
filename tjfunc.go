package tjfunc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

type CostRecord struct {
	Date   spanner.NullDate
	Cost   float64
	Amount int64
}

func SendSlackMessage(ctx context.Context, client *spanner.Client) {
	// now := time.Now()

	runningTotalCost := GetRunningTotalCostToDate(ctx, client)
	runningTotalCostPerDate := GetRunningTotalCostPerDate(ctx, client)
	runningAverageCost := GetRunningAverageCostToDate(ctx, client)
	numMessagesProcessed := GetNumMessagesProcessed(ctx, client)

	output := strings.Builder{}
	output.WriteString("```\n")
	output.WriteString("Running Total Cost to Date: ")
	output.WriteString(fmt.Sprintf("$%.2f\n", runningTotalCost))
	output.WriteString("Running Total Cost Per Date: \n")
	for _, record := range runningTotalCostPerDate {
		output.WriteString(fmt.Sprintf("%s: $%.2f\n", record.Date.String(), record.Cost))
	}
	output.WriteString(fmt.Sprintf("Running Average Cost to Date: $%.2f\n", runningAverageCost))
	output.WriteString(fmt.Sprintf("Number of Messages Processed So Far: %d\n", numMessagesProcessed))
	output.WriteString("```")

	// Print the message in the terminal
	fmt.Println(output.String())

	// Send the message to Slack
	slackWebhookURL := "https://hooks.slack.com/services/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"
	payload := map[string]string{"text": output.String()}
	payloadBytes, _ := json.Marshal(payload)
	resp, err := http.Post(slackWebhookURL, "application/json", bytes.NewBuffer(payloadBytes))
	if err != nil {
		log.Printf("Error sending message to Slack: %v", err)
		return
	}
	defer resp.Body.Close()

	// Check the response status code
	if resp.StatusCode != http.StatusOK {
		log.Printf("Slack API returned non-200 status code: %v", resp.StatusCode)
		return
	}

	log.Println("Slack message sent successfully!")

	// fmt.Println("Time taken to process and send message:", time.Since(now))
}

func GetRunningTotalCostToDate(ctx context.Context, client *spanner.Client) float64 {
	stmt := spanner.Statement{
		SQL: `SELECT SUM(cost) AS running_total_cost FROM jet_tbl`,
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var runningTotalCost spanner.NullFloat64
	row, err := iter.Next()
	if err != nil {
		log.Fatalf("Error fetching running total cost: %v", err)
	}
	if err := row.ColumnByName("running_total_cost", &runningTotalCost); err != nil {
		log.Fatalf("Error reading running total cost: %v", err)
	}

	return runningTotalCost.Float64
}

func GetRunningTotalCostPerDate(ctx context.Context, client *spanner.Client) []CostRecord {
	stmt := spanner.Statement{
		SQL: `SELECT date, SUM(cost) AS total_cost FROM jet_tbl GROUP BY date ORDER BY date`,
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var runningTotalCostPerDate []CostRecord
	for {
		var record CostRecord
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Error fetching results: %v", err)
		}
		if err := row.Columns(&record.Date, &record.Cost); err != nil {
			log.Fatalf("Error reading row: %v", err)
		}
		runningTotalCostPerDate = append(runningTotalCostPerDate, record)
	}

	return runningTotalCostPerDate
}
func GetRunningAverageCostToDate(ctx context.Context, client *spanner.Client) float64 {
	stmt := spanner.Statement{
		SQL: `SELECT AVG(cost) AS running_avg_cost FROM jet_tbl`,
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var runningAverageCost spanner.NullFloat64
	row, err := iter.Next()
	if err != nil {
		log.Fatalf("Error fetching running average cost: %v", err)
	}
	if err := row.ColumnByName("running_avg_cost", &runningAverageCost); err != nil {
		log.Fatalf("Error reading running average cost: %v", err)
	}

	return runningAverageCost.Float64
}

func GetNumMessagesProcessed(ctx context.Context, client *spanner.Client) int64 {
	currentTimestamp := time.Now() // Get the current timestamp
	stmt := spanner.Statement{
		SQL: `SELECT COUNT(*) AS num_messages FROM jet_tbl WHERE TIMESTAMP(date) <= @currentTimestamp`,
		Params: map[string]interface{}{
			"currentTimestamp": currentTimestamp,
		},
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var numMessages int64
	row, err := iter.Next()
	if err != nil {
		log.Fatalf("Error fetching number of messages processed: %v", err)
	}
	if err := row.ColumnByName("num_messages", &numMessages); err != nil {
		log.Fatalf("Error reading number of messages processed: %v", err)
	}

	return numMessages
}
