package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func handler(ctx context.Context, snsEvent events.SNSEvent) {
	start := time.Now()
	sess := session.Must(session.NewSession())
	downloader := s3manager.NewDownloader(sess)

	integratedURL := os.Getenv("URL")
	bulkCount, envErr := strconv.Atoi(os.Getenv("BULK_COUNT"))

	if envErr != nil {
		log.Fatal("Bulk Count environment variable does not exist")
	}

	log.Printf("Record Count: %d\n", len(snsEvent.Records))

	for _, record := range snsEvent.Records {
		buff := &aws.WriteAtBuffer{}

		var message map[string]interface{}
		json.Unmarshal([]byte(record.SNS.Message), &message)
		records := message["Records"].([]interface{})
		record := records[0].(map[string]interface{})

		// If SNS is not triggered via S3, you can comment.
		bucketName := record["s3"].(map[string]interface{})["bucket"].(map[string]interface{})["name"].(string)
		objectKey, _ := url.PathUnescape(record["s3"].(map[string]interface{})["object"].(map[string]interface{})["key"].(string))

		_, err := downloader.Download(buff, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})

		if err != nil {
			log.Printf("[ERROR] S3 File Download error occured for file %s, exc: %s\n", objectKey, err)
			continue
		}

		log.Printf("[INFO] File downloaded successfully")

		newReader := bufio.NewReader(bytes.NewReader(buff.Bytes()))

		//wg is WaitGroup to understand if all the goroutines are completed and execution can be finished
		var wg sync.WaitGroup
		processes := make([]string, 0)
		for {
			line, err := newReader.ReadString('\n')

			if err == io.EOF {
				log.Printf("[INFO] File process is completed\n")
				break
			} else if err != nil {
				log.Println("[ERROR] Error occured. Exc: ", err)
				break
			}

			processes = append(processes, line)
		}

		endItr := bulkCount
		startItr := 0

		log.Printf("TOTAL: %s\n", strconv.Itoa(len(processes)))

		for endItr < len(processes) {
			wg.Add(1)
			go startProcess(processes[startItr:endItr], &wg, integratedURL)
			startItr = endItr
			endItr += bulkCount
		}

		if endItr > len(processes) {
			wg.Add(1)
			go startProcess(processes[startItr:len(processes)], &wg, integratedURL)
		}

		//If all the goroutines are completed their tasks, execution can be finished.
		wg.Wait()
		elapsed := time.Since(start)
		log.Printf("Elapsed time %s", elapsed)
	}
}

// Parse datas retrieved from SNS payload and append it to the url.
func startProcess(datas []string, wg *sync.WaitGroup, url string) {
	for _, data := range datas {
		// data is unmarshalled to dm and can be used appending new query parameters to the url.
		dm := make(map[string]interface{})
		err := json.Unmarshal([]byte(data), &dm)

		if err != nil {
			log.Printf("[ERROR] SParsing error. Exc: %s\n", err)
		}
		send(url)
	}
	wg.Done()
}

// send is to complete HTTP Get request.
func send(url string) {
	var netClient = http.Client{
		Timeout: time.Second * 10,
	}

	resp, err := netClient.Get(url)
	if resp != nil {
		resp.Body.Close()
	}

	if err != nil {
		log.Printf("[ERROR] Error Occured while sending data. Error: %s\n", err)
		return
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("[ERROR] Fail Result Status Code: %d, URL: %s, Err: %s \n", resp.StatusCode, url, err)
	}
}

func main() {
	lambda.Start(handler)
}
