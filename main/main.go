package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/golang/glog"
	cfg "github.com/kanishk509/go-kcl/clientlibrary/config"
	kc "github.com/kanishk509/go-kcl/clientlibrary/interfaces"
	wk "github.com/kanishk509/go-kcl/clientlibrary/worker"
	"github.com/kanishk509/go-kcl/logger"
)

const (
	REGION_NAME            = "us-west-2"
	STREAM_NAME            = "nusights-kinesis-datastream-kanishk"
	MAX_CONCURRENT_THREADS = 1
	OUTPUT_FILE_PATH       = "output_dump.txt"
	TABLE_NAME_OUTPUT      = "output-steal-kanishk"
	CONSUMER_APP_NAME      = "steal-consumer-kanishk"
)

var f *os.File

type streaming struct {
	workerID string
}

type OutputTableItem struct {
	Num               int
	ProcessedOnWorker string
}

func (s *streaming) mockConsume(in chan int, dynamoSvc *dynamodb.DynamoDB) {
	for inputNum := range in {
		item := OutputTableItem{
			Num:               inputNum,
			ProcessedOnWorker: s.workerID,
		}

		av, _ := dynamodbattribute.MarshalMap(item)
		input := &dynamodb.PutItemInput{
			Item:      av,
			TableName: aws.String(TABLE_NAME_OUTPUT),
		}
		_, err := dynamoSvc.PutItem(input)
		if err != nil {
			fmt.Println("Got error calling PutItem:")
			fmt.Println(err.Error())
		}
	}
}

func (s *streaming) Subscribe(
	streamName string,
	workerID string,
) error {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-west-2"),
	}))

	dynamoSvc := dynamodb.New(sess)

	loggerForKCL := getLoggerForKCL()

	kclConfig := cfg.NewKinesisClientLibConfig(CONSUMER_APP_NAME, streamName, REGION_NAME, workerID).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(50).
		WithLeaseRefreshPeriodMillis(30 * 1000).
		WithFailoverTimeMillis(60 * 1000).
		WithIdleTimeBetweenReadsInMillis(10 * 1000).
		WithShardSyncIntervalMillis(10 * 1000).
		WithClaimExpirePeriodMillis(30 * 1000).
		WithLogger(loggerForKCL)

	worker := wk.NewWorker(recordProcessorFactory(s, dynamoSvc), kclConfig)

	err := worker.Start()
	if err != nil {
		panic(err)
	}

	//defer worker.Shutdown()

	return nil
}

func recordProcessorFactory(s *streaming, dynamoSvc *dynamodb.DynamoDB) kc.IRecordProcessorFactory {
	return &consumeRecordProcessorFactory{
		s:         s,
		dynamoSvc: dynamoSvc,
	}
}

type consumeRecordProcessorFactory struct {
	s         *streaming
	dynamoSvc *dynamodb.DynamoDB
}

func (cf *consumeRecordProcessorFactory) CreateProcessor() kc.IRecordProcessor {
	return &consumeRecordProcessor{
		s:         cf.s,
		dynamoSvc: cf.dynamoSvc,
	}
}

type consumeRecordProcessor struct {
	s         *streaming
	dynamoSvc *dynamodb.DynamoDB
	in        chan int
}

func (c *consumeRecordProcessor) Initialize(input *kc.InitializationInput) {
	c.in = make(chan int, 100)
	go c.s.mockConsume(c.in, c.dynamoSvc)
}

func (c *consumeRecordProcessor) ProcessRecords(input *kc.ProcessRecordsInput) {
	// don't process empty record
	if len(input.Records) == 0 {
		return
	}

	fmt.Printf("Processing batch of [%d] records.\n", len(input.Records))

	for _, record := range input.Records {
		inputNum, _ := strconv.Atoi(string(record.Data))
		fmt.Fprintf(f, "PROCESSING NUMBER: %d \n", inputNum)
		c.in <- inputNum
	}

	// checkpoint it after processing this batch
	lastRecordSequenceNumber := input.Records[len(input.Records)-1].SequenceNumber

	fmt.Printf("Checkpointing progress.\n")
	err := input.Checkpointer.Checkpoint(lastRecordSequenceNumber)
	if err != nil {
		glog.Error(err)
	}
}

func (c *consumeRecordProcessor) Shutdown(input *kc.ShutdownInput) {
	defer close(c.in)

	fmt.Printf("Shutdown Reason: %v", aws.StringValue(kc.ShutdownReasonMessage(input.ShutdownReason)))

	// When the value of {@link ShutdownInput#getShutdownReason()} is
	// {@link ShutdownReason#TERMINATE} it is required that you
	// checkpoint. Failure to do so will result in an IllegalArgumentException, and the KCL no longer making progress.
	if input.ShutdownReason == kc.TERMINATE {
		input.Checkpointer.Checkpoint(nil)
	}
	if input.ShutdownReason == kc.ZOMBIE {
		// do not checkpoint
	}
}

func getLoggerForKCL() logger.Logger {
	logConfig := logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: false,
		EnableFile:        true,
		FileLevel:         logger.Info,
		FileJSONFormat:    true,
		Filename:          "log.log",
	}
	log := logger.NewLogrusLoggerWithConfig(logConfig)

	return log
}

func main() {
	f, _ = os.OpenFile(OUTPUT_FILE_PATH, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	fmt.Fprint(f, " ==== NEW WORKER STARTED ==== \n")
	var s = new(streaming)
	s.Subscribe(STREAM_NAME, os.Args[1])
	// s.Subscribe(STREAM_NAME, "worker-"+os.Getenv("HOSTNAME"))

	select {}
}
