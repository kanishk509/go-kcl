/*
 * Copyright (c) 2018 VMware, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
// The implementation is derived from https://github.com/patrobinson/gokini
//
// Copyright 2018 Patrick robinson
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
package worker

import (
	"math"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	deagg "github.com/awslabs/kinesis-aggregation/go/deaggregator"

	chk "github.com/kanishk509/go-kcl/clientlibrary/checkpoint"
	"github.com/kanishk509/go-kcl/clientlibrary/config"
	kcl "github.com/kanishk509/go-kcl/clientlibrary/interfaces"
	"github.com/kanishk509/go-kcl/clientlibrary/metrics"
	par "github.com/kanishk509/go-kcl/clientlibrary/partition"
)

const (
	// This is the initial state of a shard consumer. This causes the consumer to remain blocked until the all
	// parent shards have been completed.
	WaitingOnParentShards ShardConsumerState = iota + 1

	// ErrCodeKMSThrottlingException is defined in the API Reference https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/#Kinesis.GetRecords
	// But it's not a constant?
	ErrCodeKMSThrottlingException = "KMSThrottlingException"
)

type ShardConsumerState int

// ShardConsumer is responsible for consuming data records of a (specified) shard.
// Note: ShardConsumer only deal with one shard.
type ShardConsumer struct {
	streamName      string
	shard           *par.ShardStatus
	kc              kinesisiface.KinesisAPI
	checkpointer    chk.Checkpointer
	recordProcessor kcl.IRecordProcessor
	kclConfig       *config.KinesisClientLibConfiguration
	stop            *chan struct{}
	consumerID      string
	mService        metrics.MonitoringService
	state           ShardConsumerState
}

func (sc *ShardConsumer) getShardIterator(shard *par.ShardStatus) (*string, error) {
	log := sc.kclConfig.Logger

	// Get checkpoint of the shard from dynamoDB
	err := sc.checkpointer.FetchShardStatus(shard)
	if err != nil && err != chk.ErrSequenceIDNotFound {
		return nil, err
	}

	// If there isn't any checkpoint for the shard, use the configuration value.
	if shard.Checkpoint == "" {
		initPos := sc.kclConfig.InitialPositionInStream
		shardIteratorType := config.InitalPositionInStreamToShardIteratorType(initPos)
		log.Debugf("No checkpoint recorded for shard: %v, starting with: %v", shard.ID,
			aws.StringValue(shardIteratorType))

		var shardIterArgs *kinesis.GetShardIteratorInput
		if initPos == config.AT_TIMESTAMP {
			shardIterArgs = &kinesis.GetShardIteratorInput{
				ShardId:           &shard.ID,
				ShardIteratorType: shardIteratorType,
				Timestamp:         sc.kclConfig.InitialPositionInStreamExtended.Timestamp,
				StreamName:        &sc.streamName,
			}
		} else {
			shardIterArgs = &kinesis.GetShardIteratorInput{
				ShardId:           &shard.ID,
				ShardIteratorType: shardIteratorType,
				StreamName:        &sc.streamName,
			}
		}

		iterResp, err := sc.kc.GetShardIterator(shardIterArgs)
		if err != nil {
			return nil, err
		}
		return iterResp.ShardIterator, nil
	}

	log.Debugf("Start shard: %v at checkpoint: %v", shard.ID, shard.Checkpoint)
	shardIterArgs := &kinesis.GetShardIteratorInput{
		ShardId:                &shard.ID,
		ShardIteratorType:      aws.String("AFTER_SEQUENCE_NUMBER"),
		StartingSequenceNumber: &shard.Checkpoint,
		StreamName:             &sc.streamName,
	}
	iterResp, err := sc.kc.GetShardIterator(shardIterArgs)
	if err != nil {
		return nil, err
	}
	return iterResp.ShardIterator, nil
}

// getRecords continously poll one shard for data record
// Precondition: it currently has the lease on the shard.
func (sc *ShardConsumer) getRecords(shard *par.ShardStatus) error {
	defer sc.removeLeaseForClosedShard(shard)

	log := sc.kclConfig.Logger

	// If the shard is child shard, need to wait until the parent finished.
	if err := sc.waitOnParentShard(shard); err != nil {
		// If parent shard has been deleted by Kinesis system already, just ignore the error.
		if err != chk.ErrSequenceIDNotFound {
			log.Errorf("Error in waiting for parent shard: %v to finish. Error: %+v", shard.ParentShardId, err)
			return err
		}
	}

	shardIterator, err := sc.getShardIterator(shard)
	if err != nil {
		log.Errorf("Unable to get shard iterator for %s: %v", shard.ID, err)
		return err
	}

	// Start processing events and notify record processor on shard and starting checkpoint
	input := &kcl.InitializationInput{
		ShardId:                shard.ID,
		ExtendedSequenceNumber: &kcl.ExtendedSequenceNumber{SequenceNumber: aws.String(shard.Checkpoint)},
	}
	sc.recordProcessor.Initialize(input)

	recordCheckpointer := NewRecordProcessorCheckpoint(shard, sc.checkpointer)
	retriedErrors := 0

	for {
		err := sc.checkpointer.FetchShardStatus(shard)
		if err != nil && err != chk.ErrSequenceIDNotFound {
			log.Errorf("Error fetching checkpoint for %s: %+v", shard.ID, err)
			time.Sleep(time.Duration(sc.kclConfig.IdleTimeBetweenReadsInMillis) * time.Millisecond)
			continue
		}

		if shard.ClaimedBy != "" {
			// Another worker wants to steal our shard. So let the lease lapse
			log.Infof("Shard %s being stolen from us", shard.ID)
			shutdownInput := &kcl.ShutdownInput{ShutdownReason: kcl.ZOMBIE}
			sc.recordProcessor.Shutdown(shutdownInput)

			sc.releaseShardForStealing(shard)
			return nil
		}

		if time.Now().UTC().After(shard.LeaseTimeout.Add(-time.Duration(sc.kclConfig.LeaseRefreshPeriodMillis) * time.Millisecond)) {
			log.Debugf("Refreshing lease on shard: %s for worker: %s", shard.ID, sc.consumerID)
			err = sc.checkpointer.GetLease(shard, sc.consumerID)
			if err != nil {
				// if errors.As(err, &chk.ErrLeaseNotAcquired{}) {
				if e, ok := err.(*chk.ErrLeaseNotAcquired); ok {
					log.Warnf("Failed in acquiring lease on shard: %s for worker: %s", shard.ID, sc.consumerID)
					log.Warnf(e.Error())
					return nil
				}

				// log and return error
				log.Errorf("Error in refreshing lease on shard: %s for worker: %s. Error: %+v",
					shard.ID, sc.consumerID, err)
				return err
			}
		}

		getRecordsStartTime := time.Now()

		log.Debugf("Trying to read %d records from %s.", sc.kclConfig.MaxRecords, shard.ID)
		getRecordsArgs := &kinesis.GetRecordsInput{
			Limit:         aws.Int64(int64(sc.kclConfig.MaxRecords)),
			ShardIterator: shardIterator,
		}
		// Get records from stream and retry as needed
		getResp, err := sc.kc.GetRecords(getRecordsArgs)
		if err != nil {
			if awsErr, ok := err.(awserr.Error); ok {
				if awsErr.Code() == kinesis.ErrCodeProvisionedThroughputExceededException || awsErr.Code() == ErrCodeKMSThrottlingException {
					log.Errorf("Error getting records from shard %v: %+v", shard.ID, err)
					retriedErrors++
					// exponential backoff
					// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html#Programming.Errors.RetryAndBackoff
					time.Sleep(time.Duration(math.Exp2(float64(retriedErrors))*100) * time.Millisecond)
					continue
				}
			}
			log.Errorf("Error getting records from Kinesis that cannot be retried: %+v Request: %s", err, getRecordsArgs)
			return err
		}

		// Convert from nanoseconds to milliseconds
		getRecordsTime := time.Since(getRecordsStartTime) / 1000000
		sc.mService.RecordGetRecordsTime(shard.ID, float64(getRecordsTime))

		// reset the retry count after success
		retriedErrors = 0

		if numOrigRecords := len(getResp.Records); numOrigRecords > 0 {
			log.Debugf("Received %d original records from %s.", numOrigRecords, shard.ID)
		}

		// De-aggregate the records if they were published by the KPL.
		dars, err := deagg.DeaggregateRecords(getResp.Records)

		if err != nil {
			// The error is caused by bad KPL publisher and just skip the bad records
			// instead of being stuck here.
			log.Errorf("Error in de-aggregating KPL records: %+v", err)
		}

		// IRecordProcessorCheckpointer
		input := &kcl.ProcessRecordsInput{
			Records:            dars,
			MillisBehindLatest: aws.Int64Value(getResp.MillisBehindLatest),
			Checkpointer:       recordCheckpointer,
		}

		recordLength := len(input.Records)
		recordBytes := int64(0)

		if recordLength > 0 {
			log.Debugf("Received %d de-aggregated records from %s, MillisBehindLatest: %v", recordLength, shard.ID, input.MillisBehindLatest)
		}

		for _, r := range dars {
			recordBytes += int64(len(r.Data))
		}

		if recordLength > 0 || sc.kclConfig.CallProcessRecordsEvenForEmptyRecordList {
			processRecordsStartTime := time.Now()

			// Delivery the events to the record processor
			input.CacheEntryTime = &getRecordsStartTime
			input.CacheExitTime = &processRecordsStartTime

			log.Debugf("Sending records from %s to record processor.", shard.ID)
			sc.recordProcessor.ProcessRecords(input)

			// Convert from nanoseconds to milliseconds
			processedRecordsTiming := time.Since(processRecordsStartTime) / 1000000
			sc.mService.RecordProcessRecordsTime(shard.ID, float64(processedRecordsTiming))
		}

		sc.mService.IncrRecordsProcessed(shard.ID, recordLength)
		sc.mService.IncrBytesProcessed(shard.ID, recordBytes)
		sc.mService.MillisBehindLatest(shard.ID, float64(*getResp.MillisBehindLatest))

		// Idle time before next reads if we reached the latest record in the stream, i.e. fetched records < queried records
		if recordLength < sc.kclConfig.MaxRecords && aws.Int64Value(getResp.MillisBehindLatest) < int64(sc.kclConfig.IdleTimeBetweenReadsInMillis) {
			time.Sleep(time.Duration(sc.kclConfig.IdleTimeBetweenReadsInMillis) * time.Millisecond)
		}

		// The shard has been closed, so no new records can be read from it
		if getResp.NextShardIterator == nil {
			log.Infof("Shard %s closed", shard.ID)
			shutdownInput := &kcl.ShutdownInput{ShutdownReason: kcl.TERMINATE, Checkpointer: recordCheckpointer}
			sc.recordProcessor.Shutdown(shutdownInput)
			return nil
		}

		shardIterator = getResp.NextShardIterator

		select {
		case <-*sc.stop:
			shutdownInput := &kcl.ShutdownInput{ShutdownReason: kcl.REQUESTED, Checkpointer: recordCheckpointer}
			sc.recordProcessor.Shutdown(shutdownInput)
			return nil
		default:
		}
	}
}

// Need to wait until the parent shard finished
func (sc *ShardConsumer) waitOnParentShard(shard *par.ShardStatus) error {
	if len(shard.ParentShardId) == 0 {
		return nil
	}

	pshard := &par.ShardStatus{
		ID:  shard.ParentShardId,
		Mux: &sync.Mutex{},
	}

	for {
		if err := sc.checkpointer.FetchShardStatus(pshard); err != nil {
			return err
		}

		// Parent shard is finished.
		if pshard.Checkpoint == chk.ShardEnd {
			return nil
		}

		time.Sleep(time.Duration(sc.kclConfig.ParentShardPollIntervalMillis) * time.Millisecond)
	}
}

// Cleanup the internal lease cache
func (sc *ShardConsumer) removeLeaseForClosedShard(shard *par.ShardStatus) {
	log := sc.kclConfig.Logger

	if shard.GetLeaseOwner() != sc.consumerID {
		return
	}

	log.Infof("Remove lease for shard %s", shard.ID)
	shard.SetLeaseOwner("")

	// Release the shard
	// Note: we don't need to do anything in case of error here and shard lease will eventually expire
	if err := sc.checkpointer.RemoveLeaseOwner(shard.ID, sc.consumerID); err != nil {
		log.Errorf("Failed to remove shard lease: %s Error: %+v", shard.ID, err)
	}

	// reporting lease lose metrics
	sc.mService.LeaseLost(shard.ID)
}

func (sc *ShardConsumer) releaseShardForStealing(shard *par.ShardStatus) {
	log := sc.kclConfig.Logger

	log.Infof("Release lease for shard %s", shard.ID)
	shard.SetLeaseOwner(chk.ShardReleased)

	// Release the shard
	// Note: we don't need to do anything in case of error here and shard lease will eventually expire
	if err := sc.checkpointer.ReleaseShard(shard.ID, sc.consumerID); err != nil {
		log.Errorf("Failed to release shard for stealing: %s Error: %+v", shard.ID, err)
	}

	// reporting lease lose metrics
	sc.mService.LeaseLost(shard.ID)
}
