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
	"math/rand"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"

	chk "github.com/kanishk509/go-kcl/clientlibrary/checkpoint"
	"github.com/kanishk509/go-kcl/clientlibrary/config"
	kcl "github.com/kanishk509/go-kcl/clientlibrary/interfaces"
	"github.com/kanishk509/go-kcl/clientlibrary/metrics"
	par "github.com/kanishk509/go-kcl/clientlibrary/partition"
)

/**
 * Worker is the high level class that Kinesis applications use to start processing data. It initializes and oversees
 * different components (e.g. syncing shard and lease information, tracking shard assignments, and processing data from
 * the shards).
 */
type Worker struct {
	streamName string
	regionName string
	workerID   string

	processorFactory kcl.IRecordProcessorFactory
	kclConfig        *config.KinesisClientLibConfiguration
	kc               kinesisiface.KinesisAPI
	checkpointer     chk.Checkpointer
	mService         metrics.MonitoringService

	stop      *chan struct{}
	waitGroup *sync.WaitGroup
	done      bool

	rng *rand.Rand

	shardStatus map[string]*par.ShardStatus

	shardStealInProgress bool
}

// NewWorker constructs a Worker instance for processing Kinesis stream data.
func NewWorker(factory kcl.IRecordProcessorFactory, kclConfig *config.KinesisClientLibConfiguration) *Worker {
	mService := kclConfig.MonitoringService
	if mService == nil {
		// Replaces nil with noop monitor service (not emitting any metrics).
		mService = metrics.NoopMonitoringService{}
	}

	// Create a pseudo-random number generator and seed it.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	return &Worker{
		streamName:           kclConfig.StreamName,
		regionName:           kclConfig.RegionName,
		workerID:             kclConfig.WorkerID,
		processorFactory:     factory,
		kclConfig:            kclConfig,
		mService:             mService,
		done:                 false,
		rng:                  rng,
		shardStealInProgress: false,
	}
}

// WithKinesis is used to provide Kinesis service for either custom implementation or unit testing.
func (w *Worker) WithKinesis(svc kinesisiface.KinesisAPI) *Worker {
	w.kc = svc
	return w
}

// WithCheckpointer is used to provide a custom checkpointer service for non-dynamodb implementation
// or unit testing.
func (w *Worker) WithCheckpointer(checker chk.Checkpointer) *Worker {
	w.checkpointer = checker
	return w
}

// Run starts consuming data from the stream, and pass it to the application record processors.
func (w *Worker) Start() error {
	log := w.kclConfig.Logger
	if err := w.initialize(); err != nil {
		log.Errorf("Failed to initialize Worker: %+v", err)
		return err
	}

	// Start monitoring service
	log.Infof("Starting monitoring service.")
	if err := w.mService.Start(); err != nil {
		log.Errorf("Failed to start monitoring service: %+v", err)
		return err
	}

	log.Infof("Starting worker event loop.")
	w.waitGroup.Add(1)
	go func() {
		defer w.waitGroup.Done()
		// entering event loop
		w.eventLoop()
	}()
	return nil
}

// Shutdown signals worker to shutdown. Worker will try initiating shutdown of all record processors.
func (w *Worker) Shutdown() {
	log := w.kclConfig.Logger
	log.Infof("Worker shutdown in requested.")

	if w.done || w.stop == nil {
		return
	}

	close(*w.stop)
	w.done = true
	w.waitGroup.Wait()

	w.mService.Shutdown()
	log.Infof("Worker loop is complete. Exiting from worker.")
}

// initialize
func (w *Worker) initialize() error {
	log := w.kclConfig.Logger
	log.Infof("Worker initialization in progress...")

	// Create default Kinesis session
	if w.kc == nil {
		// create session for Kinesis
		log.Infof("Creating Kinesis session")

		s, err := session.NewSession(&aws.Config{
			Region:      aws.String(w.regionName),
			Endpoint:    &w.kclConfig.KinesisEndpoint,
			Credentials: w.kclConfig.KinesisCredentials,
		})

		if err != nil {
			// no need to move forward
			log.Fatalf("Failed in getting Kinesis session for creating Worker: %+v", err)
		}
		w.kc = kinesis.New(s)
	} else {
		log.Infof("Use custom Kinesis service.")
	}

	// Create default dynamodb based checkpointer implementation
	if w.checkpointer == nil {
		log.Infof("Creating DynamoDB based checkpointer")
		w.checkpointer = chk.NewDynamoCheckpoint(w.kclConfig)
	} else {
		log.Infof("Use custom checkpointer implementation.")
	}

	err := w.mService.Init(w.kclConfig.ApplicationName, w.streamName, w.workerID)
	if err != nil {
		log.Errorf("Failed to start monitoring service: %+v", err)
	}

	log.Infof("Initializing Checkpointer")
	if err := w.checkpointer.Init(); err != nil {
		log.Errorf("Failed to start Checkpointer: %+v", err)
		return err
	}

	w.shardStatus = make(map[string]*par.ShardStatus)

	stopChan := make(chan struct{})
	w.stop = &stopChan

	w.waitGroup = &sync.WaitGroup{}

	log.Infof("Initialization complete.")

	return nil
}

// newShardConsumer to create a shard consumer instance
func (w *Worker) newShardConsumer(shard *par.ShardStatus) *ShardConsumer {
	return &ShardConsumer{
		streamName:      w.streamName,
		shard:           shard,
		kc:              w.kc,
		checkpointer:    w.checkpointer,
		recordProcessor: w.processorFactory.CreateProcessor(),
		kclConfig:       w.kclConfig,
		consumerID:      w.workerID,
		stop:            w.stop,
		mService:        w.mService,
		state:           WaitingOnParentShards,
	}
}

// eventLoop
func (w *Worker) eventLoop() {
	log := w.kclConfig.Logger

	var foundShards int
	for {
		// Add [-50%, +50%] random jitter to ShardSyncIntervalMillis. When multiple workers
		// starts at the same time, this decreases the probability of them calling
		// kinesis.DescribeStream at the same time, and hit the hard-limit on aws API calls.
		// On average the period remains the same so that doesn't affect behavior.
		shardSyncSleep := w.kclConfig.ShardSyncIntervalMillis/2 + w.rng.Intn(int(w.kclConfig.ShardSyncIntervalMillis))

		err := w.syncShard()
		if err != nil {
			log.Errorf("Error syncing shards: %+v, Retrying in %d ms...", err, shardSyncSleep)
			time.Sleep(time.Duration(shardSyncSleep) * time.Millisecond)
			continue
		}

		if foundShards == 0 || foundShards != len(w.shardStatus) {
			foundShards = len(w.shardStatus)
			log.Infof("Found %d shards", foundShards)
		}

		// Count the number of leases held by this worker excluding closed shards
		counter := 0
		for _, shard := range w.shardStatus {
			if shard.GetLeaseOwner() == w.workerID && shard.Checkpoint != chk.ShardEnd {
				counter++
			}
		}

		// max number of lease has not been reached yet
		if counter < w.kclConfig.MaxLeasesForWorker {
			for _, shard := range w.shardStatus {
				// already owner of the shard
				if shard.GetLeaseOwner() == w.workerID {
					continue
				}

				err := w.checkpointer.FetchCheckpoint(shard)
				if err != nil {
					// checkpoint not existing yet is not a problem
					if err != chk.ErrSequenceIDNotFound {
						log.Errorf("Error in fetching checkpoint: %+v", err)
						continue
					} else {
						log.Warnf("Error in fetching checkpoint: %+v", err)
					}
				}

				// The shard is closed and we have processed all records
				if shard.Checkpoint == chk.ShardEnd {
					continue
				}

				stealingShard := false
				if shard.ClaimedBy != "" {
					log.Debugf("Found claim on %s by %s.", shard.ID, shard.ClaimedBy)
					if time.Now().UTC().After(shard.LeaseTimeout.Add(time.Duration(w.kclConfig.ClaimExpirePeriodMillis) * time.Millisecond)) {
						// now > LeaseTimeout + ClaimExpire
						// Claim expired
						log.Debugf("Claim by %s on %s expired, clearing the claim.", shard.ClaimedBy, shard.ID)
						w.checkpointer.ClearClaim(shard.ID, shard.ClaimedBy)
						shard.ClaimedBy = ""
					} else if shard.ClaimedBy != w.workerID {
						// claimed by another worker
						log.Debugf("Shard %s being stolen by %s.", shard.ID, shard.ClaimedBy)
						continue
					} else if shard.GetLeaseOwner() != chk.ShardReleased {
						// claimed by us but shard not yet released
						log.Debugf("Shard %s claimed by us(%s) but not yet released.", shard.ID, w.workerID)
						continue
					} else {
						// shard released to us. Can steal shard now
						log.Debugf("Shard %s released us(%s).", shard.ID, w.workerID)
						stealingShard = true
					}
				} else {
					log.Debugf("No claim found on %s.", shard.ID)
				}

				err = w.checkpointer.GetLease(shard, w.workerID)
				if err != nil {
					// cannot get lease on the shard
					// if !errors.As(err, &chk.ErrLeaseNotAcquired{}) {
					log.Errorf("Cannot get lease: %+v", err)
					continue
				}

				if stealingShard {
					log.Debugf("Successfully stole shard %s.", shard.ID)
					w.shardStealInProgress = false
				}

				// log metrics on got lease
				w.mService.LeaseGained(shard.ID)

				log.Infof("Start Shard Consumer for shard: %v", shard.ID)
				sc := w.newShardConsumer(shard)
				w.waitGroup.Add(1)
				go func() {
					defer w.waitGroup.Done()
					if err := sc.getRecords(shard); err != nil {
						log.Errorf("Error in getRecords: %+v", err)
					}
				}()
				// do not grab more leases in this cycle
				break
			}
		}

		err = w.rebalance()
		if err != nil {
			log.Errorf("Error in rebalancing : %+v", err)
		}

		select {
		case <-*w.stop:
			log.Infof("Shutting down...")
			return
		case <-time.After(time.Duration(shardSyncSleep) * time.Millisecond):
			log.Debugf("Waited %d ms to sync shards...", shardSyncSleep)
		}
	}
}

func (w *Worker) rebalance() error {
	log := w.kclConfig.Logger

	numActiveShards, workers, err := w.checkpointer.FetchActiveShardsAndWorkers()
	if err != nil {
		log.Errorf("Error in fetching workers")
		return err
	}

	err = w.syncShard()
	if err != nil {
		return err
	}

	if w.shardStealInProgress {
		for _, shard := range w.shardStatus {
			if shard.ClaimedBy == w.workerID {
				log.Debugf("Shard steal in progress: %s is stealing %s from %s", shard.ID, w.workerID, shard.AssignedTo)
				return nil
			}
		}

		// shard stealing was interrupted
		w.shardStealInProgress = false
	}

	numActiveWorkers := len(workers)

	if numActiveWorkers >= numActiveShards {
		log.Debugf("1:1 shard allocation. More workers are redundant")
		return nil
	}

	ownedShards, ok := workers[w.workerID]
	var numOwnedShards int
	if ok {
		numOwnedShards = len(ownedShards)
	} else {
		numOwnedShards = 0
		numActiveWorkers++
	}

	optimalNumShards := numActiveShards / numActiveWorkers

	log.Debugf("Number of shards: %d", numActiveShards)
	log.Debugf("Number of active workers: %d", numActiveWorkers)
	log.Debugf("Optimal num of shards: %d", optimalNumShards)
	log.Debugf("Currently owned shards: %d", numOwnedShards)

	if numOwnedShards+1 > optimalNumShards || numOwnedShards+1 > w.kclConfig.MaxLeasesForWorker {
		log.Debugf("Have enough shards, not stealing any")
		return nil
	}

	maxNumShards := optimalNumShards
	var stealFrom string

	for w, shards := range workers {
		if len(shards) > maxNumShards {
			maxNumShards = len(shards)
			stealFrom = w
		}
	}

	if stealFrom == "" {
		log.Debugf("Not all shards allocated. Not stealing any")
		return nil
	}

	// steal a random shard from the worker with the maximum shards
	w.shardStealInProgress = true
	rand.Seed(time.Now().Unix())
	shardToSteal := workers[stealFrom][rand.Intn(maxNumShards)]

	log.Debugf("Attempting to steal shard: %s. From %s to %s", shardToSteal, stealFrom, w.workerID)
	err = w.checkpointer.ClaimShard(&par.ShardStatus{
		ID: shardToSteal,
	}, stealFrom, w.workerID)
	if err != nil {
		w.shardStealInProgress = true
	}

	return err
}

// List all shards and store them into shardStatus table
// If shard has been removed, need to exclude it from cached shard status.
func (w *Worker) getShardIDs(nextToken string, shardInfo map[string]bool) error {
	log := w.kclConfig.Logger

	args := &kinesis.ListShardsInput{}

	// When you have a nextToken, you can't set the streamName
	if nextToken != "" {
		args.NextToken = aws.String(nextToken)
	} else {
		args.StreamName = aws.String(w.streamName)
	}

	listShards, err := w.kc.ListShards(args)
	if err != nil {
		log.Errorf("Error in ListShards: %s Error: %+v Request: %s", w.streamName, err, args)
		return err
	}

	for _, s := range listShards.Shards {
		shardInfo[*s.ShardId] = true

		// found new shard
		if _, ok := w.shardStatus[*s.ShardId]; !ok {
			log.Infof("Found new shard with id %s", *s.ShardId)
			w.shardStatus[*s.ShardId] = &par.ShardStatus{
				ID:                     *s.ShardId,
				ParentShardId:          aws.StringValue(s.ParentShardId),
				Mux:                    &sync.Mutex{},
				StartingSequenceNumber: aws.StringValue(s.SequenceNumberRange.StartingSequenceNumber),
				EndingSequenceNumber:   aws.StringValue(s.SequenceNumberRange.EndingSequenceNumber),
			}
		}
	}

	if listShards.NextToken != nil {
		err := w.getShardIDs(aws.StringValue(listShards.NextToken), shardInfo)
		if err != nil {
			log.Errorf("Error in ListShards: %s Error: %+v Request: %s", w.streamName, err, args)
			return err
		}
	}

	return nil
}

// syncShard to sync the cached shard info with actual shard info from Kinesis
func (w *Worker) syncShard() error {
	log := w.kclConfig.Logger
	shardInfo := make(map[string]bool)
	err := w.getShardIDs("", shardInfo)

	if err != nil {
		return err
	}

	for _, shard := range w.shardStatus {
		// The cached shard no longer existed, remove it.
		if _, ok := shardInfo[shard.ID]; !ok {
			// remove the shard from local status cache
			delete(w.shardStatus, shard.ID)
			// remove the shard entry in dynamoDB as well
			// Note: syncShard runs periodically. we don't need to do anything in case of error here.
			if err := w.checkpointer.RemoveLeaseInfo(shard.ID); err != nil {
				log.Errorf("Failed to remove shard lease info: %s Error: %+v", shard.ID, err)
			}
		}
	}

	return nil
}
