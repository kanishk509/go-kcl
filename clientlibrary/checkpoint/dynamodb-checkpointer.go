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
package checkpoint

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"

	"github.com/kanishk509/go-kcl/clientlibrary/config"
	par "github.com/kanishk509/go-kcl/clientlibrary/partition"
	"github.com/kanishk509/go-kcl/logger"
)

const (
	// ErrInvalidDynamoDBSchema is returned when there are one or more fields missing from the table
	ErrInvalidDynamoDBSchema = "The DynamoDB schema is invalid and may need to be re-created"

	// NumMaxRetries is the max times of doing retry
	NumMaxRetries = 10
)

// DynamoCheckpoint implements the Checkpoint interface using DynamoDB as a backend
type DynamoCheckpoint struct {
	log                     logger.Logger
	TableName               string
	leaseTableReadCapacity  int64
	leaseTableWriteCapacity int64

	LeaseDuration int
	svc           dynamodbiface.DynamoDBAPI
	kclConfig     *config.KinesisClientLibConfiguration
	Retries       int
}

func NewDynamoCheckpoint(kclConfig *config.KinesisClientLibConfiguration) *DynamoCheckpoint {
	checkpointer := &DynamoCheckpoint{
		log:                     kclConfig.Logger,
		TableName:               kclConfig.TableName,
		leaseTableReadCapacity:  int64(kclConfig.InitialLeaseTableReadCapacity),
		leaseTableWriteCapacity: int64(kclConfig.InitialLeaseTableWriteCapacity),
		LeaseDuration:           kclConfig.FailoverTimeMillis,
		kclConfig:               kclConfig,
		Retries:                 NumMaxRetries,
	}

	return checkpointer
}

// WithDynamoDB is used to provide DynamoDB service
func (checkpointer *DynamoCheckpoint) WithDynamoDB(svc dynamodbiface.DynamoDBAPI) *DynamoCheckpoint {
	checkpointer.svc = svc
	return checkpointer
}

// Init initialises the DynamoDB Checkpoint
func (checkpointer *DynamoCheckpoint) Init() error {
	checkpointer.log.Infof("Creating DynamoDB session")

	s, err := session.NewSession(&aws.Config{
		Region:      aws.String(checkpointer.kclConfig.RegionName),
		Endpoint:    aws.String(checkpointer.kclConfig.DynamoDBEndpoint),
		Credentials: checkpointer.kclConfig.DynamoDBCredentials,
		Retryer: client.DefaultRetryer{
			NumMaxRetries:    checkpointer.Retries,
			MinRetryDelay:    client.DefaultRetryerMinRetryDelay,
			MinThrottleDelay: client.DefaultRetryerMinThrottleDelay,
			MaxRetryDelay:    client.DefaultRetryerMaxRetryDelay,
			MaxThrottleDelay: client.DefaultRetryerMaxRetryDelay,
		},
	})

	if err != nil {
		// no need to move forward
		checkpointer.log.Fatalf("Failed in getting DynamoDB session for creating Worker: %+v", err)
	}

	if checkpointer.svc == nil {
		checkpointer.svc = dynamodb.New(s)
	}

	if !checkpointer.doesTableExist() {
		return checkpointer.createTable()
	}
	return nil
}

// GetLease attempts to gain a lock on the given shard
func (checkpointer *DynamoCheckpoint) GetLease(shard *par.ShardStatus, newAssignTo string) error {
	newLeaseTimeout := time.Now().Add(time.Duration(checkpointer.LeaseDuration) * time.Millisecond).UTC()
	newLeaseTimeoutString := newLeaseTimeout.Format(time.RFC3339)
	currentCheckpoint, err := checkpointer.getItem(shard.ID)
	if err != nil {
		return err
	}

	assignedVar, assignedToOk := currentCheckpoint[LeaseOwnerKey]
	leaseVar, leaseTimeoutOk := currentCheckpoint[LeaseTimeoutKey]
	claimedVar, claimedByOk := currentCheckpoint[ClaimedByKey]

	if claimedByOk {
		if claimedBy := *claimedVar.S; claimedBy != newAssignTo {
			return ErrLeaseNotAcquired{"shard claimed by another worker(" + claimedBy + ")"}
		}
	}

	var conditionalExpression string
	var expressionAttributeValues map[string]*dynamodb.AttributeValue

	if !leaseTimeoutOk || !assignedToOk {
		conditionalExpression = "attribute_not_exists(AssignedTo)"
	} else {
		assignedTo := *assignedVar.S
		leaseTimeout := *leaseVar.S

		currentLeaseTimeout, err := time.Parse(time.RFC3339, leaseTimeout)
		if err != nil {
			return err
		}

		if time.Now().UTC().Before(currentLeaseTimeout) &&
			assignedTo != newAssignTo &&
			!claimedByOk {
			return ErrLeaseNotAcquired{"current lease timeout not yet expired"}
		}

		// checkpointer.log.Debugf("Attempting to get a lock for shard: %s, leaseTimeout: %s, assignedTo: %s", shard.ID, currentLeaseTimeout, assignedTo)
		conditionalExpression = "ShardID = :id AND AssignedTo = :assigned_to AND LeaseTimeout = :lease_timeout"
		expressionAttributeValues = map[string]*dynamodb.AttributeValue{
			":id": {
				S: aws.String(shard.ID),
			},
			":assigned_to": {
				S: aws.String(assignedTo),
			},
			":lease_timeout": {
				S: aws.String(leaseTimeout),
			},
		}
	}

	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		LeaseKeyKey: {
			S: aws.String(shard.ID),
		},
		LeaseOwnerKey: {
			S: aws.String(newAssignTo),
		},
		LeaseTimeoutKey: {
			S: aws.String(newLeaseTimeoutString),
		},
	}

	if len(shard.ParentShardId) > 0 {
		marshalledCheckpoint[ParentShardIdKey] = &dynamodb.AttributeValue{S: aws.String(shard.ParentShardId)}
	}

	if shard.Checkpoint != "" {
		marshalledCheckpoint[SequenceNumberKey] = &dynamodb.AttributeValue{
			S: aws.String(shard.Checkpoint),
		}
	}

	err = checkpointer.conditionalPut(conditionalExpression, expressionAttributeValues, marshalledCheckpoint)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return ErrLeaseNotAcquired{dynamodb.ErrCodeConditionalCheckFailedException}
			}
		}
		return err
	}

	shard.Mux.Lock()
	shard.AssignedTo = newAssignTo
	shard.LeaseTimeout = newLeaseTimeout
	shard.Mux.Unlock()

	return nil
}

// CheckpointSequence writes a checkpoint at the designated sequence ID
func (checkpointer *DynamoCheckpoint) CheckpointSequence(shard *par.ShardStatus) error {
	leaseTimeout := shard.LeaseTimeout.UTC().Format(time.RFC3339)
	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		LeaseKeyKey: {
			S: aws.String(shard.ID),
		},
		SequenceNumberKey: {
			S: aws.String(shard.Checkpoint),
		},
		LeaseOwnerKey: {
			S: aws.String(shard.AssignedTo),
		},
		LeaseTimeoutKey: {
			S: aws.String(leaseTimeout),
		},
	}

	if len(shard.ParentShardId) > 0 {
		marshalledCheckpoint[ParentShardIdKey] = &dynamodb.AttributeValue{S: &shard.ParentShardId}
	}

	return checkpointer.saveItem(marshalledCheckpoint)
}

// FetchShardStatus retrieves the details(checkpoint, assignee, claim, etc.) for the given shard
func (checkpointer *DynamoCheckpoint) FetchShardStatus(shard *par.ShardStatus) error {
	checkpoint, err := checkpointer.getItem(shard.ID)
	if err != nil {
		return err
	}

	shard.Mux.Lock()
	defer shard.Mux.Unlock()

	if assignedTo, ok := checkpoint[LeaseOwnerKey]; ok {
		shard.AssignedTo = aws.StringValue(assignedTo.S)
	}

	if leaseTimeout, ok := checkpoint[LeaseTimeoutKey]; ok {
		shard.LeaseTimeout, _ = time.Parse(time.RFC3339, aws.StringValue(leaseTimeout.S))
	}

	if claimedBy, ok := checkpoint[ClaimedByKey]; ok {
		shard.ClaimedBy = aws.StringValue(claimedBy.S)
	} else {
		shard.ClaimedBy = ""
	}

	if sequenceID, ok := checkpoint[SequenceNumberKey]; ok {
		shard.Checkpoint = aws.StringValue(sequenceID.S)
		// checkpointer.log.Debugf("Retrieved checkpoint sequence. %s", *sequenceID.S)
	} else {
		return ErrSequenceIDNotFound
	}

	return nil
}

// RemoveLeaseInfo to remove lease info for shard entry in dynamoDB because the shard no longer exists in Kinesis
func (checkpointer *DynamoCheckpoint) RemoveLeaseInfo(shardID string) error {
	err := checkpointer.removeItem(shardID)

	if err != nil {
		checkpointer.log.Errorf("Error in removing lease info for shard: %s, Error: %+v", shardID, err)
	} else {
		checkpointer.log.Infof("Lease info for shard: %s has been removed.", shardID)
	}

	return err
}

// RemoveLeaseOwner to remove lease owner for the shard entry
func (checkpointer *DynamoCheckpoint) RemoveLeaseOwner(shardID string, owner string) error {
	conditionalExpression := `AssignedTo = :assigned_to`

	expressionAttributeValues := map[string]*dynamodb.AttributeValue{
		":assigned_to": {
			S: &owner,
		},
	}

	updateExpression := "REMOVE AssignedTo"

	key := map[string]*dynamodb.AttributeValue{
		LeaseKeyKey: {
			S: aws.String(shardID),
		},
	}

	return checkpointer.conditionalUpdate(conditionalExpression, expressionAttributeValues, key, updateExpression)
}

// ReleaseShard to release a claimed shard for stealing
func (checkpointer *DynamoCheckpoint) ReleaseShard(shardID string, owner string) error {
	conditionalExpression := `AssignedTo = :assigned_to`

	shardReleased := ShardReleased
	expressionAttributeValues := map[string]*dynamodb.AttributeValue{
		":assigned_to": {
			S: &owner,
		},
		":shard_released": {
			S: &shardReleased,
		},
	}

	updateExpression := "SET AssignedTo = :shard_released"

	key := map[string]*dynamodb.AttributeValue{
		LeaseKeyKey: {
			S: aws.String(shardID),
		},
	}

	return checkpointer.conditionalUpdate(conditionalExpression, expressionAttributeValues, key, updateExpression)
}

func (checkpointer *DynamoCheckpoint) FetchActiveShardsAndWorkers() (int, map[string][]string, error) {
	items, err := checkpointer.svc.Scan(&dynamodb.ScanInput{
		TableName: aws.String(checkpointer.TableName),
	})
	if err != nil {
		return 0, nil, err
	}

	numActiveShards := 0
	workers := make(map[string][]string)
	for _, i := range items.Items {
		// Ignore closed shards, only return active shards
		if seq, ok := i[SequenceNumberKey]; ok && *seq.S == ShardEnd {
			continue
		}

		if s, ok := i[LeaseKeyKey]; !ok || s.S == nil {
			continue
		}
		shardID := *i[LeaseKeyKey].S
		numActiveShards++

		if w, ok := i[LeaseOwnerKey]; !ok || w.S == nil {
			continue
		}
		workerID := *i[LeaseOwnerKey].S

		// do not count if lease expired
		if t, ok := i[LeaseTimeoutKey]; ok && t.S != nil {
			leaseTimeout, err := time.Parse(time.RFC3339, *t.S)
			if err == nil && time.Now().UTC().After(leaseTimeout) {
				continue
			}
		}

		// Add worker to map if not already added
		if _, ok := workers[workerID]; !ok {
			workers[workerID] = []string{}
		}

		// Add shard for the worker if not claimed
		if c, ok := i[ClaimedByKey]; !ok || c.S == nil {
			workers[workerID] = append(workers[workerID], shardID)
		}
	}
	return numActiveShards, workers, nil
}

func (checkpointer *DynamoCheckpoint) ClaimShard(shardID string, fromWorkerID string, toWorkerID string) error {
	conditionalExpression := `AssignedTo = :assigned_to AND
	attribute_not_exists(ClaimedBy)`

	expressionAttributeValues := map[string]*dynamodb.AttributeValue{
		":assigned_to": {
			S: &fromWorkerID,
		},
		":claim_owner": {
			S: &toWorkerID,
		},
	}

	updateExpression := "SET ClaimedBy = :claim_owner"

	key := map[string]*dynamodb.AttributeValue{
		LeaseKeyKey: {
			S: aws.String(shardID),
		},
	}

	return checkpointer.conditionalUpdate(conditionalExpression, expressionAttributeValues, key, updateExpression)
}

func (checkpointer *DynamoCheckpoint) ClearClaim(shardID string, claimOwner string) error {
	conditionalExpression := `ClaimedBy = :claim_owner`

	expressionAttributeValues := map[string]*dynamodb.AttributeValue{
		":claim_owner": {
			S: &claimOwner,
		},
	}

	updateExpression := "REMOVE ClaimedBy"

	key := map[string]*dynamodb.AttributeValue{
		LeaseKeyKey: {
			S: aws.String(shardID),
		},
	}

	return checkpointer.conditionalUpdate(conditionalExpression, expressionAttributeValues, key, updateExpression)
}

func (checkpointer *DynamoCheckpoint) createTable() error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(LeaseKeyKey),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(LeaseKeyKey),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(checkpointer.leaseTableReadCapacity),
			WriteCapacityUnits: aws.Int64(checkpointer.leaseTableWriteCapacity),
		},
		TableName: aws.String(checkpointer.TableName),
	}
	_, err := checkpointer.svc.CreateTable(input)
	return err
}

func (checkpointer *DynamoCheckpoint) doesTableExist() bool {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(checkpointer.TableName),
	}
	_, err := checkpointer.svc.DescribeTable(input)
	return err == nil
}

func (checkpointer *DynamoCheckpoint) saveItem(item map[string]*dynamodb.AttributeValue) error {
	return checkpointer.putItem(&dynamodb.PutItemInput{
		TableName: aws.String(checkpointer.TableName),
		Item:      item,
	})
}

func (checkpointer *DynamoCheckpoint) conditionalPut(conditionExpression string, expressionAttributeValues map[string]*dynamodb.AttributeValue, item map[string]*dynamodb.AttributeValue) error {
	return checkpointer.putItem(&dynamodb.PutItemInput{
		ConditionExpression:       aws.String(conditionExpression),
		TableName:                 aws.String(checkpointer.TableName),
		Item:                      item,
		ExpressionAttributeValues: expressionAttributeValues,
	})
}

func (checkpointer *DynamoCheckpoint) conditionalUpdate(conditionExpression string, expressionAttributeValues map[string]*dynamodb.AttributeValue, key map[string]*dynamodb.AttributeValue, updateExpression string) error {
	return checkpointer.updateItem(&dynamodb.UpdateItemInput{
		ConditionExpression:       aws.String(conditionExpression),
		TableName:                 aws.String(checkpointer.TableName),
		Key:                       key,
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeValues: expressionAttributeValues,
	})
}

func (checkpointer *DynamoCheckpoint) putItem(input *dynamodb.PutItemInput) error {
	_, err := checkpointer.svc.PutItem(input)
	return err
}

func (checkpointer *DynamoCheckpoint) updateItem(input *dynamodb.UpdateItemInput) error {
	_, err := checkpointer.svc.UpdateItem(input)
	return err
}

func (checkpointer *DynamoCheckpoint) getItem(shardID string) (map[string]*dynamodb.AttributeValue, error) {
	item, err := checkpointer.svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(checkpointer.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			LeaseKeyKey: {
				S: aws.String(shardID),
			},
		},
	})
	return item.Item, err
}

func (checkpointer *DynamoCheckpoint) removeItem(shardID string) error {
	_, err := checkpointer.svc.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(checkpointer.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			LeaseKeyKey: {
				S: aws.String(shardID),
			},
		},
	})
	return err
}
