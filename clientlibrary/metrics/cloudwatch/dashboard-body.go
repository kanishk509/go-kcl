package cloudwatch

import "strings"

const DASHBOARD_TEMPLATE = `
{
    "widgets": [
        {
            "height": 6,
            "width": 12,
            "y": 1,
            "x": 12,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ { "expression": "FILL(SEARCH('{\"$CLOUDWATCH_NAMESPACE\", KinesisStreamName, WorkerID} MetricName=\"NumLeasesHeld\"', 'Average', 60), 0)", "label": "num_leases", "id": "e1", "region": "us-west-2" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "us-west-2",
                "stat": "Average",
                "period": 300,
                "title": "Number of leases held by workers",
                "yAxis": {
                    "left": {
                        "label": "Count of leases held",
                        "showUnits": false
                    }
                },
                "legend": {
                    "position": "hidden"
                }
            }
        },
        {
            "height": 6,
            "width": 12,
            "y": 13,
            "x": 0,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ { "expression": "SEARCH('{\"$CLOUDWATCH_NAMESPACE\", KinesisStreamName, WorkerID} MetricName=\"MillisBehindLatest\"', 'Average', 60)", "label": "i_age", "id": "e1", "region": "us-west-2" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "title": "Iterator age",
                "region": "us-west-2",
                "stat": "Average",
                "period": 300,
                "yAxis": {
                    "left": {
                        "label": "Milliseconds behind latest",
                        "showUnits": false
                    }
                },
                "legend": {
                    "position": "hidden"
                }
            }
        },
        {
            "height": 1,
            "width": 24,
            "y": 0,
            "x": 0,
            "type": "text",
            "properties": {
                "markdown": "\n# Per Worker Metrics\n"
            }
        },
        {
            "height": 6,
            "width": 12,
            "y": 7,
            "x": 0,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ { "expression": "SEARCH('{\"$CLOUDWATCH_NAMESPACE\", KinesisStreamName, WorkerID} MetricName=\"RecordsProcessed\"', 'Sum', 60)", "label": "records_processed", "id": "e1" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "title": "Records processed",
                "region": "us-west-2",
                "stat": "Average",
                "period": 300,
                "yAxis": {
                    "left": {
                        "label": "Records processed per minute",
                        "showUnits": false
                    }
                },
                "annotations": {
                    "horizontal": [
                        {
                            "visible": false,
                            "label": "Untitled annotation",
                            "value": 0
                        }
                    ]
                },
                "legend": {
                    "position": "hidden"
                }
            }
        },
        {
            "height": 6,
            "width": 12,
            "y": 7,
            "x": 12,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ { "expression": "SEARCH('{\"$CLOUDWATCH_NAMESPACE\", KinesisStreamName, WorkerID} MetricName=\"DataBytesProcessed\"', 'Sum', 60)", "label": "bytes", "id": "e1" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "title": "Bytes processed",
                "region": "us-west-2",
                "stat": "Average",
                "period": 300,
                "yAxis": {
                    "left": {
                        "showUnits": false,
                        "label": "Bytes processed per minute"
                    }
                },
                "legend": {
                    "position": "hidden"
                }
            }
        },
        {
            "height": 1,
            "width": 24,
            "y": 19,
            "x": 0,
            "type": "text",
            "properties": {
                "markdown": "\n# Per Shard Metrics\n"
            }
        },
        {
            "height": 6,
            "width": 12,
            "y": 33,
            "x": 0,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ { "expression": "SUM(SEARCH('{\"$CLOUDWATCH_NAMESPACE\", KinesisStreamName, Shard} MetricName=\"RecordsProcessed\"', 'Sum', 60))", "label": "records", "id": "e1", "region": "us-west-2" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "us-west-2",
                "stat": "Average",
                "period": 300,
                "title": "Records processed by consumer",
                "yAxis": {
                    "left": {
                        "label": "Records processed per minute",
                        "showUnits": false
                    }
                },
                "legend": {
                    "position": "hidden"
                }
            }
        },
        {
            "height": 6,
            "width": 12,
            "y": 33,
            "x": 12,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ { "expression": "SUM(SEARCH('{\"$CLOUDWATCH_NAMESPACE\", KinesisStreamName, Shard} MetricName=\"DataBytesProcessed\"', 'Sum', 60))", "label": "bytes", "id": "e1", "region": "us-west-2" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "title": "Bytes processed by consumer",
                "region": "us-west-2",
                "stat": "Average",
                "period": 300,
                "yAxis": {
                    "left": {
                        "showUnits": false,
                        "label": "Bytes processed per minute"
                    }
                },
                "legend": {
                    "position": "hidden"
                }
            }
        },
        {
            "height": 6,
            "width": 12,
            "y": 39,
            "x": 0,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ { "expression": "MAX(SEARCH('{\"$CLOUDWATCH_NAMESPACE\", KinesisStreamName, Shard} MetricName=\"MillisBehindLatest\"', 'Average', 60))", "label": "i_age_max", "id": "e1", "region": "us-west-2" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "title": "Iterator age (Maximum among all shards)",
                "region": "us-west-2",
                "stat": "Average",
                "period": 300,
                "yAxis": {
                    "left": {
                        "showUnits": false,
                        "label": "Milliseconds behind latest"
                    }
                },
                "legend": {
                    "position": "hidden"
                }
            }
        },
        {
            "height": 6,
            "width": 12,
            "y": 1,
            "x": 0,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ { "expression": "SUM(FILL(SEARCH('{\"$CLOUDWATCH_NAMESPACE\", KinesisStreamName, WorkerID} MetricName=\"WorkerHeartbeat\"', 'Average', 60), 0))", "label": "active_workers", "id": "e1", "region": "us-west-2" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "us-west-2",
                "stat": "Average",
                "period": 300,
                "legend": {
                    "position": "hidden"
                },
                "yAxis": {
                    "left": {
                        "label": "Count of active workers",
                        "showUnits": false,
                        "min": 0
                    }
                },
                "title": "Active workers"
            }
        },
        {
            "height": 6,
            "width": 12,
            "y": 20,
            "x": 0,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ { "expression": "SEARCH('{\"$CLOUDWATCH_NAMESPACE\", KinesisStreamName, Shard} MetricName=\"RecordsProcessed\"', 'Sum', 60)", "label": "records", "id": "e1" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "us-west-2",
                "stat": "Average",
                "period": 300,
                "title": "Records processed per shard",
                "yAxis": {
                    "left": {
                        "label": "Records processed per minute",
                        "showUnits": false
                    }
                }
            }
        },
        {
            "height": 6,
            "width": 12,
            "y": 20,
            "x": 12,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ { "expression": "SEARCH('{\"$CLOUDWATCH_NAMESPACE\", KinesisStreamName, Shard} MetricName=\"DataBytesProcessed\"', 'Sum', 60)", "label": "bytes", "id": "e1" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "title": "Bytes processed per shard",
                "region": "us-west-2",
                "stat": "Average",
                "period": 300,
                "yAxis": {
                    "left": {
                        "showUnits": false,
                        "label": "Bytes processed per minute"
                    }
                }
            }
        },
        {
            "height": 1,
            "width": 24,
            "y": 32,
            "x": 0,
            "type": "text",
            "properties": {
                "markdown": "\n# Overall consumer metrics\n"
            }
        },
        {
            "height": 6,
            "width": 12,
            "y": 26,
            "x": 0,
            "type": "metric",
            "properties": {
                "metrics": [
                    [ { "expression": "SEARCH('{\"$CLOUDWATCH_NAMESPACE\", KinesisStreamName, Shard} MetricName=\"MillisBehindLatest\"', 'Average', 60)", "label": "i_age", "id": "e1" } ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "title": "Iterator age",
                "region": "us-west-2",
                "stat": "Average",
                "period": 300,
                "yAxis": {
                    "left": {
                        "showUnits": false,
                        "label": "Milliseconds behind latest"
                    }
                }
            }
        }
    ]
}
`

func getDashboardBody(cloudwatchNamespace string) string {
	dashboardBody := strings.Replace(DASHBOARD_TEMPLATE, "$CLOUDWATCH_NAMESPACE", cloudwatchNamespace, -1)
	return dashboardBody
}
