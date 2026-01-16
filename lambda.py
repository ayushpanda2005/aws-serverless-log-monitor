import json
import boto3
import gzip
import base64
import re
from datetime import datetime
from decimal import Decimal
import os

dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
s3 = boto3.client('s3')

LOGS_TABLE = os.environ.get('LOGS_TABLE', 'LogAnalysis')
METRICS_TABLE = os.environ.get('METRICS_TABLE', 'LogMetrics')
ALERTS_TABLE = os.environ.get('ALERTS_TABLE', 'LogAlerts')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', 'arn:aws:sns:ap-south-1:141729833326:My-sns-topic')
S3_BUCKET = os.environ.get('S3_BUCKET', 'lambdadisable')

LOG_PATTERNS = {
    'ERROR': re.compile(r'\b(error|exception|failed|failure)\b', re.IGNORECASE),
    'WARNING': re.compile(r'\b(warning|warn|deprecated)\b', re.IGNORECASE),
    'CRITICAL': re.compile(r'\b(critical|fatal|panic|emergency)\b', re.IGNORECASE),
    'INFO': re.compile(r'\b(info|information|success)\b', re.IGNORECASE)
}

IP_PATTERN = re.compile(r'\b(?:\d{1,3}\.){3}\d{1,3}\b')
STATUS_CODE_PATTERN = re.compile(r'\b(status|code)[:\s]+(\d{3})\b', re.IGNORECASE)

def lambda_handler(event, context):
    try:
        compressed_payload = base64.b64decode(event['awslogs']['data'])
        uncompressed_payload = gzip.decompress(compressed_payload)
        log_data = json.loads(uncompressed_payload)
        
        print(f"Processing {len(log_data['logEvents'])} log events")
        
        processed_logs = []
        metrics = {'total': 0, 'error': 0, 'warning': 0, 'critical': 0, 'info': 0}
        
        for log_event in log_data['logEvents']:
            processed_log = process_log_event(log_event, log_data)
            processed_logs.append(processed_log)
            
            metrics['total'] += 1
            severity = processed_log.get('severity', 'INFO').lower()
            if severity in metrics:
                metrics[severity] += 1
        
        store_logs(processed_logs)
        update_metrics(metrics, log_data['logGroup'])
        check_alerts(processed_logs, metrics)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Success', 'processed': len(processed_logs)})
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        raise

def process_log_event(log_event, log_data):
    message = log_event['message']
    timestamp = log_event['timestamp']
    severity = determine_severity(message)
    
    return {
        'log_id': f"{log_event['id']}_{timestamp}",
        'timestamp': timestamp,
        'datetime': datetime.fromtimestamp(timestamp / 1000).isoformat(),
        'log_group': log_data['logGroup'],
        'log_stream': log_data['logStream'],
        'message': message,
        'severity': severity
    }

def determine_severity(message):
    for severity, pattern in LOG_PATTERNS.items():
        if pattern.search(message):
            return severity
    return 'INFO'

def store_logs(logs):
    table = dynamodb.Table(LOGS_TABLE)
    with table.batch_writer() as batch:
        for log in logs:
            batch.put_item(Item=log)
    print(f"Stored {len(logs)} logs")

def update_metrics(metrics, log_group):
    table = dynamodb.Table(METRICS_TABLE)
    timestamp = int(datetime.now().timestamp())
    interval_timestamp = timestamp - (timestamp % 300)
    
    metric_item = {
        'metric_id': f"{log_group}_{interval_timestamp}",
        'log_group': log_group,
        'timestamp': interval_timestamp,
        'total_logs': metrics['total'],
        'error_count': metrics['error'],
        'warning_count': metrics['warning'],
        'critical_count': metrics['critical']
    }
    table.put_item(Item=metric_item)

def check_alerts(logs, metrics):
    if metrics['error'] > 10 or metrics['critical'] > 0:
        alert_message = f"Alert! Errors: {metrics['error']}, Critical: {metrics['critical']}"
        print(alert_message)
        
        if SNS_TOPIC_ARN:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject="Log Analysis Alert",
                Message=alert_message
            )
        
        table = dynamodb.Table(ALERTS_TABLE)
        table.put_item(Item={
            'alert_id': f"alert_{int(datetime.now().timestamp())}",
            'timestamp': int(datetime.now().timestamp()),
            'message': alert_message,
            'severity': 'CRITICAL' if metrics['critical'] > 0 else 'WARNING'
        })
