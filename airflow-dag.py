from datetime import datetime, timedelta
import time
import requests
import boto3
import json
from airflow.decorators import task, dag
from airflow.utils.log.logging_mixin import LoggingMixin

# Global SQS client
sqs = boto3.client("sqs", region_name = "us-east-1")

# Hard-coded constants
UVA_ID = "uup3cy"
SUBMISSION_URL = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
PLATFORM = "airflow"

# --- Define the DAG ---
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

@dag( # defining the conditions for the DAG
    dag_id="dp2_airflow_dag",
    default_args=default_args,
    description="Airflow DAG to Rearrange Quote",
    schedule=None,
    start_date=datetime(2025, 10, 31),
    catchup=False,
    tags=["dp2", "quote", "sqs"],
)
def dp2_dag():

    logger = LoggingMixin().log

    @task
    def populate_queue():
        # Post to API to populate queue with messages
        try:
            url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/uup3cy"
            response = requests.post(url)
            response.raise_for_status()

            payload = response.json()
            queue_url = payload["sqs_url"]
            logger.info(f"Queue populated successfully: {queue_url}")

            attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])
            logger.debug(f"Queue attributes: {attrs}")
            return queue_url
        except Exception as e:
            logger.error(f"Error populating queue: {e}")
            raise

    @task
    def process_queue(queue_url: str): 
        # checks the queue for messages to load and brings them in
        collected = []

        while True:
            try:
                attrs = sqs.get_queue_attributes(
                    QueueUrl=queue_url,
                    AttributeNames=[
                        "ApproximateNumberOfMessages",
                        "ApproximateNumberOfMessagesNotVisible",
                        "ApproximateNumberOfMessagesDelayed"
                    ]
                )
                # Getting the message attributes and the number of messages available etc. 
                attributes = attrs.get("Attributes", {})
                visible = int(attributes.get("ApproximateNumberOfMessages", 0))
                not_visible = int(attributes.get("ApproximateNumberOfMessagesNotVisible", 0))
                delayed = int(attributes.get("ApproximateNumberOfMessagesDelayed", 0))
                total = visible + not_visible + delayed

                logger.info(f"Queue status → visible: {visible}, not_visible: {not_visible}, delayed: {delayed}, total: {total}")

                if total == 0:
                    logger.info("Queue empty, done collecting.")
                    break

                response = sqs.receive_message( # establishing parameters to receive messages
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=5,
                    MessageAttributeNames=["All"]
                )

                messages = response.get("Messages", [])
                if not messages: # 10 seconds waited before checking again
                    logger.info("No visible messages at the moment. Waiting 10 seconds...")
                    time.sleep(10)
                    continue

                delete_entries = [] # processing and deleting messages from queue
                for msg in messages:
                    attrs = msg.get("MessageAttributes", {})
                    order_no = attrs.get("order_no", {}).get("StringValue")
                    word = attrs.get("word", {}).get("StringValue")
                    receipt_handle = msg["ReceiptHandle"]

                    if order_no and word:
                        collected.append((order_no, word))
                        logger.debug(f"Collected ({order_no}, {word})")
                    else:
                        logger.warning("Skipping malformed message")

                    delete_entries.append({
                        "Id": msg["MessageId"],
                        "ReceiptHandle": receipt_handle
                    })

                if delete_entries: # deletes the messages from queue after loading
                    sqs.delete_message_batch(QueueUrl=queue_url, Entries=delete_entries)
                    logger.info(f"Deleted {len(delete_entries)} messages.")

                time.sleep(2)

            except Exception as e:
                logger.error(f"Error processing queue: {e}")
                raise

        logger.info(f"Collected {len(collected)} messages in total.")
        return collected

    @task
    def order_messages(collected: list):
        # uses the order_no variable to order the chunks of the quote
        try:
            normalized = [] 
            for order_no, word in collected:
                try: # multiple methods to ensure type casting is succesful to int
                    key = int(order_no)
                except Exception:
                    try:
                        key = int(float(order_no))
                    except Exception:
                        key = None
                normalized.append((key, order_no, word))

            normalized.sort(key=lambda x: (x[0] is None, x[0] if x[0] is not None else 0))
            ordered_words = [w for _, _, w in normalized]
            ordered_text = " ".join(ordered_words)

            logger.info(f"Ordered text: {ordered_text}")
            return ordered_text
        except Exception as e:
            logger.error(f"Error ordering messages: {e}")
            raise

    @task
    def submit_solution(phrase: str):
        # submission to sqs with computing ID and ordered quote
        try:
            response = sqs.send_message(
                QueueUrl=SUBMISSION_URL,
                MessageBody="Airflow DAG Submission: Teagan Britten",
                MessageAttributes={
                    "uvaid": {"DataType": "String", "StringValue": UVA_ID},
                    "phrase": {"DataType": "String", "StringValue": phrase},
                    "platform": {"DataType": "String", "StringValue": PLATFORM},
                },
            )
            logger.info(f"Submitted result to SQS. Message ID: {response['MessageId']}")
            return True
        except Exception as e:
            logger.error(f"Error submitting solution: {e}")
            return False

    # define task dependencies
    queue_url = populate_queue()
    collected = process_queue(queue_url)
    ordered_text = order_messages(collected)
    submit_solution(ordered_text)

# establish the DAG
dp2_airflow_dag = dp2_dag()