import requests
import boto3
from prefect import flow, task, get_run_logger
import os
import time
import json

# Initialize SQS client globally
sqs = boto3.client("sqs")

@task
def populate_queue():
    """Posts to the scatter API to populate the SQS queue and returns the queue URL."""
    logger = get_run_logger()
    UVA_ID = "uup3cy" # Hard-coded
    try:
        url = f"https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/{UVA_ID}"
        response = requests.post(url)
        response.raise_for_status() # Raise an exception for bad status codes
        payload = response.json()
        queue_url = payload["sqs_url"]

        logger.info(f"Queue populated successfully")
        logger.info(f"SQS URL: {queue_url}")
        
        # Get and log queue attributes
        attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])
        logger.debug(f"Queue attributes: {attrs}")
        
        return queue_url
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP error populating queue: {e}")
        raise
    except Exception as e:
        logger.error(f"Error populating queue: {e}")
        raise

@task
def process_queue(queue_url: str):
    """
    Polls the SQS queue, collects order_no and word attributes from messages,
    deletes the messages, and returns the collected data.
    """
    logger = get_run_logger()
    collected = []
    # Using the global sqs client initialized at the top

    while True:
        try:
            # --- Monitor the queue ---
            attrs = sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=[
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesNotVisible",
                    "ApproximateNumberOfMessagesDelayed"
                ]
            )

            # Extract counts
            attributes = attrs.get("Attributes", {})
            visible = int(attributes.get("ApproximateNumberOfMessages", 0))
            not_visible = int(attributes.get("ApproximateNumberOfMessagesNotVisible", 0))
            delayed = int(attributes.get("ApproximateNumberOfMessagesDelayed", 0))
            total = visible + not_visible + delayed

            logger.info(f"Queue status → visible: {visible}, not_visible: {not_visible}, delayed: {delayed}, total: {total}")

            # Exit loop if nothing left in queue
            if total == 0:
                logger.info("No messages left in the queue. Done collecting.")
                break

            # --- Try to receive messages ---
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10, 
                WaitTimeSeconds=5,
                MessageAttributeNames=["All"]
            )

            messages = response.get("Messages", [])
            if not messages:
                logger.info("No visible messages at the moment. Waiting 10 seconds...")
                time.sleep(10)
                continue

            # --- Process and delete messages ---
            delete_entries = []
            for msg in messages:
                attrs = msg.get("MessageAttributes", {})
                order_no = attrs.get("order_no", {}).get("StringValue")
                word = attrs.get("word", {}).get("StringValue")
                receipt_handle = msg["ReceiptHandle"]

                if order_no and word:
                    collected.append((order_no, word))
                    logger.debug(f"Collected ({order_no}, {word})")
                else:
                    logger.warning("Message missing expected attributes; skipping and deleting.")
                
                # Add to batch delete list
                delete_entries.append({
                    'Id': msg["MessageId"], 
                    'ReceiptHandle': receipt_handle
                })

            if delete_entries:
                # Batch delete messages
                sqs.delete_message_batch(
                    QueueUrl=queue_url,
                    Entries=delete_entries
                )
                logger.info(f"Deleted {len(delete_entries)} messages.")

            time.sleep(2)

        except Exception as e:
            logger.error(f"Error processing queue: {e}")
            raise e

    logger.info(f"Collected {len(collected)} messages in total.")
    return collected

@task
def order_messages(collected: list, destination_url: str = None):
    """Sorts collected messages and optionally posts the ordered text."""
    logger = get_run_logger()
    try:
        # Normalize and sort collected by numeric order
        normalized = []
        for order_no, word in collected:
            key = None
            try:
                key = int(order_no)
            except Exception:
                try:
                    key = int(float(order_no))
                except Exception:
                    pass
            normalized.append((key, order_no, word))

        # Sort: None (non-numeric) at the end, numeric by key
        normalized.sort(key=lambda x: (x[0] is None, x[0] if x[0] is not None else 0))

        # Build ordered list of words
        ordered_words = [w for _, _, w in normalized]
        ordered_text = " ".join(ordered_words)
        logger.info(f"Ordered text: {ordered_text}")
        return ordered_text

    except Exception as e:
        logger.error(f"Error in order_messages: {e}")
        raise

@task
def submit_solution(phrase: str):
    """Submits the final phrase to the submission SQS queue."""
    submission_url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    platform = "prefect"
    UVA_ID = "uup3cy" # Hard-coded
    logger = get_run_logger()
    
    try:
        response = sqs.send_message(
            QueueUrl=submission_url,
            MessageBody=f"Submission for {UVA_ID}",
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': UVA_ID # Hard-coded
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        logger.info(f"Successfully submitted result to SQS. Message ID: {response['MessageId']}")
        return True

    except Exception as e:
        logger.error(f"Error submitting solution to SQS: {e}")
        return False

@flow(log_prints=True)
def puzzle_solver_flow(submit_answer: bool = True):
    """Orchestrates the process of populating, processing, ordering, and submitting the puzzle."""
    
    queue_url = populate_queue()
    collected_messages = process_queue(queue_url) 
    ordered_text = order_messages(collected_messages, destination_url=None) 
    if submit_answer:
        submit_solution(ordered_text)

if __name__ == "__main__":
    puzzle_solver_flow(submit_answer=True)