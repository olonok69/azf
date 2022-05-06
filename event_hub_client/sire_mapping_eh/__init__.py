import os
from azure.eventhub import (
    EventHubProducerClient,
    EventHubConsumerClient,
    TransportType,
    EventHubSharedKeyCredential
)

from typing import List
import logging
import time
import azure.functions as func
import os
import threading
import asyncio

from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

CONNECTION_STRING = "Endpoint=sb://digital-sandbox.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
FULLY_QUALIFIED_NAMESPACE = "digital-sandbox.servicebus.windows.net"
EVENTHUB_NAME = "performancedata"
SAS_POLICY = "ListenPolicy"
SAS_KEY = ""
CONSUMER_GROUP = "$Default"

"""
def main(events: List[func.EventHubEvent]):
    create_consumer_client()
    for event in events:
        logging.info('Python EventHub trigger processed an event: %s',
                        event.get_body().decode('utf-8'))
"""    

def create_consumer_client():
    print('Examples showing how to create consumer client.')

    # Create consumer client from connection string.
    """
    consumer_client = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STRING,  # connection string contains EventHub name.
        consumer_group=CONSUMER_GROUP
    )
    
    # Illustration of commonly used parameters.
    consumer_client = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=EVENTHUB_NAME,  # EventHub name should be specified if it doesn't show up in connection string.
        logging_enable=False,  # To enable network tracing log, set logging_enable to True.
        retry_total=3,  # Retry up to 3 times to re-do failed operations.
        transport_type=TransportType.Amqp  # Use Amqp as the underlying transport protocol.
    )"""

    # Create consumer client from constructor.

    consumer_client = EventHubConsumerClient(
        fully_qualified_namespace=FULLY_QUALIFIED_NAMESPACE,
        eventhub_name=EVENTHUB_NAME,
        consumer_group=CONSUMER_GROUP,
        credential=EventHubSharedKeyCredential(
            policy=SAS_POLICY,
            key=SAS_KEY
        ),
        logging_enable=False,  # To enable network tracing log, set logging_enable to True.
        retry_total=3,  # Retry up to 3 times to re-do failed operations.
        transport_type=TransportType.Amqp  # Use Amqp as the underlying transport protocol.
    )

    print("Calling consumer client get eventhub properties:", consumer_client.get_eventhub_properties())
    return consumer_client.get_eventhub_properties()


CONNECTION_STRING = "Endpoint=sb://digital-sandbox.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=mkkmTbZ2tZMWxEw/YrUFkrymQY4IdWCIzEjc0n0FBiA="
FULLY_QUALIFIED_NAMESPACE = "digital-sandbox.servicebus.windows.net"
EVENTHUB_NAME = "performancedata"
SAS_POLICY = "ListenPolicy"
SAS_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
CONSUMER_GROUP = "$Default"
storage_connection_str = 'DefaultEndpointsProtocol=https;AccountName=pipelineperftest;AccountKey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX;EndpointSuffix=core.windows.net'
container_name = 'pipelineperftest'



async def on_event(partition_context,event):
    # do something 
    print(event.values)
    await partition_context.update_checkpoint(event)  # Or update_checkpoint every N events for better performance.

async def receive(client):
    await client.receive(
        on_event=on_event,
        starting_position="-1",  # "-1" is from the beginning of the partition.
    )

async def main(events: List[func.EventHubEvent]):
    checkpoint_store = BlobCheckpointStore.from_connection_string(storage_connection_str, container_name)
    client = EventHubConsumerClient.from_connection_string(
        CONNECTION_STRING,
        CONSUMER_GROUP,
        eventhub_name=eventhub_name,
        checkpoint_store=checkpoint_store,  # For load balancing and checkpoint. Leave None for no load balancing
    )
    async with client:
        await receive(client)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())











"""


connection=create_consumer_client()
print(connection)
print("\n")

credential = EventHubSharedKeyCredential(SAS_POLICY, SAS_KEY)
consumer = EventHubConsumerClient(
    fully_qualified_namespace=FULLY_QUALIFIED_NAMESPACE,
    eventhub_name=EVENTHUB_NAME,
    consumer_group='$Default',
    credential=credential)

print(consumer.get_eventhub_properties())
print("\n")

consumer2 = EventHubConsumerClient.from_connection_string(
    conn_str=CONNECTION_STRING,
    consumer_group="$Default",
    eventhub_name=EVENTHUB_NAME  # EventHub name should be specified if it doesn't show up in connection string.
)

logger = logging.getLogger("azure.eventhub")

def on_event(partition_context, event):
    # Put your code here.
    # If the operation is i/o intensive, multi-thread will have better performance.
    logger.info("Received event from partition: {}".format(partition_context.partition_id))

# The 'receive' method is a blocking call, it can be executed in a thread for
# non-blocking behavior, and combined with the 'close' method.

worker = threading.Thread(
    target=consumer.receive,
    kwargs={
        "on_event": on_event,
        "starting_position": "-1",  # "-1" is from the beginning of the partition.
    }
)
worker.start()
time.sleep(10)  # Keep receiving for 10s then close.
# Close down the consumer handler explicitly.
consumer.close()
"""