import uuid
import base58
import threading
import time
import heapq
import itertools
from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.message import DecodeError
from tron import parsed_abi_block_message_pb2
from solana import parsed_idl_block_message_pb2
import config

# Kafka consumer configuration
group_id_suffix = uuid.uuid4().hex
base_conf = {
    'bootstrap.servers': 'rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092',
    'group.id': f'{config.solana_username}-group-{group_id_suffix}',
    'session.timeout.ms': 30000,
    'security.protocol': 'SASL_PLAINTEXT',
    'ssl.endpoint.identification.algorithm': 'none',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': config.solana_username,
    'sasl.password': config.solana_password,
    'auto.offset.reset': 'latest',
}

topic = 'solana.transactions.proto'
NUM_CONSUMERS = 6  # One consumer per partition

BUFFER_CAPACITY = 300   # Reduce this for chains like Tron with lower Block rate
buffer_lock = threading.Lock()
block_buffer = []
buffer_seq = itertools.count()


import binascii


def extract_block_number(header):
    """Return block number as int, or None if unavailable."""
    number = getattr(header, 'Slot', None)
    if isinstance(number, bytes):
        return int.from_bytes(number, byteorder='big')
    try:
        return int(number) if number is not None else None
    except (TypeError, ValueError):
        return None


def decode_message(msg):
    try:
        block_msg = parsed_idl_block_message_pb2.ParsedIdlBlockMessage()
        block_msg.ParseFromString(msg.value())
        block_number = extract_block_number(block_msg.Header)
        return block_number, block_msg
    except DecodeError as e:
        print(f"Protobuf decoding error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    return None

def flush_buffer(force=False):
    """Flush buffered blocks respecting ordering."""
    with buffer_lock:
        if force:
            while block_buffer:
                _, _, block_msg = heapq.heappop(block_buffer) # heappop removes and returns the smallest element from the heap
                
            return

        if len(block_buffer) >= BUFFER_CAPACITY:
            batch = []
            while block_buffer:
                sort_key, buffer_index, block_msg = heapq.heappop(block_buffer)
                batch.append((sort_key, buffer_index, block_msg))
                print(f"Flushing block {sort_key}")

#The heap key is (block_number, buffer_seq), so it only ensures that lower block numbers are flushed before higher ones and 
# maintains arrival order for ties. 
def enqueue_block(block_number, block_msg):
    """Add block to shared buffer and flush when capacity is reached."""
    sort_key = block_number if block_number is not None else float('inf')
    with buffer_lock:
        heapq.heappush(block_buffer, (sort_key, next(buffer_seq), block_msg))
    flush_buffer(force=False)


# --- Multi-consumer setup --- #

def consumer_worker(consumer_id):
    """Worker function for each consumer thread"""
    # Create a unique consumer for this thread
    conf = base_conf.copy()
    conf['group.id'] = f'{config.solana_username}-group-{group_id_suffix}'
    
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    
    print(f"Consumer {consumer_id} started with group_id {conf['group.id']}")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # ignore end-of-partition notifications
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            timestamp_type, timestamp = msg.timestamp()
            decoded = decode_message(msg)
            if decoded:
                block_number, block_msg = decoded
                enqueue_block(block_number, block_msg)
            
    except KeyboardInterrupt:
        print(f"Consumer {consumer_id} stopping...")
    except Exception as e:
        print(f"Consumer {consumer_id} error: {e}")
    finally:
        consumer.close()
        print(f"Consumer {consumer_id} closed")

# Start multiple consumer threads
consumers = []
for i in range(NUM_CONSUMERS):
    thread = threading.Thread(target=consumer_worker, args=(i,))
    thread.daemon = True
    consumers.append(thread)
    thread.start()

try:
    # Keep the main thread alive
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping all consumers...")
    # Wait for all consumer threads to finish
    for thread in consumers:
        thread.join(timeout=5)
    flush_buffer(force=True)
