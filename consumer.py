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

buffer_lock = threading.Lock()
block_buffer = []
buffer_seq = itertools.count()
WINDOW_SIZE = 300  # Rolling window size in block numbers, Reduce this for chains like Tron with lower Block rate
max_seen_block = None


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
    """Flush buffered blocks using a rolling window on block numbers."""
    global max_seen_block

    with buffer_lock:
        if not block_buffer:
            return

        # On force, or if we don't yet know a tip, just flush everything in order.
        if force or max_seen_block is None:
            while block_buffer:
                block_number, _, block_msg = heapq.heappop(block_buffer)
                print(f"Flushing block {block_number}")
                # TODO: process block_msg here
            return

        safe_threshold = max_seen_block - WINDOW_SIZE

        remaining = []
        while block_buffer:
            block_number, seq, block_msg = heapq.heappop(block_buffer)
            # If the block is older than the rolling window, it's safe to flush.
            if isinstance(block_number, int) and block_number <= safe_threshold:
                print(f"Flushing block {block_number}")
                # TODO: process block_msg here
            else:
                remaining.append((block_number, seq, block_msg))

        # Keep only blocks that are still inside the rolling window.
        for item in remaining:
            heapq.heappush(block_buffer, item)

#The heap key is (block_number, buffer_seq), so it only ensures that lower block numbers are flushed before higher ones and 
# maintains arrival order for ties. 
def enqueue_block(block_number, block_msg):
    """Add block to shared buffer and flush based on a rolling window."""
    global max_seen_block

    sort_key = block_number if block_number is not None else float('inf')

    with buffer_lock:
        # Track the highest block number we've seen so far.
        if block_number is not None:
            if max_seen_block is None or block_number > max_seen_block:
                max_seen_block = block_number

        heapq.heappush(block_buffer, (sort_key, next(buffer_seq), block_msg))

    # Try to flush whatever is now safe to emit.
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
