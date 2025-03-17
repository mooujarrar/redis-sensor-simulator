import redis
import time
import threading
from concurrent.futures import ThreadPoolExecutor

# Redis connection
redis_client = redis.StrictRedis(host='127.0.0.1', port=6379, decode_responses=True)

# Total number of sensors
NUM_SENSORS = 2000

def sensor_task(sensor_id):
    """
    Simulates a sensor publishing data to its stream.
    """
    stream_name = f"sensor:{sensor_id}"
    while True:
        # Add a timestamp to the data payload
        timestamp = time.time()  # Current time in seconds (float)
        data = {
            "temperature": str(20 + sensor_id % 10),
            "humidity": str(50 + sensor_id % 5),
            "timestamp": str(timestamp)  # Include timestamp in payload
        }
        redis_client.xadd(stream_name, data)
        time.sleep(0.1)  # Simulate a delay between updates

def consumer_task():
    """
    Consumes data from all sensor streams in real time and calculates latency.
    """
    streams = {f"sensor:{i}": '0' for i in range(NUM_SENSORS)}  # Start reading from the beginning of each stream
    while True:
        try:
            # Read new messages from all streams
            messages = redis_client.xread(streams, block=1000)  # Block for up to 1 second
            for stream, entries in messages:
                for entry_id, data in entries:
                    # Extract the original timestamp and calculate latency
                    original_timestamp = float(data["timestamp"])
                    current_time = time.time()
                    latency = current_time - original_timestamp
                    
                    # Log the latency
                    print(f"Stream: {stream}, ID: {entry_id}, Latency: {latency:.6f} seconds, Data: {data}")
                    
                    # Update the last ID consumed
                    streams[stream] = entry_id
        except Exception as e:
            print(f"Error in consumer: {e}")

if __name__ == "__main__":
    try:
        # Start consumer in a separate thread
        consumer_thread = threading.Thread(target=consumer_task)
        consumer_thread.start()  # Ensure the consumer thread starts correctly

        # Start sensor simulation
        with ThreadPoolExecutor(max_workers=NUM_SENSORS) as executor:
            for sensor_id in range(NUM_SENSORS):
                executor.submit(sensor_task, sensor_id)
        
        # Keep the main thread alive to allow threads to run
        consumer_thread.join()  # Block main thread until consumer finishes

    except KeyboardInterrupt:
        print("Shutting down...")
