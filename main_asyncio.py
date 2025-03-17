import asyncio
import redis.asyncio as redis
import time

# Total number of sensors
NUM_SENSORS = 2000

# Create a connection pool
async def create_redis_pool():
    # Configure a connection pool
    pool = redis.ConnectionPool.from_url("redis://127.0.0.1:6379")
    redis_client = redis.StrictRedis(connection_pool=pool)
    return redis_client

async def sensor_task(redis_client, sensor_id):
    """
    Simulates a sensor publishing data to its stream.
    """
    stream_name = f"sensor:{sensor_id}"
    while True:
        # Add a timestamp to the data payload
        timestamp = time.time()
        data = {
            "temperature": str(20 + sensor_id % 10),
            "humidity": str(50 + sensor_id % 5),
            "timestamp": str(timestamp)  # Include timestamp in payload
        }
        await redis_client.xadd(stream_name, data)
        await asyncio.sleep(0.1)  # Simulate a delay between updates

async def consumer_task(redis_client):
    """
    Consumes data from all sensor streams in real time and calculates latency.
    """
    streams = {f"sensor:{i}": '0' for i in range(NUM_SENSORS)}  # Start reading from the beginning of each stream
    while True:
        try:
            # Read new messages from all streams
            messages = await redis_client.xread(streams, block=1000)  # Block for up to 1 second
            for stream, entries in messages:
                for entry_id, data in entries:
                    # Decode key and value (data is a byte-string dictionary)
                    decoded_data = {key.decode("utf-8"): value.decode("utf-8") for key, value in data.items()}
                    
                    # Extract the original timestamp and calculate latency
                    original_timestamp = float(decoded_data["timestamp"])
                    current_time = time.time()
                    latency = current_time - original_timestamp
                    
                    # Log the latency
                    print(f"Stream: {stream}, ID: {entry_id}, Latency: {latency:.6f} seconds, Data: {decoded_data}")
                    
                    # Update the last ID consumed
                    streams[stream] = entry_id
        except Exception as e:
            print(f"Error in consumer: {e}")

async def main():
    # Create a Redis client with asyncio support
    redis_client = await create_redis_pool()  # Create the connection pool

    try:
        # Launch all sensor tasks
        sensor_tasks = [
            asyncio.create_task(sensor_task(redis_client, sensor_id))
            for sensor_id in range(NUM_SENSORS)
        ]
        
        # Launch the consumer task
        consumer = asyncio.create_task(consumer_task(redis_client))
        
        # Wait for all tasks to complete (they won't, so this effectively blocks)
        await asyncio.gather(*sensor_tasks, consumer)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        await redis_client.close()  # Close the Redis connection

if __name__ == "__main__":
    asyncio.run(main())
