import os
import numpy as np
import redis
import concurrent.futures
import time

# Configure Redis connection
redis_host = 'localhost'
redis_port = 6379
r = redis.Redis(host=redis_host, port=redis_port, db=0)

def process_file_redis(file_path):
    matrices = []
    try:
        with open(file_path, 'r') as f:
            lines = f.readlines()

        for line in lines:
            try:
                dimensions, data_str = line.strip().split(':')
                rows, cols = map(int, dimensions.split('x'))
                data = list(map(int, data_str.split()))
                if len(data) != rows * cols:
                    raise ValueError(f"Not enough data for matrix dimensions {rows}x{cols}")
                matrix = np.array(data).reshape(rows, cols)
                matrices.append(matrix)
            except ValueError as e:
                print(f"Error processing line: {line.strip()} - {e}")

    except FileNotFoundError as e:
        print(f"File not found: {file_path} - {e}")
    except Exception as e:
        print(f"Unexpected error occurred: {e}")

    for i, matrix in enumerate(matrices):
        matrix_name = f"{file_path}_{i}"
        matrix_serialized = matrix.tobytes()
        r.set(matrix_name, matrix_serialized)
        r.set(f"{matrix_name}_shape", f"{matrix.shape[0]}x{matrix.shape[1]}")

    return matrices

def main(input_directory):
    files = [os.path.join(input_directory, f) for f in os.listdir(input_directory) if os.path.isfile(os.path.join(input_directory, f))]

    max_workers = min(4, os.cpu_count() or 1)

    # Redis Cache Processing
    start_redis = time.time()
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures_redis = {executor.submit(process_file_redis, file): file for file in files}
        results_redis = []
        for future in concurrent.futures.as_completed(futures_redis, timeout=600):  # 10-minute timeout
            file = futures_redis[future]
            try:
                matrices = future.result()
                results_redis.append(matrices)
            except Exception as e:
                print(f"Error processing file {file}: {e}")

    end_redis = time.time()
    redis_time = end_redis - start_redis
    print(f"Redis cache processing time: {redis_time:.2f} seconds")

    with open('task-mat-redis-cache-parallel-testing.txt', 'w') as f:
        f.write(f"Redis cache processing time: {redis_time:.2f} seconds\n")

if __name__ == "__main__":
    main('/home/andrei/input')

