from collections import defaultdict
import time
import multiprocessing
import cProfile

CPU_COUNT = multiprocessing.cpu_count()

def process_data(data: tuple, value: int) -> tuple:
    if data:
        min_value, max_value, total, count = data
    else:
        min_value = max_value = total = value
        count = 1

    if value < min_value:
        min_value = value
    if value > max_value:
        max_value = value
    total += value
    count += 1

    return min_value, max_value, total, count


def read_chunks(file_path: str, chunk_size: int):
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk

def process_chunk(chunk):
    lines = chunk.split(b'\n')
    result = defaultdict(tuple)
    for line in lines:
        try:
            key, value = line.split(b';')
            result[key] = process_data(result.get(key, tuple()), float(value))
        except ValueError:
            pass
    return result


def read_and_process_chunk(file_path, chunk_size):
    pool = multiprocessing.Pool(processes=CPU_COUNT)
    results = []
    for chunk in read_chunks(file_path, chunk_size):
        results.append(
                    pool.apply_async(
                        process_chunk, (chunk,)
                    )
                )
    return results


file_path = 'measurements.txt'
chunk_size = 100_000_000 // CPU_COUNT
t = time.time()

# pr = cProfile.Profile()
# pr.enable()
result = read_and_process_chunk(file_path, chunk_size)

def merge_results(results):
    result = defaultdict(tuple)
    for r in results:
        for key, value in r.get().items():
            result[key] = process_data(result[key], value[1])
    return result

result = merge_results(result)

for key, value in result.items():
    result[key] = (value[0], value[2] / value[3], value[1])

# pr.disable()
# pr.print_stats()

# https://github.com/booty/ruby-1-billion/blob/main/chunks-mmap.py

t_end = time.time() - t
print(f'Elapsed time: {t_end} seconds')
print(f"Sample of result : {list(result.items())[:5]} and len of result: {len(result)}")


# 115.5 seconds for started 
# 96 seconds one if statement and prcess average in the end
# 80 seconds custom min max function 
# 76 seconds using bytes for file read and remove strip() function
# 76 seconds using chunk of data 
# 70 seconds using asyncio
# 60 seconds changing process_data to extract min, max, total, count
# 24 seconds using multiprocessing
