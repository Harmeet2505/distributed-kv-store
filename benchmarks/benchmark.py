import socket
import time
import threading

def send_command(sock, cmd):
    sock.sendall((cmd + "\n").encode())
    return sock.recv(1024)

def benchmark(host, port, command_fn, num_requests, num_threads):
    results = []
    lock = threading.Lock()

    def worker(thread_id, n):
        sock = socket.create_connection((host, port))
        local_times = []
        for i in range(n):
            global_i = thread_id * n + i   # unique key per thread
            start = time.perf_counter()
            send_command(sock, command_fn(global_i))
            local_times.append(time.perf_counter() - start)
        sock.close()
        with lock:
            results.extend(local_times)

    per_thread = num_requests // num_threads
    threads = [threading.Thread(target=worker, args=(t, per_thread)) for t in range(num_threads)]

    start = time.perf_counter()
    for t in threads: t.start()
    for t in threads: t.join()
    total_time = time.perf_counter() - start

    total = len(results)
    throughput = total / total_time
    avg_latency_ms = (sum(results) / total) * 1000
    p99_latency_ms = sorted(results)[int(total * 0.99)] * 1000

    print(f"  Requests: {total}, Time: {total_time:.2f}s")
    print(f"  Throughput: {throughput:.0f} ops/sec")
    print(f"  Avg latency: {avg_latency_ms:.2f}ms, p99 latency: {p99_latency_ms:.2f}ms")

if __name__ == "__main__":
    HOST, PORT = "127.0.0.1", 9092  # point at your leader's client port
    N, THREADS = 5000, 10

    print("SET benchmark:")
    benchmark(HOST, PORT, lambda i: f"SET key{i} value{i}", N, THREADS)

    print("\nGET benchmark:")
    benchmark(HOST, PORT, lambda i: f"GET key{i % 1000}", N, THREADS)
    