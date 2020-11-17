import socket
import hashlib
from pathlib import Path
import os
import sys
import threading
from heapq import heappush, heappop
from math import ceil
import time
import csv
import numpy as np
import matplotlib.pyplot as plt

if len(sys.argv) != 2:
    print('Usage: Pass input file containing hosts and #connections as an argument')
    print('e.g. python3 client.py input.txt')
    sys.exit()

Chunk_size = 100000 # bytes
socket.setdefaulttimeout(5) # timeout, in s
output_path = 'output.txt' # where file is to be saved

def parse_input():
    input_path = sys.argv[1]
    f = open(input_path, 'r')
    lines = f.readlines()
    Server_hosts, Target_paths, N_connections = [], [], []
    for line in lines:
        path, nconn = line.split(',')
        path, nconn = path.strip(), nconn.strip()
        N_connections.append(int(nconn))
        
        # Trim http:// or https://
        if len(path)>6 and path[:7]=='http://':
            path = path[7:]
        elif len(path)>7 and path[:8]=='https://':
            path = path[8:]
        
        host = path.split('/')[0]
        file_path = path[len(host):]
        Server_hosts.append(host)
        Target_paths.append(file_path)
    return Server_hosts, Target_paths, N_connections


if not os.path.exists(output_path):
    Path(output_path).mkdir(parents=True, exist_ok=True)
f = open(output_path, 'wb')

Server_hosts, Target_paths, N_connections = parse_input()

Threads = []

Bytes_downloaded = [0 for i in range(sum(N_connections))]
Bytes_record = [[] for i in range(sum(N_connections))]
Time_record = [[] for i in range(sum(N_connections))]
start_time = time.time()

class DataQueue:
    def __init__(self):
        self.pq = []
        self.lowest_unwritten = 0
        self.lock = threading.Lock()

    def push(self, chunk_no, data):
        self.lock.acquire()
        heappush(self.pq, (chunk_no, data))
        while len(self.pq)>0 and self.pq[0][0] == self.lowest_unwritten:
            chunk_no, data = heappop(self.pq)
            f.write(data)
            print(f'Written chunk {chunk_no} to disk successfully')
            self.lowest_unwritten = self.lowest_unwritten + 1
        self.lock.release()

class TrackChunks:
    def __init__(self, content_length):
        self.n_chunks = ceil(content_length / Chunk_size)
        self.lowest_unassigned_chunk = 0
        self.lock = threading.Lock()

    def get_chunk(self):
        if self.lowest_unassigned_chunk < self.n_chunks:
            self.lock.acquire()
            chunk_no = self.lowest_unassigned_chunk
            self.lowest_unassigned_chunk = self.lowest_unassigned_chunk + 1
            self.lock.release()
            return chunk_no
        return -1

def md5checksum(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def split_header(data):
    i = 0
    while i+3 < len(data):
        if data[i:i+4] == b'\r\n\r\n':
            header = data[0:i+4]
            content = b''
            if i+4 < len(data):
                content = data[i+4:]
            return header, content
        i = i+1
    return b'',data

def check_ok(header):
    lines = header.splitlines()
    status = len(lines)>0 and (lines[0] == b'HTTP/1.1 200 OK' or lines[0]==b'HTTP/1.1 206 Partial Content') 
    return status

def get_content_length(header):
    lines = header.splitlines()
    for line in lines:
        words = line.split()
        if len(words)>0 and words[0] == b'Content-Range:':
            return int(line.split(b'/')[1])
    return -1

def get_chunk_size(header):
    lines = header.splitlines()
    for line in lines:
        words = line.split()
        if len(words)>0 and words[0] == b'Content-Length:':
            return int(words[1])
    return -1

def create_request(bytes_start, bytes_end, server_name, target_path, keep_alive=True):
    alive_message = ''
    if keep_alive:
        alive_message = 'Connection: keep-alive\r\n'
    req = f'GET {target_path} HTTP/1.1\r\nHost: {server_name}\r\n{alive_message}Range: bytes={bytes_start}-{bytes_end}\r\n\r\n'
    return req.encode()

# Get content-length
content_length = -1
while content_length < 0:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((Server_hosts[0], 80))
        s.sendall(create_request(0,0,Server_hosts[0],Target_paths[0],False))
        data = s.recv(4096)
        header, content = split_header(data)
        if check_ok(header):
            content_length = get_content_length(header)
    except Exception as e:
        print('Error in initial request (to obtain content length):', e)
        time.sleep(1)
print(f'Content_length: {content_length} bytes')

def socket_task(server_name, target_path, s, s_id, tracker, dataqueue):
    while True:
        chunk_no = tracker.get_chunk()
        if chunk_no < 0:
            try:
                s.close()
                print(f'Socket {s_id} disconnected as no more chunks left')
            except:
                print(f'Socket {s_id} not connected. Exiting as no more chunks left')
            break
        bytes_start = chunk_no * Chunk_size
        bytes_end = bytes_start + Chunk_size - 1
        chunk_content = b''
        while True:
            try:
                try:
                    s.sendall(create_request(bytes_start, bytes_end, server_name, target_path))
                except:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((server_name, 80))
                    print(f'Socket {s_id} connected to {server_name}, port 80')
                    s.sendall(create_request(bytes_start, bytes_end, server_name, target_path))
                print(f'Socket {s_id} sent a GET request for chunk {chunk_no}')
                
                data = s.recv(4096)
                
                header, content = split_header(data)
                if not check_ok(header):
                    print(f'Response on socket {s_id} not OK')
                    s.close()
                    continue
                chunk_s = get_chunk_size(header)
                if chunk_s < 0:
                    print(f'Unable to parse chunk size on socket {s_id}')
                    s.close()
                    continue

                current_length = len(content)
                
                chunk_content = chunk_content + content
                bytes_start = bytes_start + current_length

                Bytes_downloaded[s_id] += current_length
                Bytes_record[s_id].append(Bytes_downloaded[s_id])
                Time_record[s_id].append(time.time() - start_time)

                while current_length < chunk_s:
                    data = s.recv(4096)
                    if not data:
                        print(f'Chunk on socket {s_id} not received fully. Will request for chunk again.')
                        break
                    current_length = current_length + len(data)
                    chunk_content = chunk_content + data
                    bytes_start = bytes_start + len(data)

                    Bytes_downloaded[s_id] += len(data)
                    Bytes_record[s_id].append(Bytes_downloaded[s_id])
                    Time_record[s_id].append(time.time() - start_time)

                if current_length >= chunk_s:
                    print(f'Received chunk {chunk_no} on socket {s_id} fully, and will be written to disk shortly.')
                    x = threading.Thread(target=dataqueue.push, args=(chunk_no, chunk_content))
                    x.start()
                    Threads.append(x)
                    break
            except Exception as e:
                print(f'Error on socket {s_id} in receiving chunk {chunk_no}: {e}')
                s.close()
                time.sleep(2)

tracker = TrackChunks(content_length)
dataqueue = DataQueue()
Sockets = []

start_time = time.time()

for j in range(len(Server_hosts)):
    for i in range(N_connections[j]):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        Sockets.append(s)
        x = threading.Thread(target=socket_task, args=(Server_hosts[j], Target_paths[j], s, len(Sockets)-1, tracker, dataqueue))
        x.start()
        Threads.append(x)

for x in Threads:
    x.join()
f.close()

print('All socket and write threads joined')
checksum = md5checksum(output_path)
print(f'md5 checksum: {checksum}')
if checksum == '70a4b9f4707d258f559f91615297a3ec':
    print('SUCCESS!')
else:
    print('FAILURE')

plt.figure()
plt.xlabel('Time (in s)')
plt.ylabel('Bytes downloaded')
plt.title(f'Progress of file download\n(Chunk size: {Chunk_size} bytes)')
conn_no = 0
for j in range(len(Server_hosts)):
    for i in range(N_connections[j]):
        plt.plot(Time_record[conn_no], Bytes_record[conn_no], label=f'Conn-{conn_no} ({Server_hosts[j]})')
        conn_no += 1
plt.legend()
plt.savefig('download-progress.png')