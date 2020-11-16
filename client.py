import socket
import hashlib
from pathlib import Path
import os
import sys
import threading
from heapq import heappush, heappop
import math

output_path = 'output.txt'
if not os.path.exists(output_path):
    Path(output_path).mkdir(parents=True, exist_ok=True)
f = open(output_path, 'wb')

N_connections = 5
Server_host = 'vayu.iitd.ac.in'
Target_path = '/big.txt'
Chunk_size = 10000

Threads = []

class DataQueue:
    def __init__(self):
        self.pq = []
        self.lowest_unwritten = 0
        self.lock = threading.Lock()

    def push(self, chunk_no, data):
        self.lock.acquire()
        heappush(self.pq, (chunk_no, data))
        # print(f'Added chunk {chunk_no} to priority queue')
        while len(self.pq)>0 and self.pq[0][0] == self.lowest_unwritten:
            chunk_no, data = heappop(self.pq)
            f.write(data)
            print(f'Written chunk {chunk_no} to disk successfully')
            self.lowest_unwritten = self.lowest_unwritten + 1
        self.lock.release()

class TrackChunks:
    def __init__(self, content_length):
        self.n_chunks = math.ceil(content_length / Chunk_size)
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

def create_request(bytes_start, bytes_end, keep_alive=True):
    alive_message = ''
    if keep_alive:
        alive_message = 'Connection: keep-alive\r\n'
    req = f'GET {Target_path} HTTP/1.1\r\nHost: {Server_host}\r\n{alive_message}Range: bytes={bytes_start}-{bytes_end}\r\n\r\n'
    return req.encode()

def get_request(chunk_no):
    bytes_start = chunk_no*Chunk_size
    bytes_end = bytes_start + Chunk_size - 1
    return create_request(bytes_start, bytes_end)

# Get content-length
content_length = -1
while content_length < 0:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((Server_host, 80))
    s.sendall(create_request(0,0,False))
    data = s.recv(4096)
    header, content = split_header(data)
    if check_ok(header):
        content_length = get_content_length(header)
print(f'Content_length: {content_length} bytes')

def socket_task(s, s_id, tracker, dataqueue):
    while True:
        chunk_no = tracker.get_chunk()
        if chunk_no < 0:
            try:
                s.close()
                print(f'Socket {s_id} disconnected as no more chunks left')
            except:
                print(f'Socket {s_id} not connected. Exiting as no more chunks left')
            break
        while True:
            try:
                s.sendall(get_request(chunk_no))
            except:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((Server_host, 80))
                print(f'Socket {s_id} connected to {Server_host}, port 80')
                s.sendall(get_request(chunk_no))
            print(f'Socket {s_id} sent a GET request for chunk {chunk_no}')
            
            data = s.recv(4096)
            
            header, content = split_header(data)
            if not check_ok(header):
                print(f'Response on socket {s_id} not OK')
                # print(header)
                # print(content)
                # input()
                s.close()
                continue
            chunk_s = get_chunk_size(header)
            if chunk_s < 0:
                print(f'Unable to parse chunk size on socket {s_id}')
                # print(header)
                # print(content)
                # input()
                s.close()
                continue

            current_length = len(content)
            
            while current_length < chunk_s:
                data = s.recv(4096)
                if not data:
                    print(f'Chunk on socket {s_id} not received fully. Will request for chunk again.')
                    break
                current_length = current_length + len(data)
                content = content + data

            if current_length >= chunk_s:
                print(f'Received chunk {chunk_no} on socket {s_id} fully, and will be written to disk shortly.')
                x = threading.Thread(target=dataqueue.push, args=(chunk_no, content))
                x.start()
                Threads.append(x)
                break

tracker = TrackChunks(content_length)
dataqueue = DataQueue()
Sockets = []

for i in range(N_connections):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    Sockets.append(s)
    x = threading.Thread(target=socket_task, args=(s, len(Sockets), tracker, dataqueue))
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