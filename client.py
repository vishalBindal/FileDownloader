import socket
import hashlib
from pathlib import Path
import os
import sys

output_path = 'output.txt'
if not os.path.exists(output_path):
    Path(output_path).mkdir(parents=True, exist_ok=True)

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
    return len(lines)>0 and lines[0] == b'HTTP/1.1 200 OK'

def get_content_length(header):
    lines = header.splitlines()
    for line in lines:
        words = line.split()
        if len(words)>0 and words[0] == b'Content-Length:':
            return int(words[1])
    return -1

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
f = open(output_path, 'wb')
s.connect(('vayu.iitd.ac.in', 80))
s.sendall(b'GET /big.txt HTTP/1.1\r\nHost: vayu.iitd.ac.in\r\n\r\n')

data = s.recv(4096)
header, content = split_header(data)
if not check_ok(header):
    print('200 OK response not received')
    exit()
content_length = get_content_length(header)
if content_length == -1:
    print('Content-length field not found')
    exit()

current_length = len(data)
f.write(content)

while current_length < content_length:
    data = s.recv(4096)
    if len(data) == 0:
        print('Unexpected closure')
        break
    current_length = current_length + len(data)
    f.write(data)

s.close()
f.close()

print(md5checksum(output_path))