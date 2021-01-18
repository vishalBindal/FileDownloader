# File Downloader

A Python script to download a (large) file hosted on one or multiple servers, by opening multiple TCP connections to each server. The download is performed in chunks of fixed size using sockets, and is resilient to network disconnections

## Setup

1. Set up a python 3.6 environment
```
conda create -n env python=3.6
conda activate env
```

2. Install numpy and matplotlib (needed for plotting download progress on different connections)
```
conda install numpy 
conda install matplotlib
```

## Usage

1. In input.csv (or any other csv file), provide a list of urls to the file followed by the number of parallel tcp connections to open to download the file from that server. Keep a comma as the delimiter in each line. 

Example:
```
vayu.iitd.ac.in/big.txt, 3
http://norvig.com/big.txt, 2
```
indicates that *big.txt* is to be downloaded from 2 servers, *vayu.iitd.ac.in* and *norvig.com*, with 3 and 2 parallel tcp connections respectively.

2. Run the script as 
```
python3 client.py <path to input csv>
```

Example if input.csv was used, then run
```
python3 client.py ./input.csv
```

## Output

The target file, alongwith a plot showing the download progress, will be saved in the directory. 

Information regarding the download progress (e.g. chunk allocation and download, network errors) will be printed on the console.

NOTE: The file checksum will be compared against the checksum for *big.txt* mentioned above, and a message will be printed on the console. To validate the checksum of another file, modify it inside client.py 
