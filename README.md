# File Downloader

A Python script to download a (large) file hosted on one or multiple servers, by opening multiple TCP connections to each server. The download is performed in chunks of fixed size using sockets, and is resilient to network disconnections