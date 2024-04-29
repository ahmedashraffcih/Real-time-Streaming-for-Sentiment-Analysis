import json
import socket
import time
import pandas as pd 
import logging
import sys

def read_data_from_file(file_path):
    """Reads data from a JSON file and returns it."""
    try:
        with open(file_path, 'r') as file:
            data = [json.loads(line) for line in file]
        return data
    except FileNotFoundError:
        logging.error(f"File not found at path: {file_path}")
        sys.exit(1)


def chunk_data(data, chunk_size):
    """Chunks the data into smaller portions."""
    return [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]


def send_data_over_socket(data_chunks, host='127.0.0.1', port=9999):
    """Sends data chunks over a socket connection."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(1)
    logging.info(f"Listening for connections on {host}:{port}")

    while True:
        conn, addr = s.accept()
        logging.info(f"Connected to {addr}")
        try:
            for chunk in data_chunks:
                serialize_data = json.dumps(chunk).encode('utf-8')
                conn.send(serialize_data + b'\n')  # Send each chunk
                time.sleep(5)  # Simulate real-time streaming
        except (BrokenPipeError, ConnectionResetError):
            logging.error("Client disconnected.")
        finally:
            conn.close()
            logging.info("Connection closed")

if __name__ == '__main__':
    file_path = "src/datasets/yelp_dataset/yelp_academic_dataset_review.json"
    data = read_data_from_file(file_path)
    chunk_size = 2
    data_chunks = chunk_data(data, chunk_size)
    send_data_over_socket(data_chunks)