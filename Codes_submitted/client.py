import argparse
import hashlib
import json
import mmap
import os
import struct
import sys
import time
from multiprocessing import Process, Queue
from os.path import getsize
import socket



# Const Value
OP_SAVE, OP_DELETE, OP_GET, OP_UPLOAD, OP_DOWNLOAD, OP_BYE, OP_LOGIN, OP_ERROR = 'SAVE', 'DELETE', 'GET', 'UPLOAD', 'DOWNLOAD', 'BYE', 'LOGIN', "ERROR"
TYPE_FILE, TYPE_DATA, TYPE_AUTH, DIR_EARTH = 'FILE', 'DATA', 'AUTH', 'EARTH'
FIELD_OPERATION, FIELD_DIRECTION, FIELD_TYPE, FIELD_USERNAME, FIELD_PASSWORD, FIELD_TOKEN = 'operation', 'direction', 'type', 'username', 'password', 'token'
FIELD_KEY, FIELD_SIZE, FIELD_TOTAL_BLOCK, FIELD_MD5, FIELD_BLOCK_SIZE = 'key', 'size', 'total_block', 'md5', 'block_size'
FIELD_STATUS, FIELD_STATUS_MSG, FIELD_BLOCK_INDEX = 'status', 'status_msg', 'block_index'
DIR_REQUEST= 'REQUEST'
OP_EXIT = 'EXIT'
server_ip , server_port = None, 1379
processes = []
re_transmission_time = 20



def receiving_message(client_socket):
    """
    Unpack the code sent from the server side to extract the useful json and binary parts
    :param client_socket:
    :return:
    """
    message = client_socket.recv(100000)
    header = message[:8]
    json_bin_len, binary_data_len = struct.unpack('!II', header)
    if binary_data_len == 0:
        json_bin = message[8:]
        return json.loads(json_bin.decode()), None
    elif binary_data_len != 0:
        json_bin = message[8:8 + json_bin_len]
        binary_data = message[8 + json_bin_len:]
        return json.loads(json_bin.decode()), binary_data



def packing_message(json_data, bin_data=None):
    """
    pack the message
    :param json_data:
    :param bin_data:
    :return:
    """
    j = json.dumps(dict(json_data), ensure_ascii=False)
    j_len = len(j)
    if bin_data is None:
        return struct.pack('!II', j_len, 0) + j.encode()
    else:
        return struct.pack('!II', j_len, len(bin_data)) + j.encode() + bin_data



def creating_message(operation, data_type, json_data, bin_data = None, token = None):
    """
    create the message
    :param operation:
    :param data_type:
    :param json_data:
    :param bin_data:
    :param token:
    :return:
    """
    # parameter write in
    json_data[FIELD_OPERATION] = operation
    json_data[FIELD_TYPE] = data_type
    json_data[FIELD_TOKEN] = token
    json_data[FIELD_DIRECTION] = DIR_REQUEST
    # transmit the message for packing
    return packing_message(json_data, bin_data)



def error_check(json_data, status_code, client_socket):
    """
    check the error message from the server
    :param json_data:
    :param status_code:
    :param client_socket:
    :return:
    """
    if (status_code == 401 or status_code == 402 or status_code == 403 or status_code == 404 or status_code == 405 or
        status_code == 406 or status_code == 407 or status_code == 408 or status_code == 409 or status_code == 410):
        print('Server response: ' + json_data[FIELD_STATUS_MSG])
        print(f'Status code: {status_code}')
        if token is None:
            print('Login fail!')
        print('Client exit.')
        client_socket.close()
        sys.exit()



def file_upload(client_socket, client_file_path):
    """
    file operation main process
    :param client_socket
    :param client_file_path:
    :return:
    """
    # Create queue for storing file block data
    queue = Queue()
    # set total child process num
    process_num = 3
    # start/end index calculation
    dict_start_index, dict_end_index = index_calculate(process_num)
    # mark file upload start time
    upload_start_time = time.time()
    # initiate each child process
    for i in range(1, process_num + 1):
        process = Process(target=multiprocess_file_read,
                          args=(queue, dict_start_index[f'process_{i}'], dict_end_index[f'process_{i}'],
                                block_size, client_file_path, file_size))
        process.start()
        processes.append(process)
    # start queued file upload
    file_block_upload(client_socket, queue, upload_start_time)
    # process safety secure
    for process in processes:
        process.join()



def index_calculate(process_num):
    """
    calculate the start/end block index of each child process
    :param process_num:
    :return:
    """
    # calculate average block num of each child process
    block_num_each = total_block // process_num
    # calculate the start/end block index
    dict_start_index = {}
    dict_end_index = {}
    for i in range(1, process_num + 1):
        if i == 1:
            dict_start_index[f'process_{i}'] = 0
            dict_end_index[f'process_{i}'] = i * block_num_each - 1
        elif i == process_num:
            dict_start_index[f'process_{i}'] = (i - 1) * block_num_each
            dict_end_index[f'process_{i}'] = total_block - 1
        else:
            dict_start_index[f'process_{i}'] = (i - 1) * block_num_each
            dict_end_index[f'process_{i}'] = i * block_num_each - 1
    return dict_start_index, dict_end_index

        
        
def multiprocess_file_read(queue, start_index, end_index, block_size, file_path, file_size):
    """
    read file block from target file concurrently
    :param queue:
    :param start_index:
    :param end_index:
    :param block_size:
    :param file_path:
    :param file_size:
    :return:
    """
    with (open(file_path, 'rb') as file):
        # map the file into RAM to enhance reading speed
        mapped_file = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        block_index = start_index
        while True:
            if block_index > end_index:
                break
            mapped_file.seek(block_size * block_index)
            if block_size * (block_index + 1) < file_size:
                bin_data = mapped_file.read(block_size)
            else:
                bin_data = mapped_file.read(file_size - block_size * block_index)
            # put file block data into queue
            queue.put((block_index, bin_data))
            block_index += 1
        file.close()



def file_block_upload(client_socket, queue, upload_start_time):
    """
    concurrently upload the file block to server along with all the child processes
    :param client_socket:
    :param queue:
    :param upload_start_time:
    :return:
    """
    while True:
        if not queue.empty():
            # get file block data from queue
            block_index, bin_data = queue.get()
            # send file upload request
            rval = {
                FIELD_KEY: file_name,
                FIELD_BLOCK_INDEX: block_index
            }
            # conditional retransmission
            while True:
                client_socket.send(creating_message(OP_UPLOAD, TYPE_FILE, rval, bin_data, token))
                client_socket.settimeout(re_transmission_time)
                try:
                    # receive response
                    json_data, binary_data = receiving_message(client_socket)
                    status_code = json_data[FIELD_STATUS]
                    break
                except socket.timeout:
                    print(f"Re Transmission{block_index}")
            # error check
            error_check(json_data, status_code, client_socket)
            # client display
            print('Server response: ' + json_data[FIELD_STATUS_MSG])
            print(f'Status code: {status_code}')
            # check for md5
            if FIELD_MD5 in json_data.keys():
                print(f'\nFile md5: {json_data[FIELD_MD5]}')
                # mark file upload end time
                upload_end_time = time.time()
                # calculate total file upload time
                upload_total_time = (upload_end_time - upload_start_time)
                print(f'Upload time: {upload_total_time}')
                break



def upload_plan_retrieve(client_socket, client_file_path):
    """
    send file save request to server
    :param client_socket:
    :param client_file_path:
    :return:
    """
    global total_block, block_size, file_size, file_name
    # file information retrieve
    file_name = os.path.basename(client_file_path)
    file_size = getsize(client_file_path)
    # send file save request
    rval = {
        FIELD_KEY: file_name,
        FIELD_SIZE: file_size
    }
    client_socket.send(creating_message(OP_SAVE, TYPE_FILE, rval, None, token))
    # receiving response
    json_data, binary_data = receiving_message(client_socket)
    status_code = json_data[FIELD_STATUS]
    # error check
    error_check(json_data, status_code, client_socket)
    # client display
    print('\nServer response: ' + json_data[FIELD_STATUS_MSG])
    print(f'File key: {json_data[FIELD_KEY]}')
    print(f'File size: {json_data[FIELD_SIZE]}')
    print(f'File total block: {json_data[FIELD_TOTAL_BLOCK]}')
    print(f'File block size: {json_data[FIELD_BLOCK_SIZE]}')
    print(f'Status code: {status_code}\n')
    total_block = json_data[FIELD_TOTAL_BLOCK]
    block_size = json_data[FIELD_BLOCK_SIZE]



def login(client_socket, client_id):
    """
    log in and get token
    :param client_socket
    :param client_id:
    :return:
    """
    global token
    # send login request
    rval = {
        FIELD_USERNAME: client_id,
        FIELD_PASSWORD: hashlib.md5(client_id.encode()).hexdigest().lower()
    }
    client_socket.send(creating_message(OP_LOGIN, TYPE_AUTH, rval))
    # receive response
    json_data, binary_data = receiving_message(client_socket)
    status_code = json_data[FIELD_STATUS]
    # error check
    error_check(json_data, status_code, client_socket)
    # client display
    print('Server response: ' + json_data[FIELD_STATUS_MSG])
    print(f'Status code: {status_code}')
    token = json_data[FIELD_TOKEN]
    print('This is your token: ' + token)



def socket_set_up():
    """
    set up TCP connection
    :return client_socket
    """
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((server_ip, server_port))
    return client_socket



def main():
    """
    entrance of the whole program
    :return:
    """
    global server_ip
    parser = _argparse()
    server_ip = parser.server_ip
    client_id = parser.id
    client_file_path = parser.file_path
    # set up TCP connection
    client_socket = socket_set_up()
    # login
    login(client_socket, client_id)
    # start file operation
    upload_plan_retrieve(client_socket, client_file_path)
    # start file upload
    file_upload(client_socket, client_file_path)
    # end
    print("\nClient close.")
    client_socket.close()
    sys.exit()



def _argparse():
    """
    external argument adding
    :return:
    """
    parser = argparse.ArgumentParser(description="This is description!")
    parser.add_argument('--server_ip', action='store', required=True, dest='server_ip', help='The IP of server')
    parser.add_argument('--id', action='store', required=True, dest='id', help='The user id')
    parser.add_argument('--f', action='store', required=True, dest='file_path', help='The uploading file path')
    return parser.parse_args()



if __name__ == '__main__':
    main()