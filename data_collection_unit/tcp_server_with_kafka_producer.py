from kafka import KafkaProducer

import select
import socket
from datetime import datetime
import json

# based on https://habr.com/ru/companies/alfa/articles/354728/


class user_status:
    def __init__(self, _time_stamp, _entry_number, _user_id, _x, _y, _z, _pulse):
        self.time_stamp = _time_stamp
        self.entry_number = _entry_number
        self.user_id = _user_id
        self.x = _x #decimeters
        self.y = _y #decimeters
        self.z = _z #decimeters
        self.pulse = _pulse

size_of_user_status = 56

def get_user_state_from_bytes(bts : bytes)->user_status:
    r_bts_timestamp_ms = bytes(bts[0:8])
    r_bts_entry_number = bytes(bts[8:16])
    r_bts_user_id = bytes(bts[16:24])
    r_bts_x = bytes(bts[24:28])
    r_bts_y = bytes(bts[28:32])
    r_bts_z = bytes(bts[32:36])
    r_bts_pulse = bytes(bts[36:])

    r_us = user_status(datetime.now(), 0, 0, 0, 0, 0, 0)

    r_us.time_stamp = datetime.fromtimestamp(int.from_bytes(r_bts_timestamp_ms, 'little', signed=False) / 1000.0)
    r_us.entry_number = int.from_bytes(r_bts_entry_number, 'little', signed=False)
    r_us.user_id = int.from_bytes(r_bts_user_id, 'little', signed=False)
    r_us.x = int.from_bytes(r_bts_x, 'little', signed=False)
    r_us.y = int.from_bytes(r_bts_y, 'little', signed=False)
    r_us.z = int.from_bytes(r_bts_z, 'little', signed=True)
    r_us.pulse = int.from_bytes(r_bts_pulse, 'little', signed=False)

    return r_us

def get_bytes_from_user_state(entry : user_status) -> bytes:
    bts_timestamp_ms = int(entry.time_stamp.timestamp() * 1000).to_bytes(8, 'little', signed=False)
    bts_entry_number = entry.entry_number.to_bytes(8, 'little', signed=False)
    bts_user_id = entry.user_id.to_bytes(8, 'little', signed=False)
    bts_x = entry.x.to_bytes(4, 'little', signed=False)
    bts_y = entry.y.to_bytes(4, 'little', signed=False)
    bts_z = entry.z.to_bytes(4, 'little', signed=True)
    bts_pulse = entry.pulse.to_bytes(4, 'little', signed=False)

    res = bytearray()
    for i in bts_timestamp_ms:
        res.append(i)
    for i in bts_entry_number:
        res.append(i)
    for i in bts_user_id:
        res.append(i)
    for i in bts_x:
        res.append(i)
    for i in bts_y:
        res.append(i)
    for i in bts_z:
        res.append(i)
    for i in bts_pulse:
        res.append(i)

    return res

def get_user_state_from_json_string(json_string : str) -> user_status:
    json_object = json.loads(json_string)

    r_us = user_status(datetime.now(), 0, 0, 0, 0, 0, 0)

    r_us.time_stamp = datetime.strptime(json_object['time_stamp'], '%Y-%m-%d %H:%M:%S.%f')
    r_us.entry_number = json_object['entry_number']
    r_us.user_id = json_object['user_id']
    r_us.x = json_object['x']
    r_us.y = json_object['y']
    r_us.z = json_object['z']
    r_us.pulse = json_object['pulse']

    return r_us



TCP_SERVER_ADDRESS = ('192.168.0.4', 8686)
MAX_CONNECTIONS = 20
kafka_producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092')

INPUTS = list()
OUTPUTS = list()


def get_non_blocking_server_socket():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setblocking(0)
    server.bind(TCP_SERVER_ADDRESS)
    server.listen(MAX_CONNECTIONS)

    return server


def handle_readables(readables, server):
    """    Обработка появления событий на входах"""
    for resource in readables:
        # Если событие исходит от серверного сокета, то мы получаем новое подключение
        if resource is server:
            connection, client_address = resource.accept()
            connection.setblocking(0)
            INPUTS.append(connection)
            print("new connection from {address}".format(address=client_address))

        # Если событие исходит не от серверного сокета, но сработало прерывание на наполнение входного буффера
        else:
            data = bytes()
            try:
                data = resource.recv(4096)

            # Если сокет был закрыт на другой стороне
            except ConnectionResetError:
                pass

            if data:
                # user_status_obg = get_user_state_from_bytes(data)
                user_status_obg = get_user_state_from_json_string(data.decode())

                # future = kafka_producer.send('user-statuses-topic', data)
                # future = kafka_producer.send('user-statuses-topic', get_bytes_from_user_state(user_status_obg))
                
                # pulse
                future = kafka_producer.send(topic = 'pulse', key = user_status_obg.user_id.to_bytes(8, 'big', signed=False), value = user_status_obg.pulse.to_bytes(4, 'big', signed=False))
                result = future.get(timeout=1)


                # location
                location_bytes = bytearray()
                bts_x = user_status_obg.x.to_bytes(4, 'big', signed=False)
                bts_y = user_status_obg.y.to_bytes(4, 'big', signed=False)
                bts_z = user_status_obg.z.to_bytes(4, 'big', signed=True)
    
                for i in bts_x:
                    location_bytes.append(i)
                for i in bts_y:
                    location_bytes.append(i)
                for i in bts_z:
                    location_bytes.append(i)
                
                future = kafka_producer.send(topic = 'location', key = user_status_obg.user_id.to_bytes(8, 'big', signed=False), value = location_bytes)
               
                result = future.get(timeout=1)
                print("getting data: {recieved_status}".format(recieved_status=(user_status_obg.__dict__)))

                # Говорим о том, что мы будем еще и писать в данный сокет
                if resource not in OUTPUTS:
                    OUTPUTS.append(resource)

            # Если данных нет, но событие сработало, то ОС нам отправляет флаг о полном прочтении ресурса и его закрытии
            else:
                clear_resource(resource)

def clear_resource(resource):
    if resource in OUTPUTS:
        OUTPUTS.remove(resource)
    if resource in INPUTS:
        INPUTS.remove(resource)
    resource.close()

    print('closing connection ' + str(resource))


def handle_writables(writables):
    pass


if __name__ == '__main__':
    # Создаем серверный сокет без блокирования основного потока в ожидании подключения
    server_socket = get_non_blocking_server_socket()
    INPUTS.append(server_socket)

    print("server is running, please, press ctrl+c to stop")
    try:
        while INPUTS:
            readables, writables, exceptional = select.select(INPUTS, OUTPUTS, INPUTS)
            handle_readables(readables, server_socket)
            handle_writables(writables)
    except KeyboardInterrupt:
        clear_resource(server_socket)
        print("Server stopped! Thank you for using!")
