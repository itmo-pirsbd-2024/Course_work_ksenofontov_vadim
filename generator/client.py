import socket
import sys
from datetime import datetime, timedelta
from time import sleep
import json
from random import randint

min_x_coord = 0
max_x_coord = 4000
min_y_coord = 0
max_y_coord = 4000
min_z_coord = -4000
max_z_coord = 4000

class user_status:
    def __init__(self, _time_stamp, _entry_number, _user_id, _x, _y, _z, _pulse):
        self.time_stamp = _time_stamp
        self.entry_number = _entry_number
        self.user_id = _user_id
        self.x = _x #decimeters
        self.y = _y #decimeters
        self.z = _z #decimeters
        self.pulse = _pulse

def update_status_using_synthetic_data(entry : user_status, interval_in_seconds : int):
    pulse_of_rest = 70
    speed_of_rest = 0

    walking_speed = 6 #kmh
    pulse_of_walking = 81

    easy_running_speed = 12 #kmh
    pulse_of_easy_running = 140

    hard_running_speed = 20 #kmh
    pulse_of_hard_running = 190

    bicycle_speed = 20  # kmh
    pulse_of_cycling = 115

    car_speed = 30  # kmh
    pulse_of_driving = pulse_of_rest

    plane_speed = 750  # kmh
    pulse_of_flying = pulse_of_rest

    height_of_plane_flying = 10000 # km
    
    with open("user" + str(entry.user_id) + "/current_action.txt") as current_action_file:
        current_action = current_action_file.read()

        action = current_action.split(' ')[0]
        direction = current_action.split(' ')[1]

        action_speed = 0
        pulse = 0

        match action:
            case "rest":
                action_speed = speed_of_rest
                pulse = pulse_of_rest
            case "walk":
                action_speed = walking_speed
                pulse = pulse_of_walking
            case "easy_run":
                action_speed = easy_running_speed
                pulse = pulse_of_easy_running
            case "hard_run":
                action_speed = hard_running_speed
                pulse = pulse_of_hard_running
            case "cycle":
                action_speed = bicycle_speed
                pulse = pulse_of_cycling
            case "drive":
                action_speed = car_speed
                pulse = pulse_of_driving
            case "fly":
                action_speed = plane_speed
                pulse = pulse_of_flying
                entry.z = height_of_plane_flying

        delta = int((action_speed * 10000 / (3600 / interval_in_seconds)) * (randint(8, 12) / 10))

        match direction:
            case "forward":
                entry.x += delta
            case "back":
                entry.x -= delta
            case "right":
                entry.y += delta
            case "left":
                entry.y -= delta
            case "up":
                entry.z += delta
            case "down":
                entry.z -= delta

        entry.x = min_x_coord + ((entry.x - min_x_coord) % (max_x_coord - min_x_coord))
        entry.y = min_y_coord + ((entry.y - min_y_coord) % (max_y_coord - min_y_coord))
        entry.z = min_z_coord + ((entry.z - min_z_coord) % (max_z_coord - min_z_coord))

        entry.time_stamp = datetime.now()
        entry.entry_number += 1
        entry.pulse = pulse

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

def get_json_string_from_user_state(entry : user_status) -> str:

    fields = entry.__dict__

    fields['time_stamp'] = entry.time_stamp.strftime('%Y-%m-%d %H:%M:%S.%f')
    return json.dumps(fields)

def get_and_update_status(entry : user_status, interval_in_seconds : int):
    update_status_using_synthetic_data(entry, interval_in_seconds)


n = len(sys.argv)

if(n < 2):
    user_id = 0
else:
    json_file_path = sys.argv[1] + '/user_config.json'

    with open(json_file_path) as json_data:
        data = json.load(json_data)

    print(data)
    user_id = data["user_id"]


initial_x = 0
initial_y = 0
initial_z = 0

if(n >= 5):
    initial_x = int(sys.argv[2])
    initial_y = int(sys.argv[3])
    initial_z = int(sys.argv[4])

initial_x = min_x_coord + ((initial_x - min_x_coord) % (max_x_coord - min_x_coord))
initial_y = min_y_coord + ((initial_y - min_y_coord) % (max_y_coord - min_y_coord))
initial_z = min_z_coord + ((initial_z - min_z_coord) % (max_z_coord - min_z_coord))

us = user_status(datetime.now(), 1, user_id, initial_x, initial_y, initial_z, 0)

address_to_server = ('192.168.0.106', 8686)

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(address_to_server)

interval_in_seconds = 5

while client:
    get_and_update_status(us, interval_in_seconds)

    client.send(get_json_string_from_user_state(us).encode())
    print(us.__dict__)
    sleep(interval_in_seconds)

