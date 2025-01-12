import random
import subprocess
import time


random.seed(0)

for i in range(6):
    subprocess.Popen([r'.venv\Scripts\python', 'client.py', 'user' + str(i + 1),
                         str(random.randint(0, 400_000_001)),
                         str(random.randint(0, 400_000_001)),
                         str(0)])
    time.sleep(0.2)

