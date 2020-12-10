import random
from pathlib import Path


candidates = ['X', 'Y', 'Z']
home = str(Path.home())
f = open( home + "/local/" + "CondorcetVotes", "w")
for i in range(100000):
    random.shuffle(candidates)
    f.write(','.join(candidates) + "\n")

f.close()
