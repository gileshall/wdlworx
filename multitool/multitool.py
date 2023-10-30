from pprint import pformat
import os

os.makedirs("output", exist_ok=True)
listdir = os.listdir('.')
with open("output/pformat.txt", "w") as fh:
    fh.write(pformat(listdir))

