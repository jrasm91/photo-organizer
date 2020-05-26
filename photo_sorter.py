
import os
import os.path
import sys
import json
import imagehash
import pickledb

from PIL import Image, ExifTags
from multiprocessing import Pool, Process, Queue, cpu_count
from datetime import datetime

if len(sys.argv) != 3:
  print('''
usage: ./photo_sorter.py <directory> <database>
  dir      - scan for image files in this directory
  database - store image are written to this file
  ''')
  sys.exit(1)

DIRECTORY = sys.argv[1]
DB_FILE = sys.argv[2]

EXTENSIONS = ('jpg', 'JPG', 'jpeg', 'JPEG', 'png', 'PNG')
NCORE = cpu_count()
HASHER_COUNT = NCORE - 1
WRITERS_COUNT = 1


class Stats():
    def __init__(self):
        self.total = 0
        self.skipped = 0
        self.processed = 0
        self.start_time = datetime.now()

    def __str__(self):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        return '[INFO] [Stats] %d elapsed seconds, %d total files' % (elapsed, self.total)


def writer(results):
    stats = Stats()
    db = pickledb.load(DB_FILE, True)

    while True:
        item = results.get()
        if item is None:
            print(str(stats))
            break
        item = json.loads(item)
        image_hash = item['image_hash']
        filename = item['filename']
        files = db.get(image_hash)
        if not files:
            files = []

        duplicate = False
        for file in files:
          if file['filename'] == filename:
            duplicate = True

        if not duplicate:
          files.append({'filename': filename})
          db.set(image_hash, files)

        stats.total += 1
        if stats.total % 100 == 0:
            print(str(stats))
        if stats.total % 1000 == 0:
            print('[INFO] [Finished] %s' % filename)


def hasher(tasks, results):
    while True:
        filename = tasks.get()
        if filename is None:
            break
        image_hash = str(imagehash.average_hash(Image.open(filename)))
        results.put(json.dumps(
            {'image_hash': image_hash, 'filename': filename}))


def files(folder, extensions):
    count = 1
    for directory, subdirs, fileList in os.walk(folder):
        print('[INFO] [Scanning] %s...' % directory)
        for filename in fileList:
            # if count > 50:
            #     return
            if filename.endswith(extensions):
                count += 1
                yield os.path.join(directory, filename)
    yield None


def main():
    tasks = Queue()
    results = Queue()

    hashers = Pool(HASHER_COUNT,
                   initializer=hasher,
                   initargs=(tasks, results))
    writers = Pool(WRITERS_COUNT,
                   initializer=writer,
                   initargs=(results,))

    for item in files(DIRECTORY, EXTENSIONS):
        tasks.put(item)

    for _ in range(HASHER_COUNT):
        tasks.put(None)
    hashers.close()
    hashers.join()

    for _ in range(WRITERS_COUNT):
        results.put(None)
    writers.close()
    writers.join()

    print('Done')


if __name__ == '__main__':
    main()
