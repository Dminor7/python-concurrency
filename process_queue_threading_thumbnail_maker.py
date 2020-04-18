# thumbnail_maker.py
import time
import os
import logging
from urllib.parse import urlparse
from urllib.request import urlretrieve
from queue import Queue
from threading import Thread, Lock
import multiprocessing

import PIL
from PIL import Image
FORMAT = "[%(threadName)s, %(asctime)s, %(levelname)s ] %(message)s"
logging.basicConfig(filename='process_queue.log', level=logging.DEBUG)

class ThumbnailMakerService(object):
    def __init__(self, home_dir='.'):
        self.home_dir = home_dir
        self.input_dir = self.home_dir + os.path.sep + 'incoming'
        self.output_dir = self.home_dir + os.path.sep + 'outgoing'
        self.img_queue = multiprocessing.JoinableQueue()
        self.dl_size = 0
        self.resize_size = multiprocessing.Value('i',0)

    def download_image(self, dl_queue, dl_size_lock):
        os.makedirs(self.input_dir, exist_ok=True)
        while not dl_queue.empty():
            logging.info('START')
            try:
                url = dl_queue.get(block=False)
                # Download the image
                img_filename = urlparse(url).path.split('/')[-1]
                logging.info(f'Downloading image {img_filename} from {url}')
                urlretrieve(url, self.input_dir + os.path.sep + img_filename)
                logging.info(f'Downloaded image {img_filename}')
                with dl_size_lock:
                    self.dl_size += os.path.getsize(self.input_dir + os.path.sep + img_filename)

                self.img_queue.put(img_filename)

                dl_queue.task_done()

            except Queue.Empty:
                logging.exception('Queue Empty')

    

    def perform_resizing(self):
        # # validate inputs
        # if not os.listdir(self.input_dir):
        #     return
        os.makedirs(self.output_dir, exist_ok=True)

        logging.info("beginning image resizing")
        target_sizes = [32, 64, 200]
        num_images = len(os.listdir(self.input_dir))

        start = time.perf_counter()
        while True:
            filename = self.img_queue.get()
            if filename:
                logging.info(f'resizing {filename}')
                orig_img = Image.open(self.input_dir + os.path.sep + filename)
                for basewidth in target_sizes:
                    img = orig_img
                    # calculate target height of the resized image to maintain the aspect ratio
                    wpercent = (basewidth / float(img.size[0]))
                    hsize = int((float(img.size[1]) * float(wpercent)))
                    # perform resizing
                    img = img.resize((basewidth, hsize), PIL.Image.LANCZOS)
                    
                    # save the resized image to the output dir with a modified file name 
                    new_filename = os.path.splitext(filename)[0] + \
                        '_' + str(basewidth) + os.path.splitext(filename)[1]
                    img.save(self.output_dir + os.path.sep + new_filename)
                    with self.resize_size.get_lock():
                        self.resize_size.value += os.path.getsize(self.output_dir + os.path.sep + new_filename)
                os.remove(self.input_dir + os.path.sep + filename)
                logging.info(f'done resizing {filename}')
                self.img_queue.task_done()
            else:
                self.img_queue.task_done()
                break
        end = time.perf_counter()
            


        logging.info("created thumbnails in {} seconds".format(end - start))

    def make_thumbnails(self, img_url_list):
        logging.info("START make_thumbnails")
        dl_queue = Queue()
        dl_size_lock = Lock()
        start = time.perf_counter()

        for img_url in img_url_list:
            dl_queue.put(img_url)
            logging.info(f'{img_url} put in queue')

        num_dl_threads = 8
        for _ in range(num_dl_threads):
            t = Thread(target = self.download_image, args = (dl_queue,dl_size_lock,))
            logging.info(f'{t.name} created')
            t.start()

        num_workers = multiprocessing.cpu_count()

        s = time.perf_counter()
        for _ in range(num_workers):
            p = multiprocessing.Process(target = self.perform_resizing)
            p.start()


        e = time.perf_counter()
        
        dl_queue.join() # Blocks the resize thread untill all images are processed
        
        for _ in range(num_workers):
            self.img_queue.put(None) # Poison Pill so that the resize queue can terminate
        

            

        end = time.perf_counter()


        logging.info(f'Time fot resizing = {e-s}')
        logging.info(f'Download size = {self.dl_size} and resize images size = {self.resize_size.value}')
        logging.info("END make_thumbnails in {} seconds".format(end - start))
    