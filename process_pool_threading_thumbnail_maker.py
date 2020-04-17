# thumbnail_maker.py
import time
import os
import logging
from urllib.parse import urlparse
from urllib.request import urlretrieve
from queue import Queue
from threading import Thread
import multiprocessing

import PIL
from PIL import Image
FORMAT = "[%(threadName)s, %(asctime)s, %(levelname)s ] %(message)s"
logging.basicConfig(filename='process_pool.log', level=logging.DEBUG, format=FORMAT)

class ThumbnailMakerService(object):
    def __init__(self, home_dir='.'):
        self.home_dir = home_dir
        self.input_dir = self.home_dir + os.path.sep + 'incoming'
        self.output_dir = self.home_dir + os.path.sep + 'outgoing'
        self.img_list = []        

    def download_image(self, dl_queue):
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
                self.img_list.append(img_filename)

                dl_queue.task_done()

            except Queue.Empty:
                logging.exception('Queue Empty')

    def resize_image(self, filename):
        logging.info(f'resizing {filename}')
        orig_img = Image.open(self.input_dir + os.path.sep + filename)
        target_sizes = [32,64,200]
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

        os.remove(self.input_dir + os.path.sep + filename)
        logging.info(f'done resizing {filename}')




    def make_thumbnails(self, img_url_list):
        logging.info("START make_thumbnails")
        dl_queue = Queue()
        pool = multiprocessing.Pool()
        
        start = time.perf_counter()

        for img_url in img_url_list:
            dl_queue.put(img_url)
            logging.info(f'{img_url} put in queue')

        num_dl_threads = 8
        for _ in range(num_dl_threads):
            t = Thread(target = self.download_image, args=(dl_queue,))
            logging.info(f'{t.name} created')
            t.start()

        dl_queue.join() # Blocks the resize thread untill all images are processed

        start_resize = time.perf_counter()
        pool.map(self.resize_image, self.img_list)
        end_resize = time.perf_counter()
        
        end = time.perf_counter()

        pool.close()
        pool.join()
        
        logging.info(f'Created {len(self.img_list)} in { end_resize - start_resize }')
        logging.info("END make_thumbnails in {} seconds".format(end - start))
    