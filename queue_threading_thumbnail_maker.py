# thumbnail_maker.py
import time
import os
import logging
from urllib.parse import urlparse
from urllib.request import urlretrieve
from queue import Queue
from threading import Thread


import PIL
from PIL import Image
FORMAT = "[%(threadName)s, %(asctime)s, %(levelname)s ] %(message)s"
logging.basicConfig(filename='queue.log', level=logging.DEBUG)

class ThumbnailMakerService(object):
    def __init__(self, home_dir='.'):
        self.home_dir = home_dir
        self.input_dir = self.home_dir + os.path.sep + 'incoming'
        self.output_dir = self.home_dir + os.path.sep + 'outgoing'
        self.img_queue = Queue()
        self.dl_queue = Queue() # Queue to avoid sequential downloads

    def download_image(self):
        os.makedirs(self.input_dir, exist_ok=True)
        while not self.dl_queue.empty():
            logging.info('START')
            try:
                url = self.dl_queue.get(block=False)
                # Download the image
                img_filename = urlparse(url).path.split('/')[-1]
                logging.info(f'Downloading image {img_filename} from {url}')
                urlretrieve(url, self.input_dir + os.path.sep + img_filename)
                logging.info(f'Downloaded image {img_filename}')
                self.img_queue.put(img_filename)

                self.dl_queue.task_done()

            except Queue.Empty:
                logging.exception('Queue Empty')

    def download_images(self, img_url_list):
        # validate inputs
        if not img_url_list:
            return
        os.makedirs(self.input_dir, exist_ok=True)
        
        logging.info("beginning image downloads")

        start = time.perf_counter()
        for url in img_url_list:
            # download each image and save to the input dir 
            
            img_filename = urlparse(url).path.split('/')[-1]
            urlretrieve(url, self.input_dir + os.path.sep + img_filename)
            self.img_queue.put(img_filename)

        end = time.perf_counter()

        self.img_queue.put(None) # Poison Pill to let consumer know when to break loop

        logging.info("downloaded {} images in {} seconds".format(len(img_url_list), end - start))

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

        start = time.perf_counter()

        for img_url in img_url_list:
            self.dl_queue.put(img_url)
            logging.info(f'{img_url} put in queue')

        num_dl_threads = 8
        for _ in range(num_dl_threads):
            t = Thread(target = self.download_image)
            logging.info(f'{t.name} created')
            t.start()

        # t1 = Thread(target = self.download_images, args = ([img_url_list]))
        t2 = Thread(target = self.perform_resizing)

        # t1.start()
        t2.start()

        # t1.join()

        self.dl_queue.join() # Blocks the resize thread untill all images are processed
        self.img_queue.put(None) # Poison Pill so that the resize queue can terminate
        t2.join()

        end = time.perf_counter()

        logging.info("END make_thumbnails in {} seconds".format(end - start))
    