# thumbnail_maker.py
import time
import os
import logging
from urllib.parse import urlparse
from urllib.request import urlretrieve
from queue import Queue
from threading import Thread, Lock
import multiprocessing

import asyncio
import aiohttp
import aiofiles

import PIL
from PIL import Image

FORMAT = "[%(threadName)s, %(asctime)s, %(levelname)s ] %(message)s"
logging.basicConfig(filename='async_process.log', level=logging.DEBUG)

class ThumbnailMakerService(object):
    def __init__(self, home_dir='.'):
        self.home_dir = home_dir
        self.input_dir = self.home_dir + os.path.sep + 'incoming'
        self.output_dir = self.home_dir + os.path.sep + 'outgoing'
        self.img_queue = multiprocessing.JoinableQueue()
        self.dl_size = 0
        self.resize_size = multiprocessing.Value('i',0)

    # Coroutine Function
    async def download_image_coro(self, session, url):
        
        
        img_filename = urlparse(url).path.split('/')[-1]
        img_filepath = self.input_dir + os.path.sep + img_filename
        
        # Download the image and save
        async with session.get(url) as response:
            async with aiofiles.open(img_filepath, 'wb') as f:
                content = await response.content.read()
                await f.write(content)

        self.img_queue.put(img_filename)        
        self.dl_size += os.path.getsize(self.input_dir + os.path.sep + img_filename)

    async def download_images_coro(self, img_url_list):
        async with aiohttp.ClientSession() as session:
            for url in img_url_list:
                await self.download_image_coro(session, url)
    
    def download_images(self, img_url_list):
        if not img_url_list:
            return
        os.makedirs(self.input_dir, exist_ok=True)

        logging.info('Begining Image Downloads')

        start = time.perf_counter()
        
        loop = asyncio.get_event_loop()

        try:
            loop.run_until_complete(self.download_images_coro(img_url_list))
        finally:
            loop.close()

        end = time.perf_counter()

        logging.info(f'Downloading {len(img_url_list)} images took {end - start} seconds')
        

    def perform_resizing(self):
        # # validate inputs
        if not os.listdir(self.input_dir):
            return
        os.makedirs(self.output_dir, exist_ok=True)

        target_sizes = [32, 64, 200]
        num_images = len(os.listdir(self.input_dir))

        start = time.perf_counter()
        while True:
            filename = self.img_queue.get()
            if filename:
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
                self.img_queue.task_done()
            else:
                self.img_queue.task_done()
                break
        end = time.perf_counter()

    def make_thumbnails(self, img_url_list):
        start = time.perf_counter()
        logging.info("START make_thumbnails")

        self.download_images(img_url_list)
        s = time.perf_counter()
        num_workers = multiprocessing.cpu_count()
        for _ in range(num_workers):
            p = multiprocessing.Process(target = self.perform_resizing)
            p.start()
        e = time.perf_counter()
        
        # Calling download_images after processess are start 
        # so as soon as an image is in the queue processess
        # can start resizing
        # self.download_images(img_url_list)

        for _ in range(num_workers):
            self.img_queue.put(None) # Poison Pill so that the resize queue can terminate
        

        end = time.perf_counter()

        logging.info(f'Time for resizing = {e-s}')
        logging.info(f'Download size = {self.dl_size} and resize images size = {self.resize_size.value}')
        logging.info("END make_thumbnails in {} seconds".format(end - start))
    