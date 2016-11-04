# -*- coding: utf-8 -*-
from __future__ import print_function

"""
    Date: 10/1/16
    &copy;2016 Takanori Ogata. All rights reserved.
"""
__author__ = 'ogata'

import os
import re
import csv
from time import time
import requests
from tqdm import tqdm
from concurrent import futures
import click
from scipy.misc import imresize, imread, imsave
from time import sleep
from io import BytesIO
from PIL import Image
import pandas as pd
Image.MAX_IMAGE_PIXELS = 1000000000


def resize_image(img, new_w):

    # if gray scale
    if len(img.shape) == 2:
        h, w = img.shape
    elif len(img.shape) == 3:
        h, w, _ = img.shape
    else:
        raise ValueError('unknown image shape: {}'.format(img.shape))
    scale = float(new_w) / w
    new_h = int(h * scale)
    new_img = imresize(img, (new_h, new_w), interp='bilinear')
    return new_img


def download_image(image_id, url, save_dirname, timeout, resize_width,
                   enable_resize=False):
    file_name = url.split("/")[-1]

    # remove url query
    result = re.search(r'.jpg', file_name)
    if result is None:
        print('warn: cannot parse filename - {}'.format(file_name))
        return False
    file_name = file_name[: result.end()]

    filepath = os.path.join(save_dirname,
                            '{}____{}'.format(image_id, file_name))

    # skip it if already downloaded
    if os.path.exists(filepath):
        return True

    if enable_resize:
        res = requests.get(url, timeout=timeout)
        if res.status_code != 200:
            print('warn: status code is {} - {}'.format(res.status_code, url))
            return False
        try:
            # resize and save image
            img = imread(BytesIO(res.content))
            if img.shape[1] > resize_width:
                resized = resize_image(img, resize_width)
                imsave(filepath, resized)
        except Exception as e:
            print('warn: failed to process {}, url: {}, img.shape: {}'.format(filepath, url, img.shape))
            print(e)
            os.remove(filepath)
            return False
        return True

    # save image directly
    res = requests.get(url, timeout=timeout, stream=True)
    if res.status_code != 200:
        print('warn: status code is {} - {}'.format(res.status_code, url))
        return False

    with open(filepath, 'wb') as f:
        for chunk in res.iter_content():
            f.write(chunk)

    return True


def load_csv(csv_filepath):
    with open(csv_filepath, 'rb') as fp:
        reader = csv.reader(fp)
        next(reader)  # skip header
        lists = list(reader)
    return lists


def download_images_from_csv(csv_filepath, save_dir, num_workers,
                             timeout, enable_resize, resize_width):
    # lists = load_csv(csv_filepath)
    print('csv:', csv_filepath, 'save dir:', save_dir)

    # CSV format is below
    #  [u'ImageID', u'Subset', u'OriginalURL', u'OriginalLandingURL',
    #   u'License', u'AuthorProfileURL', u'Author', u'Title',
    #   u'OriginalSize', u'OriginalMD5', u'Thumbnail300KURL']
    df = pd.read_csv(csv_filepath)
    print('csv data is loaded')

    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    def process(row):
        image_id, url = row['ImageID'], row['Thumbnail300KURL']
        if row['Thumbnail300KURL'] is None:
            url = row['OriginalURL']
        retry_count = 3
        while retry_count > 0:
            # print('download {} {}'.format(image_id, url))
            res = download_image(image_id, url, save_dir, timeout,
                                 resize_width, enable_resize)
            if res:
                break
            retry_count -= 1
            sleep(3.0)
        if retry_count == 0:
            print('warn: cannot download {} {}'.format(image_id, url))

    print('start to download...')
    t_start = time()
    with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        mappings = [executor.submit(process, row) for index, row in df.iterrows()]
        for dummy in tqdm(futures.as_completed(mappings), total=len(mappings)):
            pass

    t_end = time()
    print('total: {}[s]'.format(t_end - t_start))


@click.command()
@click.argument('train_csv')
@click.argument('validation_csv')
@click.argument('save_dir')
@click.option('--num_workers', default=1)
@click.option('--timeout', default=60)
@click.option('--enable_resize', default=False)
@click.option('--resize_width', default=1024)
def main(train_csv, validation_csv, save_dir, num_workers, timeout,
         enable_resize, resize_width):
    print('opt - save_dir: {}, num_workers: {}, enable_resize: {} resize_width: {}, timeout: {}'
          .format(save_dir, num_workers, enable_resize, resize_width, timeout))
    print('download train images')
    download_images_from_csv(train_csv,
                             os.path.join(save_dir, 'train'), num_workers,
                             timeout, enable_resize, resize_width)

    print('download validation images')
    download_images_from_csv(validation_csv,
                             os.path.join(save_dir, 'validation'),
                             num_workers, timeout, enable_resize, resize_width)


if __name__ == '__main__':
    main()
