# -*- coding: utf-8 -*-
from __future__ import print_function

"""
    Date: 10/1/16
    &copy;2016 Takanori Ogata. All rights reserved.
"""
__author__ = 'ogata'

import os
import csv
from time import time
import requests
from tqdm import tqdm
from concurrent import futures
import click


def download_image(url, save_dirname):
    file_name = url.split("/")[-1]
    if os.path.exists(os.path.join(save_dirname, file_name)):
        return

    res = requests.get(url, stream=True)
    if res.status_code == 200:
        with open(os.path.join(save_dirname, file_name), 'wb') as file:
            for chunk in res.iter_content(chunk_size=1024):
                file.write(chunk)
    else:
        print('warn: status code is {} - {}'.format(res.status_code, url))


def load_csv(csv_filepath):
    with open(csv_filepath, 'rb') as fp:
        reader = csv.reader(fp)
        next(reader)  # skip header
        lists = list(reader)
    return lists


def download_images_from_csv(csv_filepath, save_dir, num_workers):
    lists = load_csv(csv_filepath)

    def process(row):
        image_id, url = row[0], row[1]
        save_dirname = os.path.join(save_dir, image_id)
        if not os.path.exists(save_dirname):
            os.makedirs(save_dirname)
        download_image(url, save_dirname)

    print('start to download...')
    t_start = time()
    with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        mappings = [executor.submit(process, row) for row in lists]
        for dummy in tqdm(futures.as_completed(mappings), total=len(mappings)):
            pass

    t_end = time()
    print('total: {}[s]'.format(t_end - t_start))


@click.command()
@click.argument('train_csv')
@click.argument('validation_csv')
@click.argument('save_dir')
@click.option('--num_workers', default=1)
def main(train_csv, validation_csv, save_dir, num_workers):
    print('download train images')
    download_images_from_csv(train_csv,
                             os.path.join(save_dir, 'train'), num_workers)

    print('download validation images')
    download_images_from_csv(validation_csv,
                             os.path.join(save_dir, 'validation'), num_workers)


if __name__ == '__main__':
    main()
