#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import logging
import collections
import pathlib
import time
import numpy as np
import threading
from optparse import OptionParser
from concurrent.futures.thread import ThreadPoolExecutor
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])
Statistic = {"errors": 0, "processed": 0}

def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if dry_run:
            ua_formated = str(ua).replace('\n', ' ')
           # logging.debug(f"{memc_addr} - {key} -> {ua_formated} \n" )
        else:
            memc = memcache.Client([memc_addr])
            memc.set(key, packed)
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def process_lines(device_memc, options, lines, thread_id):
    global Statistic
    chunk_errors = chunk_processed = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        line = line.decode()
        appsinstalled = parse_appsinstalled(line)
        if not appsinstalled:
            chunk_errors += 1
            continue
        memc_addr = device_memc.get(appsinstalled.dev_type)
        if not memc_addr:
            chunk_errors += 1
            logging.error("Unknow device type: %s" % appsinstalled.dev_type)
            continue
        ok = insert_appsinstalled(memc_addr, appsinstalled, options.dry)  # Эту часть можно запараллелить
        if ok:
            chunk_processed += 1
        else:
            chunk_errors += 1
    print(f"Log  {thread_id} finish work")
    Statistic["errors"] += chunk_errors
    Statistic["processed"] += chunk_processed


def main(options):
    global Statistic
    global Threads_finish_work
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    rename_query = []  # очередь для переименования (эта операция должна быть последовательной)

   # print(f"LOG0  {list(glob.iglob(options.pattern))}")
   # print(f"LOG1  {list(pathlib.Path('.').glob(options.pattern))}")
    for fn in pathlib.Path('.').glob(options.pattern):
        logging.info('Processing %s' % fn)
        fd = gzip.open(fn)

        start_time = time.time()
        lines_chunks = np.array_split([line for line in fd], 100)
        #threads = []
        #for i in range(len(lines_chunks)):
        #    t = threading.Thread(target=process_lines, args=(device_memc, options, lines_chunks[i], f"thread{i}"))
        #    threads.append(t)
        #    t.start()
        #while Threads_finish_work < 100:
        #    pass
        with ThreadPoolExecutor(max_workers=100) as executor:
            for i in range(len(lines_chunks)):
                future = executor.submit(process_lines, device_memc, options, lines_chunks[i], f"thread{i}")
                print(future.result())
        print(f"Time for lines procecced --- {(time.time() - start_time)} seconds ---")

        if not Statistic["processed"]:
            fd.close()
            rename_query.append(fn)
            dot_rename(fn)
            Statistic = {"errors": 0, "processed": 0}
            continue

        err_rate = float(Statistic["errors"]) / Statistic["processed"]
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        rename_query.append(fn)
        dot_rename(fn)
        Statistic = {"errors": 0, "processed": 0}
    print(f"LOG2  {len(rename_query)} {rename_query}")


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        start_time = time.time()
        main(opts)
        print(f"--- {(time.time() - start_time)} seconds ---")
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
