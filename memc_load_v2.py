#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import logging
import collections
import pathlib
import time

from optparse import OptionParser
from multiprocessing import Process, Manager
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])

def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc, memc_addr, appsinstalled, dry_run=False, retry=3):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    while retry:
        if dry_run:
            ua_formated = str(ua).replace('\n', ' ')
            # logging.debug(f"{memc_addr} - {key} -> {ua_formated} \n" )
        else:
            try:
                memc.set(key, packed)
            except Exception as e:
                logging.exception(f"Cannot write to memc {memc_addr}: {e} - retries {retry}")
                retry -= 1
                if retry:
                    time.sleep(1)
                    continue
                else:
                    break
        return True
    return False


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


class Loader:
    def __init__(self):
        self.lines = []

    def process_lines(self, device_memc, options, process_fished, Statistic):
        chunk_errors = chunk_processed = 0
        Client_connections = {}   # for persistent connection
        for line in self.lines:
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
            if memc_addr in Client_connections.keys():
                ok = insert_appsinstalled(Client_connections[memc_addr], memc_addr, appsinstalled, options.dry)
            else:
                Client_connections[memc_addr] = memcache.Client([memc_addr])
                ok = insert_appsinstalled(Client_connections[memc_addr], memc_addr, appsinstalled, options.dry)
            if ok:
                chunk_processed += 1
            else:
                chunk_errors += 1
        Statistic["errors"] += chunk_errors
        Statistic["processed"] += chunk_processed
        process_fished.value += 1


def use_loaders_for_lines(lines, device_memc, options):
    manager = Manager()
    process_fished = manager.Value('i', 0)
    Statistic = manager.dict()
    Statistic["errors"] = 0
    Statistic["processed"] = 0
    line_num = 0
    loaders_list = []
    for i in range(int(options.p_count)):
        loaders_list.append(Loader())
    for line in lines:
        loaders_list[line_num % len(loaders_list)].lines.append(line)
        line_num += 1
    for i in range(len(loaders_list)):
        p = Process(target=Loader.process_lines, args=(loaders_list[i], device_memc, options,
                                                       process_fished, Statistic))
        p.start()

    while process_fished.value < int(options.p_count):
        time.sleep(5)
    return Statistic


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    for fn in pathlib.Path('.').glob(options.pattern):
        logging.info('Processing %s' % fn)
        fd = gzip.open(fn)
        Statistic = {"errors": 0, "processed": 0}

        # Распределение линий по лоадерам
        file_iter = iter(fd)
        lines = []
        while True:
            try:
                lines.append(file_iter.__next__())
                if len(lines) == 100000:  # ограничение для экономии памяти
                    result = use_loaders_for_lines(lines, device_memc, options)
                    Statistic["errors"] += result["errors"]
                    Statistic["processed"] += result["processed"]
                    lines = []
                    print(f"Statistic update {Statistic}")
            except StopIteration:
                print("I break it")
                if len(lines):
                    print(f"Log lines left {len(lines)}")
                    result = use_loaders_for_lines(lines, device_memc, options)
                    Statistic["errors"] += result["errors"]
                    Statistic["processed"] += result["processed"]
                break

        print(f"Statistic {Statistic}")

        if not Statistic["processed"]:
            fd.close()
            dot_rename(fn)
            continue

        err_rate = float(Statistic["errors"]) / Statistic["processed"]
        if err_rate < NORMAL_ERR_RATE:
            logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
        fd.close()
        dot_rename(fn)


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
    op.add_option("--p_count", action="store", default=10)
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
        print(f"--- Total Time {(time.time() - start_time)} seconds ---")
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
