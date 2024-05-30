# multiprocess_queue.py

import argparse
import multiprocessing
import time
import queue
import json
from multiprocessing import Queue
from threading import Event as EventTh, Thread
from datetime import datetime, timedelta
from typing import Callable, Iterator


from numpy._typing import NDArray
from sqlalchemy import text
import random
import numpy as np

from executor import Query4Execute
from worker import Worker

activity_log_ids = np.array([])
devices_client_ids = np.array([])
hashes = np.array([])
activity_log_statuses: list[str] = ["wait", "process", "done"]
block_statuses: list[int] = [1, 2, 3, 6, 7]
update_at_max: datetime = datetime.strptime("2024-02-06 17:06:01", "%Y-%m-%d %H:%M:%S")


def wait_thread_for_lock(event: EventTh, interval: int):
    time.sleep(interval)
    event.set()


def split_into_equal_parts(total, n):
    base_value = total // n
    remainder = total % n
    result = [base_value + 1 if i < remainder else base_value for i in range(n)]
    return result


def gen1query() -> Query4Execute:
    ttl_delta = random.randint(1, 100)
    id_index = random.randint(0, len(activity_log_ids) - 1)
    status_index = random.randint(0, len(activity_log_statuses) - 1)
    smth = text(
        'update "activityLog" set status = :status, ttl = ttl + :ttl_delta where id = :id;'
    )
    params = {
        "status": activity_log_statuses[status_index],
        "ttl_delta": ttl_delta,
        "id": int(activity_log_ids[id_index]),
    }
    query = Query4Execute(clause=smth, params=params)
    return query


def gen2query() -> Query4Execute:
    client_id_index = random.randint(0, len(devices_client_ids) - 1)
    hash_index = random.randint(0, len(hashes) - 1)
    smth = text(
        'insert into "appVersions" ("appId", version, hash) values (:clientId, \'1\', :hash) \
        on conflict ("appId", version) \
        do update set version=excluded.version, hash=excluded.hash;'
    )
    params = {
        "hash": str(hashes[hash_index]),
        "clientId": str(devices_client_ids[client_id_index]),
    }
    query = Query4Execute(clause=smth, params=params)
    return query


def gen3query() -> Query4Execute:
    client_id_index = random.randint(0, len(devices_client_ids) - 1)
    status_index = random.randint(0, len(block_statuses) - 1)
    smth = text(
        'select id, "paramId", url from "blockPageRequests" where "clientId"=:clientId and status = :status;'
    )
    params = {
        "status": int(block_statuses[status_index]),
        "clientId": str(devices_client_ids[client_id_index]),
    }
    query = Query4Execute(clause=smth, params=params)
    return query


def gen4query() -> Query4Execute:
    # нужно чтобы не всегда создавать новые профили
    # 10 процентов должны быть новыми
    if random.randint(0, 100) < 5:
        hash = random.randbytes(20).hex()
    else:
        rand_hash_ind = random.randint(0, len(hashes) - 1)
        hash = hashes[rand_hash_ind]
    profile = {
        "1": random.randbytes(20).hex(),
        "2": random.randbytes(20).hex(),
        "3": random.randbytes(20).hex(),
    }
    smth = text(
        'insert into "profileHashes" (hash, profile) \
        values (:hash, :profile) \
        on conflict (hash) do update set profile=excluded.profile;'
    )
    params = {
        "hash": hash,
        "profile": json.dumps(profile, ensure_ascii=False),
    }
    query = Query4Execute(clause=smth, params=params)
    return query


def gen5query() -> Query4Execute:
    client_id_index = random.randint(0, len(devices_client_ids) - 1)
    rand_user_id = random.randint(0, 1000)
    smth = text(
        'select "deviceName" from devices \
        where "clientId" = :clientId and "userId"=:userId;'
    )
    params = {
        "userId": rand_user_id,
        "clientId": str(devices_client_ids[client_id_index]),
    }
    query = Query4Execute(clause=smth, params=params)
    return query


def gen6query() -> Query4Execute:
    count_days = timedelta(days=random.randint(0, 600))
    update_at = update_at_max - count_days
    smth = text('select extra from "cacheSites" where "updatedAt" > :updatedAt;')
    params = {
        "updatedAt": update_at.strftime("%Y-%m-%d %H:%M:%S"),
    }
    query = Query4Execute(clause=smth, params=params)
    return query


def gen7query() -> Query4Execute:
    client_id_index = random.randint(0, len(devices_client_ids) - 1)
    if random.randint(0, 100) < 5:
        hash = random.randbytes(20).hex()
    else:
        rand_hash_ind = random.randint(0, len(hashes) - 1)
        hash = hashes[rand_hash_ind]
    pages = {
        "1": random.randbytes(20).hex(),
        "2": random.randbytes(20).hex(),
        "3": random.randbytes(20).hex(),
    }
    smth = text(
        'insert into "filterReport" \
                ("userId","clientId", "startTimestamp", "endTimestamp",\
                site, pages, action, "processId", "processName", "profileHash")  \
                values (:userId, :clientId, :startTimestamp, :endTimestamp,\
                \'www.google.com\', :pages, \'allowed\', :processId, \'chrome\', :hash);'
    )
    params = {
        "userId": random.randint(0, 10000),
        "startTimestamp": int(time.time()),
        "endTimestamp": int(time.time()),
        "clientId": str(devices_client_ids[client_id_index]),
        "pages": json.dumps(pages, ensure_ascii=False),
        "processId": random.randint(0, 10000),
        "hash": hash,
    }
    query = Query4Execute(clause=smth, params=params)
    return query


def gen8query() -> Query4Execute:
    client_id_index = random.randint(0, len(devices_client_ids) - 1)
    if random.randint(0, 100) < 5:
        hash = random.randbytes(20).hex()
    else:
        rand_hash_ind = random.randint(0, len(hashes) - 1)
        hash = hashes[rand_hash_ind]
    info = {
        "1": random.randbytes(20).hex(),
        "2": random.randbytes(20).hex(),
        "3": random.randbytes(20).hex(),
    }
    smth = text(
        'insert into "activityLog" \
            ("userId", "clientId", "logTimestamp", "processId",\
            path, "fileHash", info, action, "profileHash")\
            values (:userId, :clientId, :logTimestamp, \
            :processId, \'filedownload.lenovo.com\', \
            \'hash\', :info, \'blocked\', :hash);'
    )
    params = {
        "userId": random.randint(0, 100000),
        "clientId": str(devices_client_ids[client_id_index]),
        "logTimestamp": int(time.time()),
        "processId": random.randint(0, 10000),
        "info": json.dumps(info, ensure_ascii=False),
        "hash": hash,
    }
    query = Query4Execute(clause=smth, params=params)
    return query


def gen9query() -> Query4Execute:
    ttl = random.randint(1, 100)
    limit = random.randint(1, 100)
    status_index = random.randint(0, len(activity_log_statuses) - 1)
    smth = text(
        'select * from "activityLog" \
        where status = :status and ttl > 1 order by "logTimestamp" limit :limit;'
    )
    params = {
        "ttl": ttl,
        "limit": limit,
        "status": activity_log_statuses[status_index],
    }
    query = Query4Execute(clause=smth, params=params)
    return query


def random_queries_ids() -> NDArray:
    query_inexes = []
    query_inexes += [0] * 5
    query_inexes += [1] * 5
    query_inexes += [2] * 37
    query_inexes += [3] * 83
    query_inexes += [4] * 195
    query_inexes += [5] * 195
    query_inexes += [6] * 195
    query_inexes += [7] * 4642
    query_inexes += [8] * 4645
    return np.array(query_inexes)


def gen_query() -> Iterator[Query4Execute]:
    rng = np.random.default_rng()
    query_functions: list[Callable[..., Query4Execute]] = [
        gen1query,
        gen2query,
        gen3query,
        gen4query,
        gen5query,
        gen6query,
        gen7query,
        gen8query,
        gen9query,
    ]
    while True:
        rand_ids_array = random_queries_ids()
        rng.shuffle(rand_ids_array)
        for i in range(rand_ids_array):
            func = query_functions[i]
            yield func()


def main(args):
    global activity_log_ids, devices_client_ids, hashes
    activity_log_ids = np.load("activityLog_ids.npy")
    devices_client_ids = np.load("devices_clientIds.npy")
    hashes = np.load("profileHashes.npy")
    queue_in: Queue[Query4Execute] = Queue(maxsize=100000)
    queue_out: Queue[int] = Queue()

    events = [multiprocessing.Event() for _ in range(args.num_workers)]
    stopEvent = multiprocessing.Event()
    conn_str = "postgresql+psycopg2://root:mypassword@localhost/eticum_app"

    def report() -> int:
        for event in events:
            event.set()
        all_counts = 0
        for _ in range(args.num_workers):
            try:
                count = queue_out.get(timeout=0.1)
                all_counts += count
            except queue.Empty:
                print("queue Empty")
        return all_counts

    def stop():
        stopEvent.set()

    list_counts = split_into_equal_parts(args.connections, args.num_workers)
    workers = [
        Worker(
            queue_in=queue_in,
            queue_out=queue_out,
            report_event=events[i],
            stopEvent=stopEvent,
            number=i,
            connect_string=conn_str,
            count_c=connections,
        )
        for i, connections in enumerate(list_counts)
    ]

    for worker in workers:
        worker.init_worker()
        worker.start()

    exit_event = EventTh()
    print_event = EventTh()

    exit_waiter = Thread(
        target=wait_thread_for_lock, kwargs={"event": exit_event, "interval": args.time}
    )

    print_waiter = Thread(
        target=wait_thread_for_lock,
        kwargs={"event": print_event, "interval": args.print},
    )
    exit_waiter.start()
    print_waiter.start()

    full_counts = 0
    query_generator = gen_query()
    while not exit_event.is_set():
        try:
            queue_in.put(
                next(query_generator),
                timeout=1,
            )
        except queue.Full:
            pass

        if print_event.is_set():
            counts = report()
            full_counts += counts
            print(
                f"all_counts = {counts} by interval: {args.print}: tps: {counts/args.print}"
            )
            print_event.clear()
            print_waiter = Thread(
                target=wait_thread_for_lock,
                kwargs={"event": print_event, "interval": args.print},
            )
            print_waiter.start()

    stop()
    for worker in workers:
        worker.join()

    counts = report()
    print(f"all_counts = {counts} by interval: {args.print}")
    full_counts += counts
    print(
        f"full_counts = {full_counts}: by interval: {args.time}: tps: {full_counts/args.time}"
    )

    print(f"in queue {queue_in.qsize()}")
    print(f"out queue {queue_out.qsize()}")
    print("workers alive")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--time", type=int, default=30)
    parser.add_argument("-P", "--print", type=int, default=10)
    parser.add_argument(
        "-w",
        "--num-workers",
        type=int,
        default=multiprocessing.cpu_count(),
    )
    parser.add_argument(
        "-c",
        "--connections",
        type=int,
        default=50,
    )
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_args())
