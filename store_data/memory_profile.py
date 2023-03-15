import linecache
import os
import time
import tracemalloc
import psutil
from datetime import datetime
from queue import Queue, Empty
from threading import Thread


class MemoryProfile:
    """"""

    def __init__(self):
        """"""

    @staticmethod
    def memory_monitor(command_queue: Queue, poll_interval=0.1, alpha=.9):
        """"""
        tracemalloc.start()

        max_rss = 0
        curr_rss = 0
        snapshot = None

        while True:

            try:
                command_queue.get(timeout=poll_interval)
                if snapshot is not None:
                    print(datetime.now())
                    MemoryProfile.display_top(snapshot)

                return

            except Empty:
                process = psutil.Process(os.getpid())
                rss = process.memory_info().rss
                # rss = psutil.virtual_memory()[3]

                if rss > max_rss:
                    max_rss = rss
                    curr_rss = rss
                    snapshot = tracemalloc.take_snapshot()
                    msg = f"curr RSS {curr_rss / (1024 * 1024):,.0f} MiB max RSS {max_rss / (1024 * 1024):,.0f} MiB"
                    print(datetime.now(), msg)

                if rss < alpha * curr_rss:
                    curr_rss = rss
                    snapshot = tracemalloc.take_snapshot()
                    msg = f"curr RSS {curr_rss / (1024 * 1024):,.0f} MiB max RSS {max_rss / (1024 * 1024):,.0f} MiB"
                    print(datetime.now(), msg)

    @staticmethod
    def display_top(snapshot, key_type='lineno', limit=3):
        """"""
        snapshot = snapshot.filter_traces((
            tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
            tracemalloc.Filter(False, "<unknown>"),
        ))
        top_stats = snapshot.statistics(key_type)

        print("Top %s lines" % limit)
        for index, stat in enumerate(top_stats[:limit], 1):
            frame = stat.traceback[0]
            # replace "/path/to/module/file.py" with "module/file.py"
            filename = os.sep.join(frame.filename.split(os.sep)[-2:])
            print("#%s: %s:%s: %.1f KiB"
                  % (index, filename, frame.lineno, stat.size / 1024))
            line = linecache.getline(frame.filename, frame.lineno).strip()
            if line:
                print('    %s' % line)

        other = top_stats[limit:]
        if other:
            size = sum(stat.size for stat in other)
            print(f"{len(other)} other: {(size / 1024):,.1f} KiB")
        total = sum(stat.size for stat in top_stats)
        print(f"Total allocated size: {(total / 1024):,.1f} KiB")

    @staticmethod
    def profile(poll_interval=0.1, start_sleep=0, end_sleep=0, alpha=.9):
        """"""

        def wrapper(f):
            """"""

            def inner(*args, **kwargs):
                """"""

                print(f"START {f.__name__}")

                queue = Queue()
                monitor_thread = Thread(target=MemoryProfile.memory_monitor, args=(queue, poll_interval, alpha))
                monitor_thread.start()

                time.sleep(start_sleep)

                try:
                    t0 = time.time()
                    res = f(*args, **kwargs)
                    t1 = time.time()

                finally:
                    time.sleep(end_sleep)
                    queue.put('stop')
                    monitor_thread.join()

                print(f"END {f.__name__} - done in {t1 - t0:,.2f}s")

                return res

            return inner

        return wrapper

    @staticmethod
    def timer(start_sleep=0, end_sleep=0):
        """"""

        def wrapper(f):
            """"""

            def inner(*args, **kwargs):
                """"""

                print(f"START {f.__name__}")

                time.sleep(start_sleep)

                t0 = time.time()
                res = f(*args, **kwargs)
                t1 = time.time()

                time.sleep(end_sleep)

                if t1 - t0 < 1e-8:
                    str_dt = f"{(t1 - t0) * 1e9:,.1f} ns"
                elif t1 - t0 < 1e-5:
                    str_dt = f"{(t1 - t0) * 1e6:,.1f} µs"
                elif t1 - t0 < 1e-2:
                    str_dt = f"{(t1 - t0) * 1e3:,.1f} ms"
                else:
                    str_dt = f"{t1 - t0:,.1f} s"

                print(f"END {f.__name__} - done in {str_dt}")

                return res

            return inner

        return wrapper
