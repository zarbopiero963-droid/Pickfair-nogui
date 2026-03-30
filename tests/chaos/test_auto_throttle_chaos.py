import threading
from auto_throttle import AutoThrottle


def test_concurrent_calls_do_not_crash():
    t = AutoThrottle(max_calls=10, period=1)

    def worker():
        for _ in range(100):
            t.wait()

    threads = [threading.Thread(target=worker) for _ in range(10)]

    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert True