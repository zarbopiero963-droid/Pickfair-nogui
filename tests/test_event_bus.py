import threading

from core.event_bus import EventBus


def test_subscribe_and_publish_calls_handler_once():
    bus = EventBus()
    received = []

    def handler(payload):
        received.append(payload)

    bus.subscribe("PING", handler)
    bus.publish("PING", {"x": 1})

    assert received == [{"x": 1}], "publish deve invocare il subscriber con il payload corretto"


def test_subscribe_does_not_duplicate_same_callback():
    bus = EventBus()
    calls = []

    def handler(payload):
        calls.append(payload)

    bus.subscribe("PING", handler)
    bus.subscribe("PING", handler)
    bus.publish("PING", 123)

    assert calls == [123], "lo stesso callback non deve essere registrato due volte"


def test_unsubscribe_removes_callback():
    bus = EventBus()
    calls = []

    def handler(payload):
        calls.append(payload)

    bus.subscribe("PING", handler)
    bus.unsubscribe("PING", handler)
    bus.publish("PING", 123)

    assert calls == [], "unsubscribe deve impedire future invocazioni del callback"


def test_publish_continues_after_subscriber_exception():
    bus = EventBus()
    calls = []

    def broken(_payload):
        raise RuntimeError("boom")

    def healthy(payload):
        calls.append(payload)

    bus.subscribe("PING", broken)
    bus.subscribe("PING", healthy)

    bus.publish("PING", "ok")

    assert calls == ["ok"], "un subscriber che fallisce non deve bloccare gli altri subscriber"


def test_publish_uses_copy_of_subscribers_to_avoid_mutation_issues():
    bus = EventBus()
    calls = []

    def second(payload):
        calls.append(("second", payload))

    def first(payload):
        calls.append(("first", payload))
        bus.unsubscribe("PING", second)

    bus.subscribe("PING", first)
    bus.subscribe("PING", second)

    bus.publish("PING", "data")

    assert calls == [
        ("first", "data"),
        ("second", "data"),
    ], "publish deve lavorare su una copia dei subscriber per evitare effetti collaterali durante l'iterazione"


def test_thread_safe_subscribe_from_multiple_threads():
    bus = EventBus()
    calls = []

    def make_handler(i):
        def _handler(payload):
            calls.append((i, payload))
        return _handler

    threads = []
    for i in range(20):
        t = threading.Thread(target=bus.subscribe, args=("PING", make_handler(i)))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    bus.publish("PING", "ok")

    assert len(calls) == 20, "subscribe concorrente non deve perdere subscriber"