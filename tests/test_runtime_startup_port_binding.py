import time

import app.main as main


def test_startup_handler_does_not_wait_for_runtime_warmup(monkeypatch):
    worker_started = main.threading.Event()
    release_worker = main.threading.Event()

    def slow_warmup():
        worker_started.set()
        release_worker.wait(timeout=2)

    previous_thread = main._warmup_thread
    monkeypatch.setattr(main, "_warmup_thread", None)
    monkeypatch.setattr(main, "_warmup_vectra_runtime_sync", slow_warmup)

    started_at = time.monotonic()
    main.warmup_vectra_runtime()
    elapsed = time.monotonic() - started_at

    assert elapsed < 0.5
    assert worker_started.wait(timeout=0.5)
    assert main.get_vectra_warmup_state()["status"] == "STARTING"

    release_worker.set()
    main._warmup_thread.join(timeout=2)
    main._warmup_thread = previous_thread


def test_warmup_state_is_returned_as_a_copy():
    state = main.get_vectra_warmup_state()
    state["status"] = "MUTATED"

    assert main.get_vectra_warmup_state()["status"] != "MUTATED"
