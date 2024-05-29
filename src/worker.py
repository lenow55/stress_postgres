import multiprocessing
from multiprocessing import Queue
from multiprocessing.synchronize import Event as EventMP
from threading import Event as EventTh
from sqlalchemy import Engine, create_engine
from atomic_counter import AtomicCounter
from executor import QueryExecutor, Query4Execute


class Worker(multiprocessing.Process):
    def __init__(
        self,
        queue_in: "Queue[Query4Execute]",
        queue_out: "Queue[int]",
        report_event: EventMP,
        stopEvent: EventMP,
        number,
        connect_string: str,
        count_c: int,
    ):
        super().__init__(daemon=True)
        self.number: int = number

        self.queue_in: "Queue[Query4Execute]" = queue_in
        self.queue_out: "Queue[int]" = queue_out

        self.report_event: EventMP = report_event
        self.stop_event: EventMP = stopEvent
        self.counter = AtomicCounter()

        self.connect_string: str = connect_string
        self.connections_per_proc: int = count_c
        self.init_flag: bool = False

    def init_worker(self):
        self.engine: Engine = create_engine(
            self.connect_string, pool_size=self.connections_per_proc, max_overflow=0
        )
        self.stop_thread_event = EventTh()
        self.threads_pool = [
            QueryExecutor(
                self.queue_in, self.stop_thread_event, self.engine, self.counter
            )
            for _ in range(self.connections_per_proc)
        ]
        for thread in self.threads_pool:
            thread.init_executor()
        self.init_flag = True
        print("inited")

    def run(self):
        if not self.init_flag:
            print("error")
            raise RuntimeError("Worker not inited")

        for thread in self.threads_pool:
            thread.start()
        while True:
            if self.stop_event.is_set():
                self.queue_out.put(self.counter.get_flush(), timeout=0.1)
                break
            if self.report_event.wait(5):
                self.queue_out.put(self.counter.get_flush(), timeout=0.1)
                self.report_event.clear()

        self.shutdown()

    def shutdown(self):
        self.stop_thread_event.set()
        for thread in self.threads_pool:
            try:
                try:
                    thread.join(60.0)
                except RuntimeError:
                    thread.shutdown()
            except Exception as e:
                print(e)
        self.engine.dispose()
