import queue
from typing import Optional
from multiprocessing import Queue
import threading
from dataclasses import dataclass
from threading import Event as EventTh
from atomic_counter import AtomicCounter
from sqlalchemy import Connection, Engine, Executable


@dataclass(frozen=True)
class Query4Execute:
    clause: Executable
    params: Optional[dict | list[dict]]


class QueryExecutor(threading.Thread):
    """
    Исполнитель запросов
    Сюда именно должна быть подключена очередь
    И потокобезопасный счётчик
    """

    def __init__(
        self,
        queriesQueue: "Queue[Query4Execute]",
        stopEvent: EventTh,
        engine: Engine,
        counter: AtomicCounter,
    ):
        super().__init__(daemon=True)
        self.stop_event: EventTh = stopEvent
        self.queue: "Queue[Query4Execute]" = queriesQueue
        self.engine = engine
        self.counter = counter
        self.init_flag: bool = False

    def init_executor(self):
        self.connection: Connection = self.engine.connect()
        self.init_flag = True

    def run(self):
        if not self.init_flag:
            print("error")
            raise RuntimeError("Executor not inited")
        while True:
            if self.stop_event.is_set():
                break
            try:
                job = self.queue.get(timeout=0.5)
                with self.connection.begin():
                    self.connection.execute(statement=job.clause, parameters=job.params)
                self.counter.increment()
            except queue.Empty:
                pass
            except Exception as e:
                print("sql exceptinon")
                raise e

        self.shutdown()

    def shutdown(self):
        self.connection.close()
