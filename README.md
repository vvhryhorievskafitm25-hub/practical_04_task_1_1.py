#practical_04_all.py
"""
Практична робота №4: Мультипоточні та багатопроцесні додатки в Python
Виконано студенткою групи КБ-25: Григор'євська
"""

import threading
import multiprocessing
import queue
import time
import random
import os
import hashlib
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed


# =====================================================================
# ЧАСТИНА 1. Завдання 1.1: Модуль threading та демон-потоки
# =====================================================================
def download_simulation(file_name: str, size_mb: float) -> dict:
    thread_name = threading.current_thread().name
    print(f"[{thread_name}] Початок завантаження '{file_name}' ({size_mb} MB)...")
    time.sleep(size_mb * 0.3)
    print(f"[{thread_name}] Завершено завантаження '{file_name}'.")
    return {"file": file_name, "size": size_mb, "thread": thread_name}


class MonitorThread(threading.Thread):
    def __init__(self, interval: float = 1.0):
        super().__init__(name="Monitor", daemon=True)
        self.interval = interval
        self._stop_event = threading.Event()

    def run(self):
        while not self._stop_event.is_set():
            print(f"--- [MONITOR] Активних потоків: {threading.active_count()} ---")
            self._stop_event.wait(self.interval)

    def stop(self):
        self._stop_event.set()


def task_1_1():
    print("\n--- Завдання 1.1: Управління потоками (threading) ---")
    files = [
        ("data.csv", 2.0),
        ("report.pdf", 1.0),
        ("image.png", 0.5),
        ("archive.zip", 3.0),
    ]

    monitor = MonitorThread(interval=0.4)
    monitor.start()

    threads = []
    for name, size in files:
        t = threading.Thread(target=download_simulation, args=(name, size), name=f"Worker-{name}")
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    monitor.stop()
    monitor.join()
    print("Усі потоки завантаження успішно завершено.")


# =====================================================================
# ЧАСТИНА 1. Завдання 1.2: Модуль multiprocessing та CPU-bound задачі
# =====================================================================
def is_prime(n: int) -> bool:
    if n < 2: return False
    if n < 4: return True
    if n % 2 == 0 or n % 3 == 0: return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0: return False
        i += 6
    return True


def sum_primes_in_range(start: int, end: int) -> tuple:
    pid = os.getpid()
    total_sum = sum(num for num in range(start, end) if is_prime(num))
    print(f"[PID {pid}] Оброблено діапазон [{start}, {end})")
    return (total_sum, pid)


def task_1_2():
    print("\n--- Завдання 1.2: Паралельні процеси (multiprocessing) ---")
    total_range = 300_000
    num_workers = 4
    chunk = total_range // num_workers
    ranges = [(i * chunk, (i + 1) * chunk if i < num_workers - 1 else total_range) for i in range(num_workers)]

    start = time.perf_counter()
    with multiprocessing.Pool(processes=num_workers) as pool:
        results = pool.starmap(sum_primes_in_range, ranges)
    elapsed = time.perf_counter() - start

    total_sum = sum(res[0] for res in results)
    print(f"Сума простих чисел: {total_sum}, час виконання: {elapsed:.3f} сек")


# =====================================================================
# ЧАСТИНА 2. Завдання 2.1: Гонка даних та Lock
# =====================================================================
class SafeBankAccount:
    def __init__(self, balance: float = 1000.0):
        self.balance = balance
        self._lock = threading.Lock()

    def deposit(self, amount: float):
        with self._lock:
            curr = self.balance
            time.sleep(0.0001)
            self.balance = curr + amount

    def withdraw(self, amount: float):
        with self._lock:
            curr = self.balance
            time.sleep(0.0001)
            if curr >= amount:
                self.balance = curr - amount


def task_2_1():
    print("\n--- Завдання 2.1: Захист спільного ресурсу через Lock ---")
    account = SafeBankAccount(1000.0)
    threads = []
    for _ in range(50):
        t1 = threading.Thread(target=account.deposit, args=(10.0,))
        t2 = threading.Thread(target=account.withdraw, args=(10.0,))
        threads.extend([t1, t2])
        t1.start(); t2.start()

    for t in threads: t.join()
    print(f"Баланс рахунку після 100 транзакцій: {account.balance:.2f} (очікувано: 1000.00)")


# =====================================================================
# ЧАСТИНА 2. Завдання 2.2: Семафори та Event
# =====================================================================
class ConnectionPool:
    def __init__(self, max_conn: int = 2):
        self._sem = threading.Semaphore(max_conn)
        self._lock = threading.Lock()
        self.active = 0
        self.peak = 0

    def access(self, name: str):
        self._sem.acquire()
        with self._lock:
            self.active += 1
            if self.active > self.peak: self.peak = self.active
        print(f"[{name}] Отримав з'єднання (активних: {self.active})")
        time.sleep(0.3)
        with self._lock:
            self.active -= 1
        print(f"[{name}] Звільнив з'єднання")
        self._sem.release()


def client_thread(pool, name, event):
    event.wait()
    pool.access(name)


def task_2_2():
    print("\n--- Завдання 2.2: Семафори та координація Event ---")
    pool = ConnectionPool(max_conn=2)
    event = threading.Event()
    threads = [threading.Thread(target=client_thread, args=(pool, f"Client-{i}", event)) for i in range(5)]
    for t in threads: t.start()
    
    time.sleep(0.2)
    print(">>> Старт клієнтів!")
    event.set()
    for t in threads: t.join()
    print(f"Пікове навантаження пулу: {pool.peak}")


# =====================================================================
# ЧАСТИНА 3. Завдання 3.1: concurrent.futures та виключення
# =====================================================================
def risky_task(val: int) -> float:
    if val == 0: raise ZeroDivisionError("Нуль!")
    return 100.0 / val


def task_3_1():
    print("\n--- Завдання 3.1: concurrent.futures та обробка виключень ---")
    values = [5, 2, 0, 4]
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(risky_task, v): v for v in values}
        for fut in as_completed(futures):
            v = futures[fut]
            try:
                print(f"Значення {v} -> Результат: {fut.result():.2f}")
            except Exception as e:
                print(f"Значення {v} -> Помилка: {e}")


# =====================================================================
# ЧАСТИНА 3. Завдання 3.2: Черги (Producer-Consumer) та дедлоки
# =====================================================================
def producer(q: queue.Queue, count: int):
    for i in range(count):
        q.put(f"Задача #{i}")
        time.sleep(0.05)


def consumer(q: queue.Queue):
    while True:
        try:
            item = q.get(timeout=1.0)
        except queue.Empty:
            break
        if item is None:
            q.put(None)
            break
        time.sleep(0.1)
        q.task_done()


def task_3_2():
    print("\n--- Завдання 3.2: Producer-Consumer та запобігання дедлоку ---")
    q = queue.Queue(maxsize=3)
    p = threading.Thread(target=producer, args=(q, 5))
    c = threading.Thread(target=consumer, args=(q,))
    p.start(); c.start()
    p.join()
    q.put(None)
    c.join()
    print("Чергу успішно оброблено без зависань.")


# =====================================================================
# ГОЛОВНЕ МЕНЮ ПРОГРАМИ
# =====================================================================
def main():
    while True:
        print("\n" + "=" * 65)
        print(" ГОЛОВНЕ МЕНЮ — ПРАКТИЧНА РОБОТА №4 (Григор'євська, КБ-25)")
        print("=" * 65)
        print(" 1. Завдання 1.1 (Управління потоками threading)")
        print(" 2. Завдання 1.2 (Паралельні процеси multiprocessing)")
        print(" 3. Завдання 2.1 (Захист через Lock)")
        print(" 4. Завдання 2.2 (Семафори та Event)")
        print(" 5. Завдання 3.1 (concurrent.futures та виключення)")
        print(" 6. Завдання 3.2 (Producer-Consumer та черги)")
        print(" 0. Вихід з програми")
        print("=" * 65)

        choice = input(" Оберіть номер завдання (0-6): ").strip()

        if choice == '1': task_1_1()
        elif choice == '2': task_1_2()
        elif choice == '3': task_2_1()
        elif choice == '4': task_2_2()
        elif choice == '5': task_3_1()
        elif choice == '6': task_3_2()
        elif choice == '0':
            print("\nРоботу програми завершено. До побачення!")
            break
        else:
            print("\nНевірний вибір! Введіть число від 0 до 6.")


if __name__ == "__main__":
    main()
