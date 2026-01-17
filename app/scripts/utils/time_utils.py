import time
from datetime import datetime


class TimeUtils:
    def inicio_contador_tempo(self):
        self.__start = time.perf_counter()

    def fim_contador_tempo(self):
        self.__end = time.perf_counter()

    def tempo_processamento_segundos(self):
        elapsed_seconds = self.__end - self.__start
        return elapsed_seconds

    def timestamp(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return timestamp
