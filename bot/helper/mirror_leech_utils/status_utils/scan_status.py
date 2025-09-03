import time
from bot.helper.ext_utils.bot_utils import MirrorStatus, get_readable_time, get_readable_file_size

class ScanStatus:
    def __init__(self, message, total, processed, start_time):
        self.__message = message
        self.__total = total
        self.__processed = processed
        self.__start_time = start_time
        self.__uid = message.id
        self.is_cancelled = False

    def gid(self):
        return self.__uid

    def progress_raw(self):
        try:
            return (self.__processed / self.__total) * 100
        except ZeroDivisionError:
            return 0

    def progress(self):
        return f'{round(self.progress_raw(), 2)}%'

    def speed(self):
        elapsed_time = time.time() - self.__start_time
        if elapsed_time == 0:
            return '0 msg/s'
        messages_per_second = self.__processed / elapsed_time
        return f'{round(messages_per_second, 2)} msg/s'

    def name(self):
        return "Channel Scan"

    def size(self):
        return f"{self.__processed} of {self.__total}"

    def eta(self):
        if self.progress_raw() > 0:
            elapsed_time = time.time() - self.__start_time
            remaining_messages = self.__total - self.__processed
            if self.__processed == 0:
                return '-'
            seconds_per_message = elapsed_time / self.__processed
            eta_seconds = remaining_messages * seconds_per_message
            return get_readable_time(eta_seconds)
        return '-'

    def status(self):
        return MirrorStatus.STATUS_DOWNLOADING # Using downloading status for visual representation

    def processed_bytes(self):
        # We can use this to represent processed messages for the progress bar
        return self.__processed

    def total_bytes(self):
        # We can use this to represent total messages for the progress bar
        return self.__total
        
    def message(self):
        return self.__message
        
    def cancel(self):
        self.is_cancelled = True

    @property
    def processed(self):
        return self.__processed

    @processed.setter
    def processed(self, value):
        self.__processed = value
