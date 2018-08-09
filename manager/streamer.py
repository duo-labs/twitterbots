import json
import gzip
import os

DEFAULT_BUFFER_SIZE = 16 * 1024


class BaseStreamer(object):
    """Base class for implementing a streamer
    """

    def write_row(self, row):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError


class StdoutStreamer(BaseStreamer):
    """A generic streamer that streams ndjson entries to stdout
    """

    def __init__(self, filename):
        pass

    def write_row(self, row):
        record = json.dumps(row)
        print(record)

    def close(self):
        pass


class JSONStreamer(BaseStreamer):
    """
    A generic class for streaming JSON entries to a compressed file as ndjson.
    """

    def __init__(self, filename):
        """Creates a new instance of the streamer.

        If the file specfied by {filename} already exists, we will append to
        the file. Otherwise a new file is created.

        Arguments:
            filename {str} -- The filename to stream ndjson results to
        """
        filemode = 'w'
        if os.path.exists(filename):
            filemode = 'a'
        self.file_handle = gzip.open(filename, filemode)

    def write_row(self, row):
        """Writes a new row to the file in ndjson format

        Arguments:
            row {dict} -- The dict to write to the file
        """
        record = '{}\n'.format(json.dumps(row))
        record = record.encode('utf-8')
        self.file_handle.write(record)

    def flush(self):
        self.file_handle.flush()

    def close(self):
        self.file_handle.close()
