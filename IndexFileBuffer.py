import pickle
import struct

class IndexFileBuffer:
    def __init__(self, filename, blocksize):
        self.filename = filename
        self.file = open(filename, "rb")
        self.blocksize = blocksize
        self.buffer = b''
        self.end_of_file = False
        self.load()
        self.header_size = struct.calcsize("II")

    def load(self):
        if self.end_of_file:
            return 0
        next_block = self.file.read(self.blocksize)
        if not next_block:
            self.end_of_file = True
            return 0
        self.buffer += next_block
        return len(next_block)
    
    def next(self):
        while True:
            if self.end_of_file and len(self.buffer) < self.header_size:
                return None
            if len(self.buffer) < self.header_size:
                if not self.load():
                    return None
                continue
            # first part of the data is the header "II"
            # first I is the size of the term
            # second I is the size of postings list
            header = self.buffer[:self.header_size]
            term_size, postings_size = struct.unpack("II", header)
            index_record_size = self.header_size + term_size + postings_size
            if len(self.buffer) < index_record_size:
                remaining_record = index_record_size - len(self.buffer)
                allowed_size = self.blocksize - remaining_record
                read_amount = max(allowed_size, self.blocksize)
                new_data = self.file.read(read_amount)
                if not new_data:
                    self.end_of_file = True
                    return None
                self.buffer += new_data
                continue
            term_in_bytes = self.buffer[self.header_size:self.header_size+term_size]
            postings_in_bytes = self.buffer[self.header_size+term_size:self.header_size+term_size+postings_size]
            self.buffer = self.buffer[index_record_size:]
            term = term_in_bytes.decode("utf-8")
            postings = pickle.loads(postings_in_bytes)
            return term, postings

    def close(self):
        self.file.close()

