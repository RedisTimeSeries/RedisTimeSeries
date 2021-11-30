#!/usr/bin/env python3

import hiredis
import sys

def parse_file(path):
    def next_line(fp):
        line = fp.readline().strip()
        return line.strip() + "\r\n" if line else line

    with open(path) as fp:
        line = next_line(fp)
        cur_request = ""
        result = []

        while line:
            cur_request += line
            req_reader = hiredis.Reader()
            req_reader.setmaxbuf(0)
            req_reader.feed(cur_request)
            command = req_reader.gets()
            try:
                if command is not False:
                    result.append([item.decode('utf-8') for item in command])
                    cur_request = ''
            except hiredis.ProtocolError:
                raise
            line = next_line(fp)

        return result

if __name__ == '__main__':
    if len(sys.argv) != 2:
       print (sys.argv[0], 'AOF_file')
       sys.exit()

    print(parse_file(sys.argv[1]))
