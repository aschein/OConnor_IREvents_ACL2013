import os
import re
from collections import defaultdict
import logging

# DATA_DIR = '/Users/aaronschein/Documents/research/mlds/OConnor_IREvents_ACL2013/email_data/samples'
# OUT_DIR = '/Users/aaronschein/Documents/research/mlds/OConnor_IREvents_ACL2013/email_data/samples'

def nonblank_lines(f):
    for l in f:
        line = l.rstrip()
        if line:
            yield line

class EmailParser:
    def __init__(self, data_dir, out_dir):
        self.data_dir = data_dir
        self.out_dir = out_dir
        self.entities = defaultdict(set) # key = email, value = set(known aliases)
        self.messages = {} # key = MID, value = (Timestamp, From, [To], Body)
        self.file_count = 0

        self.logger = logging.getLogger('email_parser')
        hdlr = logging.FileHandler(os.path.join(out_dir, 'email_parser.log'))
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        self.logger.addHandler(hdlr)
        self.logger.setLevel(logging.WARNING)
        self.logger.info('Initialized.')

    def recursive_walk(self, data_dir):
        for root, dirs, files in os.walk(data_dir):
            if dirs and not sum(['sent' in x for x in dirs]):
                self.logger.warning('No sent folder found in:\n\t\t\t%s'%root)
            if 'sent' not in root:
                continue    
            for f in files:
                self.file_count += 1
                yield os.path.join(root, f)

    def get_body(self, lines):
        reg_1 = re.compile('on [0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}|^----')
        reg_2 = re.compile('[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2} [A-P]{1}M')
        reg_3 = re.compile('^To:|^Sent by|=09')
        main_lines = []
        flag = False
        for idx, line in enumerate(lines):
            if 'X-FileName:' in line:
                flag = True
                continue
            if flag:
                if reg_1.search(line.strip()) is not None:
                    sep = '\n-----------------\n'
                    self.logger.info('Found thread text:%s%s%s'%(sep, ' '.join(lines), sep))
                    break
                main_lines.append(line.strip())
                if reg_2.search(line.strip()) is not None:
                    try:
                        if reg_3.search(lines[idx + 1].strip()) is not None:
                            main_lines.pop()
                            # print 'DELETING: %s'%main_lines.pop()
                            sep = '\n-----------------\n'
                            self.logger.warning('Found TRICKY thread text:%s%s%s'%(sep, '\n'.join(lines), sep))
                            self.logger.warning('Printed:%s%s%s'%(sep, ' '.join(main_lines), sep))
                            break
                        else:
                            sep = '-----------------'
                            print sep
                            print lines[idx - 1]
                            print lines[idx]
                            print lines[idx + 1]
                            print lines[idx + 2]
                            print lines[idx + 3]
                            print sep
                    except IndexError:
                        pass
        assert flag
        return (' '.join(main_lines)).strip()

    def parse(self, data_dir=None):
        if data_dir is None:
            data_dir = self.data_dir

        for full_path in self.recursive_walk(data_dir):
            with open(full_path, 'r') as f:
                lines = list(nonblank_lines(f))
                if 'Message-ID' not in lines[0]:
                    self.logger.warning('MID not found in:\n\t\t\t%s'%full_path)
                    continue
                mi = lines[0].rstrip().split('Message-ID: ', 1)[1]
                if mi in self.messages:
                    self.logger.info('Found duplicated MID in:\n\t\t\t%s'%full_path)
                    continue
                body = self.get_body(lines)
                if len(body) == 0:
                    continue
                try:
                    ts = lines[1].rstrip().split('Date: ', 1)[1]
                    fr = lines[2].rstrip().split('From: ', 1)[1]
                    to = lines[3].rstrip().split('To: ', 1)[1].split(', ')
                except IndexError:
                    sep = '\n-----------------\n'
                    self.logger.error('Unexpected header lines:\n\t\t\t%s\n%s%s%s'%(full_path, sep, ' '.join(lines[:20]), sep))
                    continue
                self.messages[mi] = (ts, fr, to, body)

                for line in lines:
                    if 'X-From' in line:
                        alias = line.rstrip().split('X-From: ', 1)[1]
                        self.entities[fr].add(alias)
                        break

    def serialize(self, out_dir=None):
        if out_dir is None:
            out_dir = self.out_dir

        with open(os.path.join(out_dir, 'entities.tsv'), 'w+') as f:
            f.write('EMAIL|ALIASES\n')
            for email, aliases in self.entities.iteritems():
                f.write('%s|%s\n'%(email, '|'.join(aliases)))

        with open(os.path.join(out_dir, 'messages.txt'), 'w+') as f:
            f.write('MID|TIME|FROM|TO|BODY\n')
            for mid, (ts, fr, to, body) in self.messages.iteritems():
                f.write('%s|%s|%s|%s|%s\n'%(mid, ts, fr, to, body))

if __name__ == '__main__':
    import time
    from argparse import ArgumentParser
    from path import path

    p = ArgumentParser()
    p.add_argument('--data', type=path, required=True,
                   help='directory to search recursively within for email files')
    p.add_argument('--out', type=path, required=True,
                   help='directory to serialize results to')

    args = p.parse_args()

    start = time.time()
    e = EmailParser(args.data, args.out)
    e.parse()
    e.serialize()
    end = time.time() - start
    e.logger.info('Completed job for %d files in %f secs.'%(e.file_count, end))
    print 'Completed job for %d files in %f secs.'%(e.file_count, end)





