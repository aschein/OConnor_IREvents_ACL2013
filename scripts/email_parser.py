import os
import re
from collections import defaultdict

DATA_DIR = '/Users/aaronschein/Documents/research/mlds/OConnor_IREvents_ACL2013/email_data/samples'
OUT_DIR = '/Users/aaronschein/Documents/research/mlds/OConnor_IREvents_ACL2013/email_data/samples'

class EmailParser:
    def __init__(self, data_dir, out_dir):
        self.data_dir = data_dir
        self.out_dir = out_dir
        self.entities = defaultdict(set) # key = email, value = set(known aliases)
        self.messages = {} # key = MID, value = (Timestamp, From, [To], Body)

    def recursive_walk(self, data_dir):
        for root, dirs, files in os.walk(data_dir):
            for f in files:
                yield os.path.join(root, f)

    def get_body(self, lines):
        reg = re.compile('on [0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}|^----')
        main_lines = []
        flag = False
        for idx, line in enumerate(lines):
            if 'X-FileName:' in line:
                flag = True
                continue
            if flag:
                if reg.search(line.strip()) is not None:
                    break
                main_lines.append(line.strip())
        assert flag
        return (' '.join(main_lines)).strip()

    def parse(self, data_dir=None):
        if data_dir is None:
            data_dir = self.data_dir

        for full_path in self.recursive_walk(data_dir):
            with open(full_path, 'r') as f:
                lines = f.readlines()
                # TODO: write a testcase for MID collision
                if 'Message-ID' not in lines[0]:
                    continue
                mi = lines[0].rstrip().split('Message-ID: ', 1)[1]
                if mi in self.messages:
                    print 'D00PZ!'
                    continue
                body = self.get_body(lines)
                if len(body) == 0:
                    continue
                ts = lines[1].rstrip().split('Date: ', 1)[1]
                fr = lines[2].rstrip().split('From: ', 1)[1]
                to = lines[3].rstrip().split('To: ', 1)[1].split(', ')
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
    e = EmailParser(DATA_DIR, OUT_DIR)
    e.parse()
    e.serialize()





