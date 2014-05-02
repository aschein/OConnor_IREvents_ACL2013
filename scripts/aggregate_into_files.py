import os
import re
import time
from numpy import mean
from collections import defaultdict
import string

MSG_FILE='/mnt/nfs/work1/wallach/aschein/data/enron/preproc/v2.messages'
DYAD_FILE='/mnt/nfs/work1/wallach/aschein/data/enron/preproc/v2.dyadcounts'
OUT_DIR='/mnt/nfs/work1/wallach/aschein/data/enron/preproc/enron_directed/v2/dyad_files/raw'
# DYAD_VOCAB='/mnt/nfs/work1/wallach/aschein/data/enron/preproc/enron_directed/v2/dyad.vocab'

endings = ['.', '?', '!']

reg1 = re.compile(r'---+ Forward.*?---+')
reg2 = re.compile(r'---+ Forwarded by .*? on .*?=20')
reg3 = re.compile('-?Original Message-----')

empties = 0

if __name__ == '__main__':
    with open(DYAD_FILE, 'r') as f:
        lines = f.readlines()
    dyads = []
    for line in lines:
        e1, e2 = line.split('\t')[1:]
        dyads.append((e1.strip(), e2.strip()))

    assert len(dyads) == len(set(dyads))

    context_sents = defaultdict(str)

    with open(MSG_FILE, 'r') as f:
        lines = f.readlines()

    for line in lines:
        rel_path, mid, fr, to, msg = line.split('|')
        dyad = (fr.strip(), to.strip())
        if dyad not in dyads:
            print dyad
            raise
        dyad_idx = dyads.index(dyad)

        if reg1.search(msg) is not None:
            msg = re.split(reg1, msg)[0]

        elif reg2.search(msg) is not None:
            msg = re.split(reg2, msg)[0]

        elif reg3.search(msg) is not None:
            msg = re.split(reg3, msg)[0]

        msg = msg.strip()

        if len(msg) == 0:
            empties += 1
            continue

        if msg[-1] not in endings:
            msg += '.'

        context_sents[dyad_idx] += ' %s'%msg

    print len(context_sents)
    
    for context, msgs in context_sents.iteritems():
        # print context, dyads[int(context)]
        with open(os.path.join(OUT_DIR, '%d.sentences'%(int(context))), 'w+') as f:
            f.write(msgs)










