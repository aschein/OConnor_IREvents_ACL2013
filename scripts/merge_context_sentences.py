from collections import defaultdict
import time

data_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/enron_undirected/v1.messages'
concat_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/enron_undirected/v1.dyadsentences'

endings = ['.', '?', '!']

if __name__ == '__main__':
    start = time.time()
    context_sentences = defaultdict(str)
    with open(data_file, 'r') as f:
        lines = f.readlines()

    for i, line in enumerate(lines):
        if i % 500 == 0:
            print 'line %d'%i
        split_line = line.split('|', 2)
        entity_idx = split_line[1]
        msg = split_line[-1].strip()
        if msg[-1] not in endings:
            msg += '.'
        context_sentences[entity_idx] += ' %s'%msg

    with open(concat_file, 'w+') as f:
        for idx, text in context_sentences.iteritems():
            f.write('%s\t%s\n'%(idx, text.rstrip()))
    end = time.time() - start
    print '%f : time for %d lines'%(end, len(lines))








