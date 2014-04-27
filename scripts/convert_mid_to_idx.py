messages_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/clean_messages.txt'
entity_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/enron_undirected/v1.dyadcounts'
data_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/enron_undirected/v1.dyadsentences'
mid_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/enron_mid.vocab'
import time

def get_dyad(line):
    return (line.split()[1], line.split()[2])

if __name__ == '__main__':
    start = time.time()
    with open(entity_file, 'r') as f:
        lines = f.readlines()

    entities = [get_dyad(line) for line in lines]

    with open(messages_file, 'r') as f:
        lines = f.readlines()[400000:]
    print len(lines)

    mids = []
    with open(data_file, 'a+') as f:
        for i, line in enumerate(lines):
            if i % 1000 == 0:
                print 'line %d'%i
            if line.strip():
                split_line = line.strip().split('|', 4)
                dyad = (split_line[2], split_line[3])
                if dyad in entities:
                    idx = entities.index(dyad)
                else:
                    continue

                mid = split_line[0]
                assert mid not in mids
                midx = len(mids)
                mids.append(mid)
                f.write('%d|%d|%s\n'%(midx, idx, split_line[-1]))

    # with open(mid_file, 'a+') as f:
    #     f.write('\n')
    #     mid_str = '\n'.join([str(i) for i in mids])
    #     f.write(mid_str)

    end = time.time() - start
    print '%f : time for %d lines'%(end, len(lines))






