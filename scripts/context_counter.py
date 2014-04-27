"""Module for obtaining dyad counts."""

from collections import Counter

def get_undirected_dyad(line):
    split_line = line.strip().split('|')
    return tuple(sorted((split_line[2], split_line[3])))

def get_directed_dyad(line):
    split_line = line.strip().split('|')
    return (split_line[2], split_line[3])

if __name__ == '__main__':
    messages_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/clean_messages.txt'
    out_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/enron_undirected.dyadcounts'
    with open(messages_file, 'r') as f:
        lines = f.readlines()[1:]
    dyads = [get_undirected_dyad(line) for line in lines if line.strip()]
    c = Counter(dyads)
    with open(out_file, 'w+') as f:
        for dyad, count in c.most_common(None):
            if '@enron' in dyad[0] and '@enron' in dyad[1]:
                f.write('%d %s\t%s\n'%(count, dyad[0], dyad[1]))

