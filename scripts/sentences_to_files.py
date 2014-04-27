concat_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/enron_directed/v1.dyadsentences'
dyad_dir = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/enron_directed/dyad_files'

import os

if __name__ == '__main__':

    with open(concat_file, 'r') as f:
        lines = f.readlines()

    lines = [line.split('\t', 1) for line in lines]
    lines = [(int(a), b.strip()) for a,b in lines]
    lines = sorted(lines)
    for dyad, sentences in lines:
        with open(os.path.join(dyad_dir, '%d.sentences'%dyad), 'w+') as f:
            f.write(sentences)
