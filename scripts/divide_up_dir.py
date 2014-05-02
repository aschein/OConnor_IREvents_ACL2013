import os
import shutil

DATA_DIR='/mnt/nfs/work1/wallach/aschein/data/enron/preproc/enron_directed/v2/parallel_files/raw'
# DATA_DIR='/Users/aaronschein/Documents/research/mlds/OConnor_IREvents_ACL2013/enron_data/undirected/dyad_files/parsed'

def count_words(num):
    with open(os.path.join(DATA_DIR, '%d.sentences'%num), 'r') as f:
        text = f.read()
    return len(text)

if __name__=='__main__':
    divide_by=20

    global_word_total = 0
    global_docs_total = 0

    files = [x for x in os.listdir(DATA_DIR) if '.sentences' in x and '.srl' not in x]

    file_ints = sorted([int(x.split('.')[0]) for x in files])

    true_word_total = sum([count_words(n) for n in file_ints])
    true_docs_total = len(file_ints)

    for d in xrange(divide_by):

        subdir =  os.path.join(DATA_DIR, 'group_%d'%d)
        if os.path.isdir(subdir):
            print subdir
            raise
        os.mkdir(subdir)

        group = filter(lambda x: file_ints.index(x)%divide_by==d, file_ints)
        docs_total = len(group)
        word_total = sum([count_words(n) for n in group])

        global_docs_total += docs_total
        global_word_total += word_total

        print '%d files in group %d'%(docs_total, d)
        print '%d words in group %d'%(word_total, d)

        for f in group:
            endpath = '%d.sentences'%f
            shutil.move(os.path.join(DATA_DIR, endpath), os.path.join(subdir, endpath))

    if not global_docs_total == len(files):
        print 'bad docs tally:'
        print global_docs_total, len(files)

    if not global_word_total == true_word_total:
        print 'bad word tally:'
        print global_word_total, true_word_total

