if __name__ == '__main__':
    entity_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/entities.tsv'
    messages_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/messages.txt'

    entities = []
    with open(entity_file, 'r') as f:
        lines = f.readlines()[1:]
        for line in lines:
            entities.append(line.split('|')[0])

    assert len(entities) == 300

    with open(messages_file, 'r') as f:
        lines = f.readlines()[1:]

    hits = 0

    for line in lines:
        to = [x[1:-1] for x in line.split('|')[3][1:-1].split(',')]
        if not sum([t in entities for t in to]):
            continue
        hits += 1

    print '%f proportion hits'%(hits/float(len(lines)))



