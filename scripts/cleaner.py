if __name__ == '__main__':
    from pandas import Timestamp

    entity_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/2_entities.tsv'
    messages_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/2_messages.txt'

    entities = []
    with open(entity_file, 'r') as f:
        lines = f.readlines()[1:]
        for line in lines:
            entities.append(line.split('|')[0])

    # assert len(entities) == 300
    print "%d entities"%len(entities)
    # assert sum(['@enron' not in e.lower() for e in entities]) == 0
    # for e in entities:
    #     if '@enron' not in e.lower():
    #         print e
    print sum(['@enron' in e.lower() for e in entities])/float(len(entities))
    for e in entities:
        if '@enron' in e.lower():
            print e

    with open(messages_file, 'r') as f:
        lines = f.readlines()[1:]

    hits = 0

    times = []

    for line in lines:
        to = [x[1:-1] for x in line.split('|')[3][1:-1].split(',')]
        if not sum([t in entities for t in to]):
            continue
        hits += 1
        times.append(Timestamp(line.split('|')[1]))

    times.sort()

    print '%d documents'%(hits)
    print 'Dates range from %s to %s'%(str(times[0]), str(times[-1]))
    print '%f proportion hits'%(hits/float(len(lines)))

