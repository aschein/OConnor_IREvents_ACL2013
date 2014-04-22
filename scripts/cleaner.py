if __name__ == '__main__':
    from pandas import Timestamp
    from collections import defaultdict

    entity_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/3_entities.tsv'
    messages_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/3_messages.txt'

    entities = []
    with open(entity_file, 'r') as f:
        lines = f.readlines()[1:]
        for line in lines:
            entities.append(line.split('|')[0])

    print "%d entities"%len(entities)

    with open(messages_file, 'r') as f:
        lines = f.readlines()[1:]

    hits = 0
    times = []

    check_ts = Timestamp('19900101 00:00')
    shitty_ts = 0

    dyads = defaultdict(int)

    for line in lines:
        fr = line.split('|')[2]
        assert fr in entities
        to = [x[1:-1] for x in line.split('|')[3][1:-1].split(',')]
        if not len(to) == 1:
            continue
        if not to[0] in entities:
            continue
        ts = Timestamp(line.split('|')[1])
        if ts.year < check_ts.year:
            shitty_ts += 1
            continue
        dyads[(fr, to[0])] += 1
        hits += 1
        times.append(ts)
    times.sort()

    dyad_items = dyads.items()
    dyad_items.sort(key=lambda x: x[1], reverse=True)
    for i in dyad_items[:100]:
        print i



    print '%d documents'%(hits)
    print 'Dates range from %s to %s'%(str(times[0]), str(times[-1]))
    print '%f proportion hits'%(hits/float(len(lines)))
    print '%f proportion of bogus dates'%(shitty_ts/float(len(lines)))

