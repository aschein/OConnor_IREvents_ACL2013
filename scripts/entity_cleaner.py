if __name__ == '__main__':
    entity_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/3_entities.tsv'

    with open(entity_file, 'r') as f:
        lines = f.readlines()[1:]

    for line in lines:
        split_line = line.split('|')
        assert len(split_line) > 1
        email = split_line[0]

        gold_aliases = [x for x in split_line[1:] if '<' not in x and ',' not in x]
        assert len(gold_aliases) < 2
        if gold_aliases:
            print gold_aliases[0]


