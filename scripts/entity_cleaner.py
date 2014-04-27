if __name__ == '__main__':

    from collections import defaultdict
    import re

    entity_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/3_entities.tsv'

    with open(entity_file, 'r') as f:
        lines = f.readlines()[1:]

    gold_count = 0
    silver_count = 0
    misses = 0
    gold_entities = defaultdict(int)

    tricky = 0
    trickies = []

    reg = re.compile('^.*<')

    for i, line in enumerate(lines):
        split_line = line.split('|')
        assert len(split_line) > 1
        email = split_line[0]

        gold_aliases = [x for x in split_line[1:] if '<' not in x and ',' not in x and '@' not in x]
        if gold_aliases:
            gold_count += 1
            gold_entities[email] = gold_aliases[0].strip()
        else:
            bronze_aliases = [x for x in split_line[1:] if '<' not in x and ',' in x and '@' not in x]
            if bronze_aliases:
                target = ''
                for alias in bronze_aliases:
                    if len(alias.split(',')[1]) > 2:
                        continue
                    target = alias
                    break
                if target:
                    if i == 6160:
                        assert False

                    a, b = [x.strip() for x in target.split(',')]
                    gold_entities[email] = '%s %s'%(b, a)
                    silver_count += 1
                    break
                else:
                    tricky += 1
                    trickies.append((i, line))
            else:
                silver_aliases = [x for x in split_line[1:] if '<' in x]
                if silver_aliases:
                    target = ''
                    for alias in silver_aliases:
                        alias = reg.findall(alias)[0].strip()[:-1]
                        if '<' in alias:
                            continue
                        if '@' in alias:
                            continue
                        if '"' in alias:
                            alias = alias.strip()[1:-1]
                        if ',' in alias:
                            target = alias
                            break
                        # elif '-' in alias:
                        #     continue
                        # else:
                        #     print alias.strip()
                        #     gold_entities[email] = alias.strip()
                        #     silver_count += 1
                        #     break
                    if target:
                        a,b = [x.strip() for x in target.split(',')]
                        gold_entities[email] = '%s %s'%(b, a)
                        silver_count += 1
                    else:
                        trickies.append((i, line))
                        tricky += 1
                else:
                    trickies.append((i, line))
                    tricky += 1

    new_trickies = []

    assert len(trickies) == tricky
    for tricky_line in trickies:
        split_line = tricky_line[1].split('|')
        email = split_line[0]
        aliases = split_line[1:]
        flag = False
        for alias in aliases:
            if ',' in alias:
                if '<' in alias or '@' in alias:
                    continue
                a, b = alias.strip().split(',')
                name = '%s %s'%(b, a)
                gold_entities[email] = name
                flag = True
                break
        if flag:
            silver_count += 1
            tricky -= 1
        else:
            new_trickies.append(tricky_line)

    hopeless = 0 
    print len(new_trickies)
    print tricky
    for i, line in new_trickies:
        if (len(line.split('|')[1:]) == 1):
            if '@' in line.split('|')[1]:
                if '<' not in line.split('|')[1]:
                    hopeless += 1
                    tricky -= 1
                    # new_trickies.remove((i, line))
                    continue
                else:
                    target = reg.findall(line.split('|')[1])[0][:-1]
                    if '@' not in target:
                        if ',' not in target:
                            if '<' not in target:
                                if '"' not in target:
                                    if '-' not in target:
                                        tricky -= 1
                                        silver_count += 1
                                        gold_entities[line.split('|')[0]] = target.strip()
                                        # new_trickies.remove((i, line))
                                else:
                                    tricky -= 1
                                    silver_count += 1
                                    gold_entities[line.split('|')[0]] = target.strip()[1:-1]
                                    # new_trickies.remove((i, line))
            else:
                target = reg.findall(line.split('|')[1])[0][:-1]
                gold_entities[line.split('|')[0]] = target.strip()
                tricky -= 1
                silver_count += 1
                # new_trickies.remove((i, line))

    # for i, line in new_trickies:
    #     assert line.split('|')[0] not in gold_entities
    print tricky
    print len(new_trickies)

    assert 'ken.lay@enron.com' in gold_entities

    assert len([x for x in gold_entities.values() if '@' in x]) == 0

    print float(gold_count + silver_count)/len(lines)
    print float(tricky)/len(lines)
    if not hopeless + tricky + gold_count + silver_count == len(lines):
        print '---------'
        print hopeless
        print tricky
        print gold_count
        print silver_count
        print '--------'
        print tricky + hopeless + gold_count + silver_count
        print '=========='
        print len(lines)

    with open('entity_gazeteer.csv', 'w+') as f:
        for email, name in gold_entities.iteritems():
            f.write('%s, %s\n'%(email.strip(), name.strip()))







