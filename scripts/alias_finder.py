
from collections import defaultdict, Counter
import re

RAW_FILE = '/home/aschein/research/data/enron/preproc/v2.rawaliases'
GAZ_FILE = '/home/aschein/research/data/enron/preproc/v2.aliases'

bra_reg = re.compile('(.*?)\s*<')
at_reg = re.compile('(.*?)\sAT\s')

if __name__ == '__main__':

    with open(RAW_FILE, 'r') as f:
        lines = f.readlines()

    orig_total = len(lines)

    junk = defaultdict(set)
    gold = defaultdict(set)

    for idx, line in enumerate(lines):

        split_line = line.split('\t')

        email = split_line[0].strip()
        aliases = split_line[1:]

        for alias in aliases:
            target = alias.strip()
            if '<' in target:
                target = bra_reg.findall(target)[0].strip()
            if '@' in target:
                junk[email].add(alias)
                continue
            if ',' in target:
                split_target = target.split(',')
                if len(split_target) != 2:
                    assert len(split_target) == 3
                    split_target = split_target[1:]
                last, first = split_target
                target = ' '.join([first.strip(), last.strip()])
            target = target.replace('"', '')
            target = target.replace("'", '')
            target = target.strip()
            if len(target) < 2:
                junk[email].add(alias)
                continue

            if '.' in target:
                print target

            gold[email.strip()].add(target)

    overlap = []
    for key in junk.keys():
        if key not in gold.keys():
            for a, alias in enumerate(junk[key]):
                if len(alias.split()) == 1 and '@' in alias:
                    continue
                if at_reg.search(alias) is not None:
                    target = at_reg.findall(alias)[0]
                    gold[key].add(target)
                    overlap.append(key)
                    continue

    print '%f extracted'%(float(len(gold.keys()))/float(len(lines)))


    with open(GAZ_FILE, 'w+') as f:
        for email, aliases in gold.iteritems():
            final_aliases = [x.strip() for x in aliases]
            joined_aliases = ','.join(final_aliases)
            f.write('%s,%s\n'%(email.strip(), joined_aliases))

        for email in junk.keys():
            if email in gold.keys():
                continue
            f.write('%s,\n'%email)

    with open(GAZ_FILE, 'r') as f:
        lines = f.readlines()

    assert len(lines) == orig_total

