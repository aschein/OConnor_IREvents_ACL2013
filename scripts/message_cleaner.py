from collections import defaultdict


if __name__ == '__main__':
    # MID|TIME|FROM|TO|BODY
    messages_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/3_messages.txt'
    clean_messages_file = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/clean_messages.txt'

    context_dict = defaultdict(int)


    with open(messages_file, 'r') as f:
        lines = f.readlines()[1:]

    seen_mids = []
    with open(clean_messages_file, 'w+') as f:
        for i, line in enumerate(lines):
            mid, ts, fr, to, body = line.split('|', 4)
            assert mid not in seen_mids
            seen_mids.append(mid)
            if len(to[1:-1].split(',')) > 1:
                continue
            print i, mid
            clean_to = to[2:-2]
            f.write('%s|%s|%s|%s|%s\n'%(mid, ts, fr, clean_to, body))




