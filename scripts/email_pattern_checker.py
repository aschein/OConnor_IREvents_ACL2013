import os
import re
import time
from numpy import mean
from collections import defaultdict

DATA_DIR='/home/aschein/research/data/enron/enron_mail_20110402/maildir'
# DATA_DIR='/home/aschein/research/data/enron/test'
# DATA_DIR='/Users/aaronschein/Documents/research/mlds/OConnor_IREvents_ACL2013/scripts/test'

OUT_FILE='/mnt/nfs/work1/wallach/aschein/data/enron/preproc/v2.messages'
DYAD_FILE='/mnt/nfs/work1/wallach/aschein/data/enron/preproc/v2.dyadcounts'
GAZ_FILE='/mnt/nfs/work1/wallach/aschein/data/enron/preproc/v2.rawaliases'
    
def nonblank_lines(f):
    for l in f:
        line = l.rstrip()
        if line:
            yield line

def recursive_walk(data_dir):
    for root, dirs, files in os.walk(data_dir):
        for f in files:
            yield os.path.join(root, f)

def writeout_entities(dyads, entities):
    sorted_dyads = sorted(list(dyads.iteritems()), key=lambda x: x[1], reverse=True)
    with open(DYAD_FILE, 'w+') as f:
        for (fr, to), count in sorted_dyads:
            f.write('%d\t%s\t%s\n'%(count, fr, to))
    with open(GAZ_FILE, 'w+') as f:
        for email, aliases in entities.iteritems():
            sorted_aliases = sorted(list(aliases), key=lambda x: len(x))
            joined_aliases = '\t'.join(sorted_aliases)
            f.write('%s\t%s\n'%(email, joined_aliases))

def parse(data_dir):
    reg = re.compile('Message-ID:.+?\nDate:.+?\nFrom:.+?\nTo:.+?\n')
    mid_reg = re.compile('Message-ID:.+?\n')
    to_reg = re.compile('To:.+?\n')
    fr_reg = re.compile('From:.+?\n')
    xto_reg = re.compile('X-To:.+?\n')
    xfr_reg = re.compile('X-From:.+?\n')

    msg_reg = re.compile('X-FileName:.+\n')
    ts_reg = re.compile('\n*\n.*?\n.*?[0-9]{2}/[0-9]{2}/[0-9]{2,4} [0-9]{2}:[0-9]{2}:?[0-9]* [A|P]{1}M.*?\n')
    for_reg = re.compile('(\n*.*---+.*(Forward|Message).*\n?.*---+.*\n?)')
    for2_reg = re.compile('.*---+.*Forwarded.*---+')
    des_reg = re.compile('\n\n+.*?To:.*?\ncc:.*\n')

    times = []
    total = 0 
    good_total = 0

    no_content = 0

    dyads = defaultdict(int)
    entities = defaultdict(set)

    try:
        out_file = open(OUT_FILE, 'w+')
        for full_path in recursive_walk(data_dir):
            total += 1
            start = time.time()
            with open(full_path, 'r') as f:
                text = f.read()
            hits = reg.findall(text)
            if len(hits) == 0:
                continue            
            elif len(hits) > 1:
                continue
            metadata = hits[0]
            
            to_line = to_reg.findall(metadata)[0].strip()
            if ',' in to_line:
                continue
            if '@enron' not in to_line:
                continue
            fr_line = fr_reg.findall(metadata)[0].strip()
            if ',' in fr_line:
                continue
            if '@enron' not in fr_line:
                continue
            to = to_line.split(':')[1].strip()
            fr = fr_line.split(':')[1].strip()

            mid = mid_reg.findall(metadata)[0].split(':')[1].strip()

            xfr_hits = xfr_reg.findall(text)
            if len(xfr_hits) == 0:
                continue
            elif len(xfr_hits) > 1:
                continue
            fr_alias = xfr_hits[0].split(':')[1].strip()

            xto_hits = xto_reg.findall(text)
            if len(xto_hits) == 0:
                continue
            elif len(xto_hits) > 1:
                continue
            to_alias = xto_hits[0].split(':')[1].strip()

            entities[fr].add(fr_alias)
            entities[to].add(to_alias)
            dyads[(fr, to)] += 1

            msg_hits = msg_reg.findall(text)
            if len(msg_hits) == 0:
                print full_path
                print 'No message found...'
            elif len(msg_hits) > 1:
                print full_path
                print 'Several messages found...'
            else:
                msg_body = re.split(msg_reg, text)[1]
                msg_body = re.split(for_reg, msg_body)[0]
                if msg_body.strip():
                    msg_body = re.split(ts_reg, msg_body)[0]
                    msg_body = re.split(des_reg, msg_body)[0]
                else:
                    no_content += 1

                msg_body = msg_body.strip()
                msg_body = msg_body.replace('|', ';')
                msg_body = msg_body.replace('\n', ' ')
                msg_body = msg_body.replace('\r', ' ')

                # if for2_reg.search(msg_body) is not None:
                #     print 'WTF'
                #     raise

                # if 'Forwarded' in msg_body:
                #     print msg_body
                #     raise

                if len(msg_body) == 0: 
                    continue

            rel_path = full_path.split('maildir/')[1]
            out_file.write('%s|%s|%s|%s|%s\n'%(rel_path, mid, fr, to, msg_body))
            good_total += 1
            end = time.time() - start
            times.append(end)
    except KeyboardInterrupt or TypeError:
        avg_time = mean(times)
        print 'Estimated time: %f mins\n'%(avg_time*540000.0/60.0)
        print 'Proportion of good: %f\n'%(float(good_total)/float(total))
        print 'Proportion of no content: %f\n'%(float(no_content)/float(total))
        out_file.close()
        writeout_entities(dyads, entities)

    out_file.close()
    writeout_entities(dyads, entities)
    avg_time = mean(times)
    print 'Estimated time: %f mins\n'%(avg_time*540000.0/60.0)
    print 'Proportion of good: %f\n'%(float(good_total)/float(total))
    print 'Proportion of no content: %f\n'%(float(no_content)/float(total))

if __name__ == '__main__':
    parse(DATA_DIR)
