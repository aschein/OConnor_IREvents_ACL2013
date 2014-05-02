import itertools
from collections import Counter, defaultdict
import time
import sys
import os
from imperatives import *

GAZETTEER = '/home/aschein/research/data/enron/preproc/v2.aliases'
LOOKUP = '/home/aschein/research/data/enron/preproc/v2.dyadcounts'
PARSED_DIR = '/home/aschein/research/data/enron/preproc/directed/v2/parallel_files/parsed'

OUT_1 = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/directed/v3.paths'
OUT_2 = '/mnt/nfs/work1/wallach/aschein/data/enron/preproc/directed/v3.pathcounts'

# loc_GAZETTEER = '/Users/aaronschein/Documents/research/mlds/OConnor_IREvents_ACL2013/enron_data/undirected/entity_gazeteer.csv'
# loc_LOOKUP = '/Users/aaronschein/Documents/research/mlds/OConnor_IREvents_ACL2013/enron_data/undirected/v1.dyadcounts'
# loc_PARSED_DIR = '/Users/aaronschein/Documents/research/mlds/OConnor_IREvents_ACL2013/enron_data/undirected/dyad_files/parsed'
# loc_OUT_1 = '/Users/aaronschein/Documents/research/mlds/OConnor_IREvents_ACL2013/enron_data/undirected/v1.paths'
# loc_OUT_2 = '/Users/aaronschein/Documents/research/mlds/OConnor_IREvents_ACL2013/enron_data/undirected/v1.pathcounts'

ME_subj_WORDS = ['I']
ME_obj_WORDS = ['me']
YOU_WORDS = ['you', 'ya']

class PathFinder:
    def __init__(self, dyad_lookup, entity_gazetteer):
        self.counts = {}
        self.dyads = []
        self.aliases = defaultdict(list)
        with open(dyad_lookup, 'r') as f:
            self.dyads = [(line.split()[1].strip(), line.split()[2].strip()) for line in f.readlines()]
        with open(dyad_lookup, 'r') as f:
            for line in f.readlines():
                self.counts[(line.split()[1].strip(), line.split()[2].strip())] = int(line.split()[0].strip())
        with open(entity_gazetteer, 'r') as f:
            for line in f.readlines():
                self.aliases[line.split(',')[0].strip()].extend([x.strip() for x in line.split(',')[1:]])
        self.sentences = []
        self.curr_dyad = None
        self.curr_date = '1999-01-01' # placeholder
        self.first_name_fr = ''
        self.first_name_to = ''

    def perform_check(self):
        import string
        exlude = set(string.punctuation)

        orphan_aliases = []
        orphan_dyads = []

        flat_dyads = []
        for dyad in self.dyads:
            flat_dyads.append(dyad[0])
            flat_dyads.append(dyad[1])
        flat_dyads = set(flat_dyads)
        for alias in self.aliases.keys():
            if alias not in flat_dyads:
                orphan_aliases.append(alias)
        for dyad in flat_dyads:
            if dyad not in self.aliases.keys():
                orphan_dyads.append(dyad)

        orphan_aliases = [''.join(ch for ch in s if ch not in exlude) for s in orphan_aliases]
        orphan_dyads = [''.join(ch for ch in s if ch not in exlude) for s in orphan_dyads]
        print [o for o in orphan_aliases if o in orphan_dyads]

    def load_sentences(self, file_path):
        self.sentences = []
        dyad_num = int(os.path.basename(file_path).split('.')[0])
        if os.path.isfile(file_path):
            self.curr_dyad = self.dyads[dyad_num]
            if self.aliases[self.curr_dyad[0]]:
                if self.aliases[self.curr_dyad[0]][0]:
                    self.first_name_fr = self.aliases[self.curr_dyad[0]][0].split()[0].lower()
            if self.aliases[self.curr_dyad[1]]:
                if self.aliases[self.curr_dyad[1]][0]:
                    self.first_name_to = self.aliases[self.curr_dyad[1]][0].split()[0].lower()
            with open(file_path, 'r') as f:
                lines = [l.strip() for l in f.readlines() if l.strip()]
            for sent_num, sent in iterate_sentences(lines):
                processed_sent = [process_sent_line(sent_line) for sent_line in sent]
                self.sentences.append(processed_sent)

def check_for_dupes(file_path, thresh=5):
    sents = []
    with open(file_path, 'r') as f:
        lines = [l.strip() for l in f.readlines() if l.strip()] 
        for sent_num, sent in iterate_sentences(lines):
            processed_sent = [process_sent_line(sent_line) for sent_line in sent]
            sents.append(sentence_to_string(processed_sent))
    probable_dupes = 0
    if len(sents) != len(set(sents)):
        c = Counter(sents)
        for s, count in c.most_common(None):
            if count == 1:
                break
            if len(s.split()) > thresh:
                probable_dupes += count - 1
    return probable_dupes, len(sents)

def get_lowest_common_ancestor(e1_offset, e2_offset, sent):
    e1_token = sent[e1_offset - 1]
    assert e1_token[0] == e1_offset
    e2_token = sent[e2_offset - 1]
    assert e2_token[0] == e2_offset

    e1_ancestors = []
    curr_token = e1_token
    curr_parent = curr_token[5]
    while(curr_parent > 0):
        e1_ancestors.append(curr_parent)
        curr_token = sent[curr_parent - 1]
        curr_parent = curr_token[5]

    e2_ancestors = []
    curr_token = e2_token
    curr_parent = curr_token[5]
    while(curr_parent > 0):
        e2_ancestors.append(curr_parent)
        curr_token = sent[curr_parent - 1]
        curr_parent = curr_token[5]

    lowest_common_ancestor = 0
    if len(e1_ancestors) == 0 or len(e2_ancestors) == 0: return 0
    while(e1_ancestors[-1] == e2_ancestors[-1]):
        lowest_common_ancestor = e1_ancestors.pop()
        e2_ancestors.pop()
        if len(e1_ancestors) == 0 or len(e2_ancestors) == 0:
            break

    return lowest_common_ancestor

def get_dependency_path(e1_offset, e2_offset, sent, include_ends=True):
    root = get_lowest_common_ancestor(e1_offset, e2_offset, sent)
    toks = []
    arcs = []
    poss = []
    if root == 0: return -1, toks, poss, arcs
    curr_offset = e1_offset
    while(curr_offset != root):
        curr_token = sent[curr_offset - 1]
        lemma = curr_token[2]
        pos = curr_token[3]
        arc = curr_token[6]
        arcs.append(arc)
        if include_ends or (curr_offset != e1_offset):
            toks.append(lemma)
            poss.append(pos)
        curr_offset = curr_token[5]

    root_token = sent[root - 1]
    root_idx = len(toks)
    toks.append(root_token[2])
    poss.append(root_token[3])

    curr_offset = e2_offset
    temp_arcs = []
    temp_toks = []
    temp_poss = []
    while(curr_offset != root):
        curr_token = sent[curr_offset - 1]
        lemma = curr_token[2]
        pos = curr_token[3]
        arc = curr_token[6]
        temp_arcs.append(arc)
        if include_ends or (curr_offset != e2_offset):
            temp_toks.append(lemma)
            temp_poss.append(pos)
        curr_offset = curr_token[5]
    temp_arcs.reverse()
    temp_toks.reverse()
    temp_poss.reverse()
    arcs.extend(temp_arcs)
    toks.extend(temp_toks)
    poss.extend(temp_poss)
    return root_idx, toks, poss, arcs

def dep_to_string(root, toks, arcs, include_ends=False):
    if include_ends: 
        return dep_to_string_with_ends(root, toks, arcs)
    path_str = ''
    if root == -1: return path_str
    for arc, tok in zip(arcs[:root+1], toks[:root+1]):
        path_str += '<-%s-%s'%(arc, tok)
    path_str += '-%s->'%arcs[root+1]
    for tok, arc in zip(toks[root+1:], arcs[root+2:]):
        path_str += '%s-%s->'%(tok, arc)
    return path_str

def dep_to_string_with_ends(root, toks, arcs):
    path_str = ''
    if root == -1: return path_str
    for arc, tok in zip(arcs[:root], toks[:root]):
        path_str += '%s<-%s-'%(tok, arc)
    path_str += toks[root]
    for tok, arc in zip(toks[root+1:], arcs[root:]):
        path_str += '-%s->%s'%(arc, tok)
    return path_str

def get_str_dependency_path(e1_offset, e2_offset, sent):
    path = ''
    path_length = 0 
    root = get_lowest_common_ancestor(e1_offset, e2_offset, sent)
    if root == 0: return path, path_length

    curr_offset = e1_offset
    path_bits = []
    while(curr_offset != root):
        curr_token = sent[curr_offset - 1]
        lemma = curr_token[2]
        arc = curr_token[6]
        path_bits.append('%s<-%s-'%(lemma, arc))
        # if curr_offset != e1_offset:
        #     path_bits.append('%s<-%s-'%(lemma, arc))
        # else:
        #     path_bits.append('<-%s-'%arc)
        # curr_offset = curr_token[5]

    path_length += len(path_bits)
    path = ''.join(path_bits)

    root_token = sent[root - 1]
    path += root_token[2]

    curr_offset = e2_offset
    path_bits = []
    while(curr_offset != root):
        curr_token = sent[curr_offset - 1]
        lemma = curr_token[2]
        arc = curr_token[6]

        path_bits.append('-%s->%s'%(arc, lemma))
        # if curr_offset != e2_offset:
        #     path_bits.append('-%s->%s'%(arc, lemma))
        # else:
        #     path_bits.append('-%s->'%arc)
        curr_offset = curr_token[5]

    path_length += len(path_bits)
    path_bits.reverse()
    path += ''.join(path_bits)

    return path, path_length

def process_sent_line(sent_line):
    split_line = sent_line.split('\t')
    return (int(split_line[0]),) + tuple(split_line[1:5]) + (int(split_line[5]), ) + tuple(split_line[6:])

def get_sentence_offsets(parse_lines):
    offsets = []
    for idx, line in enumerate(parse_lines):
        if line.split('\t')[0] == '1':
            offsets.append(idx)
    return offsets

def iterate_sentences(parse_lines):
    offsets = get_sentence_offsets(parse_lines)
    idx = 0
    while(idx < len(offsets)):
        start_line = offsets[idx]
        last_line = None if idx+1 == len(offsets) else offsets[idx+1]
        yield idx, parse_lines[start_line:last_line]
        idx += 1

def get_sentence(sent_num, parse_lines):
    for idx, sent in iterate_sentences(parse_lines):
        if idx == sent_num:
            return sent

def sentence_to_string(sentence):
    return ' '.join(token[1] for token in sentence)

def get_path_candidates_2(sentence):
    names = ['I', 'me', 'you']
    # from_names = ['I', 'me']
    # if self.first_name_fr:
    #     from_names.append(self.first_name_fr)
    # to_names = ['you']
    # if self.first_name_to:
    #     to_names.append(self.first_name_to)
    offsets = [token[0] for token in sentence if token[2] in names]
    candidates = []
    while(offsets):
        i = offsets.pop(0)
        token = sentence[i - 1]
        a = token[2]
        for j in offsets:
            partner = sentence[j - 1]
            b = partner[2]
            if (a == 'I' and b == 'you') or (a == 'you' and b == 'me'):
                candidates.append((i, j))
    return candidates

def get_path_candidates(sentence):
    me_subj_tokens = []
    me_obj_tokens = []
    you_tokens = []
    for token in sentence:
        if token[2] == 'I': me_subj_tokens.append(token[0])
        elif token[2] == 'me': me_obj_tokens.append(token[0])
        elif token[2] == 'you': you_tokens.append(token[0])
    candidates = []
    if len(you_tokens) > 0:
        if len(me_subj_tokens) > 0:
            candidates.extend([(a, b) for a, b in itertools.product(me_subj_tokens, you_tokens) if a < b])
        if len(me_obj_tokens) > 0:
            candidates.extend([(a, b) for a, b in itertools.product(you_tokens, me_obj_tokens) if a < b])
    return candidates

def good_path(root, toks, poss, arcs):

    if len(arcs) < 1: return False
    if len(arcs) > 5: return False

    bad_guys = ['parataxis', 'conj', 'dep']

    for bad in bad_guys:
        if bad in arcs:
            return False

    for p in poss:
        if 'VB' in p:
            return True

if __name__ == '__main__':

    args = sys.argv[1:]
    args = [os.path.join(PARSED_DIR, f) for f in os.listdir(PARSED_DIR)]

    paths = []
    p = PathFinder(LOOKUP, GAZETTEER)

    # sample_keys = p.aliases.keys()[100:115]
    # for key in sample_keys:
    #     print key
    #     print p.aliases[key]
    #     print key

    f = open(OUT_1, 'w+')

    file_count = 0
    file_hits = 0

    good = 0
    total = 0

    include_ends = True

    start = time.time()
    try:
        for arg in args:
            hit = False
            file_name = os.path.basename(arg)
            if not os.path.isfile(arg):
                continue
                print arg
            else:
                p.load_sentences(arg)
                file_count += 1
                for sent_num, sentence in enumerate(p.sentences):
                    candidates = get_path_candidates_2(sentence)
                    if candidates:
                        for i, (e1_offset, e2_offset) in enumerate(candidates):
                            total += 1
                            root, toks, poss, arcs = get_dependency_path(e1_offset, e2_offset, sentence, include_ends=include_ends)
                            if good_path(root, toks, poss, arcs):
                                hit = True
                                good += 1
                                path_str = dep_to_string(root, toks, arcs, include_ends=include_ends)
                                f.write('%s\t%d\t%s\t%s\t%s\t%s\n'%(file_name, sent_num, p.curr_date, p.curr_dyad[0], p.curr_dyad[1], path_str))
                                paths.append(path_str)
                    sentence, head = find_subjectless_sentence(sentence)
                    if sentence is not None:
                        for (root, toks, poss, arcs) in find_me_you_paths(sentence, head, include_ends=include_ends):
                            total += 1
                            if good_path(root, toks, poss, arcs):
                                hit = True
                                good += 1
                                path_str = dep_to_string(root, toks, arcs, include_ends=include_ends)
                                f.write('%s\t%d\t%s\t%s\t%s\t%s\n'%(file_name, sent_num, p.curr_date, p.curr_dyad[0], p.curr_dyad[1], path_str))
                                paths.append(path_str)
            file_hits += hit

    except KeyboardInterrupt:
        f.close()
        c = Counter(paths)
        with open(OUT_2, 'w+') as f:
            for common_path in c.most_common(None):
                f.write('%d\t%s\n'%(common_path[1], common_path[0]))
        end = time.time() - start
        print '%f secs on %d files'%(end, file_count)
        num_files = len(args)
        rate = end/float(file_count)
        print '%f proportion of good filters'%(float(good)/float(total))
        print '%f proportion of files with a path'%(float(file_hits)/float(file_count))
        print '%f mins projected for %d total files'%(rate*num_files/60.0, num_files)
        sys.exit()

    f.close()
    c = Counter(paths)
    with open(OUT_2, 'w+') as f:
        for common_path in c.most_common(None):
            f.write('%d\t%s\n'%(common_path[1], common_path[0]))
    end = time.time() - start
    print '%f secs on %d files'%(end, file_count)
    num_files = len(args)
    rate = end/float(file_count)
    print '%f proportion of good filters'%(float(good)/float(total))
    print '%f proportion of files with a path'%(float(file_hits)/float(file_count))
    print '%f mins projected for %d total files'%(rate*num_files/60.0, num_files)
