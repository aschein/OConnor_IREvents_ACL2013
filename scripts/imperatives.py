from path_finder import *
import os

PARSED_DIR='/home/aschein/research/data/enron/preproc/directed/v2/parallel_files/parsed'

class SentenceParser:
    def __init__(self):
        self.sentences = []

    def load_sentences(self, file_path):
        self.sentences = []
        if os.path.isfile(file_path):
            with open(file_path, 'r') as f:
                lines = [l.strip() for l in f.readlines() if l.strip()]
            for sent_num, sent in iterate_sentences(lines):
                processed_sent = [process_sent_line(sent_line) for sent_line in sent]
                self.sentences.append(processed_sent)

def find_subjectless_sentence(sent):
    # handle questions

    root = -1
    root_pos = None
    has_subj = False

    for token in sent:
        if token[5] == 0 and 'VB' in token[3]:
            root = token[0]
            root_pos = token[3]
            break

    for token in sent:
        if token[6] == 'nsubj' or token[6] == 'expl':
            if token[5] == root:
                has_subj = True
                break

    # bad_tokens = ['THANK', 'LET', 'PLEASE']

    if not has_subj and root != -1:
        if root_pos != 'VBZ':
            dependents = [(tok[3], tok[6]) for tok in sent if tok[5] == root]
            if root == 1 or ('UH', 'intj') in dependents:
                return sent, root
    return None, root

def insert_pronoun_subj_of_root(root, person, sent):
    new_sent = [list(t) for t in sent]
    if person == 1:
        word = lemma = 'I'
    elif person == 2:
        word = 'You' if root == 1 else 'you'
        lemma = 'you'

    new_sent[root-1][1] = new_sent[root-1][1].lower()

    new_token = tuple([root, word, lemma, 'PRP', '_', root+1, 'nsubj', '%d:A0=PPT'%(root+1)])
    for token in new_sent:
        if token[0] >= root:
            token[0] += 1
        if token[5] >= root:
            token[5] += 1
            if token[-1] != '_':
                token[-1] = '%d:%s'%(token[5], token[-1].split(':')[1])
    new_sent.insert(root-1, new_token)
    return tuple(new_sent)

SEEN = []

def find_me_you_paths(sent, root, include_ends=False):
    root_pos = sent[root - 1][3]
    assert 'VB' in root_pos

    dependents = [tok[2] for tok in sent if tok[5] == root]

    dep_flag = False

    if root_pos == 'VBZ':
        pass

    elif 'you' in dependents and 'me' in dependents:
        pass

    elif 'you' in dependents:
        sent = insert_pronoun_subj_of_root(root, 1, sent)
        root += 1
        dep_flag = True

    elif 'me' in dependents:
        sent = insert_pronoun_subj_of_root(root, 2, sent)
        root += 1
        dep_flag = True

    if dep_flag:
        toks = [token[1].lower() for token in sent]
        toks[root-1] = toks[root-1].upper()
        if ' '.join(toks) not in SEEN:
            print ' '.join(toks)
            SEEN.append(' '.join(toks))
        if sent[-1][0] == '?':
            # toks = [token[1].lower() for token in sent]
            # toks[root-1] = toks[root-1].upper()
            # print ' '.join(toks)
            assert False
        candidates = get_path_candidates_2(sent)
        if candidates:
            for i, (e1_offset, e2_offset) in enumerate(candidates):
                yield get_dependency_path(e1_offset, e2_offset, sent, include_ends=include_ends)

if __name__=='__main__':

    seen = []
    hits = 0
    per_file_hits = 0
    total_files = 0
    sp = SentenceParser()
    for f in os.listdir(PARSED_DIR):
        total_files += 1
        found_path = False
        # print f
        sp.load_sentences(os.path.join(PARSED_DIR, f))
        for sent in sp.sentences:
            sent, head = find_subjectless_sentence(sent)
            if sent is not None:
                for (root, toks, poss, arcs) in find_me_you_paths(sent, head):
                    toks = [token[1].lower() for token in sent]
                    toks[head-1] = toks[head-1].upper()
                    # if ' '.join(toks) not in seen:
                    #     if toks[head-1] != 'THANK' and head == 1:
                    #         print ' '.join(toks)
                        # seen.append(' '.join(toks))
                    if good_path(root, toks, poss, arcs):
                        path_str = dep_to_string(root, toks, arcs)
                        # print path_str
                        hits += 1
                        found_path = True
        if found_path:
            per_file_hits += 1
    print '%d : total extra paths extracted'%hits
    print '%f : proportion of files with a hit'%(float(per_file_hits)/float(total_files))


