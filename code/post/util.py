import re,sys,json

def blue(s): return '<FONT COLOR="0000FF">%s</FONT>'%s 
def green(s): return '<FONT COLOR="196C19">%s</FONT>'%s
def red(s): return '<FONT COLOR="FF6600">%s</FONT>'%s

'I<-nsubj-BLAH<-fljf-BLAH-alj->YOU'

def nicepath4(pathstr):
    left = '&larr;'
    right = '&rarr;'
    pathstr = pathstr.replace('<-', left).replace('->', right)
    reg = re.compile(r'&[l|r]{1}arr;')
    seen_root = False
    pretty_str = ''
    split_string = re.split(reg, pathstr)
    pretty_str += red(split_string[0].upper())
    for piece in split_string[1:-1]:
        split_piece = piece.split('-')
        if len(split_piece) == 3:
            pretty_str += green('%s%s-'%(left, split_piece[0]))
            pretty_str += red(split_piece[1].upper())
            pretty_str += green('-%s%s'%(split_piece[2], right))
            seen_root = True
        else:
            assert len(split_piece) == 2
            if not seen_root:
                pretty_str += green('%s%s-'%(left, split_piece[0]))
                pretty_str += blue(split_piece[1].lower())
            else:
                pretty_str += blue(split_piece[0].lower())
                pretty_str += green('-%s%s'%(split_piece[1], right))
    pretty_str += red(split_string[-1].upper())
    return pretty_str

def nicepath3(pathstr):
    left = '&larr;'
    right = '&rarr;'
    pathstr = pathstr.replace('<-', left).replace('->', right)
    reg = re.compile(r'&[l|r]{1}arr;')
    seen_root = False
    pretty_str = ''
    for piece in re.split(reg, pathstr)[1:-1]:
        split_piece = piece.split('-')
        if len(split_piece) == 3:
            pretty_str += green('%s%s-'%(left, split_piece[0]))
            pretty_str += red(split_piece[1].upper())
            pretty_str += green('-%s%s'%(split_piece[2], right))
            seen_root = True
        else:
            assert len(split_piece) == 2
            if not seen_root:
                pretty_str += green('%s%s-'%(left, split_piece[0]))
                pretty_str += blue(split_piece[1].lower())
            else:
                pretty_str += blue(split_piece[0].lower())
                pretty_str += green('-%s%s'%(split_piece[1], right))
    return pretty_str

def nicepath(pathstr, html=True):
    print pathstr
    d = json.loads(pathstr)
    s = nicepath2(d)
    print s
    print 
    if not html:
        s = re.sub(r'\<.*?\>', "", s)
        s = re.sub(" +", " ", s)
        s = s.replace("&larr;", "<-").replace("&rarr;", "->")
        s = s.strip()
    return s

def nicepath2(path_arr):
    out = []
    for x in path_arr:
        if x[0]=='A':
            _,rel,arrow = x
            if rel.startswith("prep_"):
                out.append(rel.replace("prep_",""))
            elif rel=='dobj' or rel=='semagent':
                pass
            else:
                deparc = ("&larr;"+rel) if arrow=='<-' else (rel+"&rarr;")
                out.append("<span class=depedge>" +deparc+ "</span>")
        elif x[0]=='W':
            _,lemma,pos = x
            out.append(lemma)
    return ' '.join(out)

def parse_wordpos(wpos):
    # report/VERB
    if wpos is None: return wpos
    parts = wpos.split('/')
    return ['/'.join(parts[:-1]), parts[-1]]

def parse_relhop(relhop):
    direction = 'DOWN' if relhop.endswith("<-") else 'UP' if relhop.endswith("->") else None
    assert direction is not None
    rel = relhop.replace("<-","").replace("->","")
    rel = re.sub(",$","", rel)
    return [rel,direction]

def parsepath(pathstr):
    # deal with the stupid pipe/arrow format i made up a while ago
    # returns a JSON-friendly nest list format
    parts = pathstr.split('|')
    parts[0] = None
    result = [None]*len(parts)
    for i in range(0, len(parts), 2):
        result[i] = parse_wordpos(parts[i])
        result[i+1]=parse_relhop(parts[i+1])
    return result

def pageheader():
    print """
    <meta content="text/html; charset=utf-8" http-equiv="Content-Type"/>
    <style>
    .depedge { font-size: 8pt; color: #333; }
    .wordinfo { font-size: 9pt; }
    .pos { color: blue; }
    .neg { color: red; }
    .score a { color: inherit; }
    </style>
    <link rel="stylesheet" href="http://brenocon.com/js/tablesorter/2.7.2/css/mytheme.css" >
    
    <script type="text/javascript" src="https://ajax.googleapis.com/ajax/libs/jquery/1.9.0/jquery.min.js"></script> 
    <script type="text/javascript" src="http://brenocon.com/js/tablesorter/2.7.2/js/jquery.tablesorter.min.js"></script> 
    """
    # <script type="text/javascript" src="http://brenocon.com/js/tablesorter/2.7.2/js/jquery.tablesorter.widgets.js"></script> 
    print """<script>
    $(document).ready(function() 
        { 
            $("table").tablesorter();
        } 
    ); 
    </script>
    """

if __name__=='__main__':
    for line in sys.stdin:
        parts = line.rstrip('\n').split()
        parts.append(nicepath(parts[-1], html=False))
        print '\t'.join(parts)
        # print parsepath(line.strip())
