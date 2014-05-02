import os
import time

DATA_DIR='/mnt/nfs/work1/wallach/aschein/data/enron/preproc/enron_directed/v2/parallel_files/raw'

start_time = 1398805495.269101

if __name__ == '__main__':
    runtime = time.time() - start_time

    total_unparsed = 0
    total_parsed = 0

    subdirs = [os.path.join(DATA_DIR, x) for x in os.listdir(DATA_DIR) if 'group_' in x]
    subdirs = sorted(subdirs, key=lambda x: int(os.path.basename(x).split('.')[0].split('group_')[1]))

    completed = []
    uncompleted = []
    for subdir in subdirs:
        unparsed = len([x for x in os.listdir(subdir) if '.srl' not in x])
        parsed = len([x for x in os.listdir(subdir) if '.srl' in x])
        total_unparsed += unparsed
        total_parsed += parsed
        if unparsed == parsed:
            completed.append(os.path.basename(subdir))
        else:
            uncompleted.append(((float(parsed)/float(unparsed)), os.path.basename(subdir)))
        # print '%f : proportion of parsed files in %s'%((float(parsed)/float(unparsed)), os.path.basename(subdir))

    print
    print 'Groups completed:'
    for c in completed:
        print c
    print
    print 'Groups in progress:'
    for u in uncompleted:
        print '%s : %f complete'%(u[1], u[0])

    print
    print '%f : proportion of parsed files OVERALL'%(float(total_parsed)/float(total_unparsed))
    print '%f : current runtime (mins)'%(runtime/60.0)
    rate = runtime/float(total_parsed)
    proj_time = rate*(total_unparsed - total_parsed)
    print '%f : projected runtime (mins)'%(proj_time/60.0)

