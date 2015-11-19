from mrjob.job import MRJob
from MRKMeans import MRKMeans
import sys,os
import os.path
import shutil
from math import sqrt

input_c = "cent.txt"

CENTROIDS_FILE = "temp/cent.txt"


def get_c(job, runner):
    c = []    
    for line in runner.stream_output():
        key, value = job.parse_output_line(line)
        c.append(key)
    return c


def get_first_c(fname):
    centroids = []
    with open(fname, 'r') as f:        
        for line in f:
            if line:
                x, y = line.split('\t')
                centroids.append([float(x), float(y)])
    return centroids

def write_c(centroids):
    with open(CENTROIDS_FILE,'w') as f:
        centroids.sort()
        for c in centroids:
            k,cx,cy = c.split(',')
            f.write("%s\t%s\n"%(cx,cy))

def dist_vec(v1,v2):
    return sqrt((v2[0]-v1[0])*(v2[0]-v1[0])+(v2[1]-v1[1])*(v2[1]-v1[1]))

def diff(cs1,cs2):
    max_dist = 0.0
    for i in range(len(cs1)):
        dist = dist_vec(cs1[i],cs2[i])
        if dist > max_dist:
            max_dist = dist
    return max_dist
 
if __name__ == '__main__':
    args = sys.argv[1:]

    print args

    os.remove(CENTROIDS_FILE)
    shutil.copy(input_c,CENTROIDS_FILE)

    old_c = get_first_c(input_c)
    i=1 
    while True:
        print "Iteration #%i" % i
        mr_job=MRKMeans(args=args + ['--c='+CENTROIDS_FILE])        
        with mr_job.make_runner() as runner:
            runner.run()
            centroids = get_c(mr_job,runner)
            write_c(centroids)
        n_c = get_first_c(CENTROIDS_FILE)
        for c in n_c:
            print c[0],c[1]
        max_d = diff(n_c,old_c)
        print max_d
        if max_d < 0.0001:
            break
        else:
            old_c = n_c
        i=i+1
