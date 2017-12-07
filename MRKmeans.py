"""
.. module:: MRKmeans

MRKmeans
*************

:Description: MRKmeans

    Iterates the MRKmeansStep script

:Authors: bejar


:Version:

:Created on: 17/07/2017 10:16

"""
from __future__ import print_function, division

from MRKmeansStep import MRKmeansStep
import shutil
import argparse
import os
import time
import filecmp

__author__ = 'bejar'

def storePrototypesFile(prototype_dic, iteration):
    fp = open('prototypes%d.txt' % iteration, 'w')
    fa = open('assig%d.txt' % iteration, 'w')
    for key, (value,docs) in prototype_dic.items():
        docvec = ''
        docstxt = ''
        for doc_id in docs:
            docstxt += (doc_id+' ')
        fa.write(key + ':' + docstxt + '\n')
        for (word, freq) in value:
            docvec += (word+'+'+str(freq)+' ')
        str_dovec=str(docvec.encode('ascii','replace'))
        fp.write(key + ':' + str_dovec[2:-1] + '\n')
    fp.flush()
    fp.close()
    fa.flush()
    fa.close()

def compareIterations(act):
    ant = act-1
    f1 = open('assig%d.txt' % ant, 'r')
    f2 = open('assig%d.txt' % act, 'r')
    return filecmp.cmp('assig'+str(ant)+'.txt','assig'+str(act)+'.txt')

def resumePrototype(prototype,i):
    f = open('resume%d.txt' % i, 'w')
    for key, (value, doc) in prototype.items():
        value_sorted = sorted(value, key=lambda x: x[1])
        docvec = ''
        for (word, freq) in value_sorted[:5]:
            docvec += (word+'+'+str(freq)+' ')
        str_dovec=str(docvec.encode('ascii','replace'))
        f.write(key + ':' + str_dovec[2:-1] + '\n')
    f.flush()
    f.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--prot', default='prototypes.txt', help='Initial prototpes file')
    parser.add_argument('--docs', default='documents.txt', help='Documents data')
    parser.add_argument('--iter', default=5, type=int, help='Number of iterations')
    parser.add_argument('--nmaps', default=2, type=int, help='Number of parallel map processes to use')
    parser.add_argument('--nreduces', default=2, type=int, help='Number of parallel reduce processes to use')

    args = parser.parse_args()
    assign = {}

    # Copies the initial prototypes
    cwd = os.getcwd()
    shutil.copy(cwd + '/' + args.prot, cwd + '/prototypes0.txt')

    nomove = False  # Stores if there has been changes in the current iteration
    for i in range(args.iter):
        tinit = time.time()  # For timing the iterations

        # Configures the script
        print('Iteration %d ...' % (i + 1))
        # The --file flag tells to MRjob to copy the file to HADOOP
        # The --prot flag tells to MRKmeansStep where to load the prototypes from
        mr_job1 = MRKmeansStep(args=['-r', 'local', args.docs,
                                     '--file', cwd + '/prototypes%d.txt' % i,
                                     '--prot', cwd + '/prototypes%d.txt' % i,
                                     '--jobconf', 'mapreduce.job.maps=%d' % args.nmaps,
                                     '--jobconf', 'mapreduce.job.reduces=%d' % args.nreduces])

        # Runs the script
        with mr_job1.make_runner() as runner1:
            runner1.run()
            new_assign = {}
            new_proto = {}
            # Process the results of the script, each line one results
            for line in runner1.stream_output():
                key, value = mr_job1.parse_output_line(line)
                # You should store things here probably in a datastructure
                new_proto[key] = value
            # If your scripts returns the new assignments you could write them in a file here
            for key, (value,docs) in new_proto.items():
                new_assign[key] = docs
            if bool(assign):
                for key, (value,docs) in new_proto.items():
                    if set(assign[key])!=set(docs):
                        break
                else:
                    nomove=True
            assign = new_assign.copy()

            # You should store the new prototypes here for the next iteration
            storePrototypesFile(new_proto, i+1)
            resumePrototype(new_proto,i)
            # If you have saved the assignments, you can check if they have changed from the previous iteration
        print("Time= %f seconds" % (time.time() - tinit))

        if nomove:  # If there is no changes in two consecutive iteration we can stop
            print("Algorithm converged")
            break

    # Now the last prototype file should have the results
