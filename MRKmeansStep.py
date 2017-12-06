"""
.. module:: MRKmeansDef

MRKmeansDef
*************

:Description: MRKmeansDef



:Authors: bejar


:Version:

:Created on: 17/07/2017 7:42

"""

from __future__ import division
from mrjob.job import MRJob
from mrjob.step import MRStep

__author__ = 'bejar'

class MRKmeansStep(MRJob):
    prototypes = {}

    def jaccard(self, prot, doc):
        """
        Compute here the Jaccard similarity between  a prototype and a document
        prot should be a list of pairs (word, probability)
        doc should be a list of words
        Words must be alphabeticaly ordered

        The result should be always a value in the range [0,1]
        """
        intersection = sum([prob for (word,prob) in prot if (word in doc)])
        sq_prot = sum([prob*prob for (_,prob) in prot])
        union = sq_prot + len(doc) -  intersection
        return intersection/union

    def configure_options(self):
        """
        Additional configuration flag to get the prototypes files

        :return:
        """
        super(MRKmeansStep, self).configure_options()
        self.add_file_option('--prot')

    def load_data(self):
        """
        Loads the current cluster prototypes

        :return:
        """
        f = open(self.options.prot, 'r')
        for line in f:
            cluster, words = line.split(':')
            cp = []
            for word in words.split():
                cp.append((word.split('+')[0], float(word.split('+')[1])))
            self.prototypes[cluster] = cp

    def assign_prototype(self, _, line):
        """
        This is the mapper it should compute the closest prototype to a document

        Words should be sorted alphabetically in the prototypes and the documents

        This function has to return at list of pairs (prototype_id, document words)

        You can add also more elements to the value element, for example the document_id
        """

        # Each line is a string docid:wor1 word2 ... wordn
        doc, words = line.split(':')
        lwords = words.split()

        #
        # Compute map here
        #
        prot_i = 0
        distance_min = 2
        for i, prot in enumerate(self.prototypes):
            distance = self.jaccard(prot, lwords)
            if distance < distance_min:
                prot_i = i
                distance_min = distance

        yield "CLASS"+prot_i, line

    def aggregate_prototype(self, key, values):
        """
        input is cluster and all the documents it has assigned
        Outputs should be at least a pair (cluster, new prototype)

        It should receive a list with all the words of the documents assigned for a cluster

        The value for each word has to be the frequency of the word divided by the number
        of documents assigned to the cluster

        Words are ordered alphabetically but you will have to use an efficient structure to
        compute the frequency of each word

        :param key:
        :param values:
        :return:
        """

        n_docs = len(values)
        prot = []
        for line in values:
            _, words = line.split(':')
            lwords = words.split()
            i=0
            j=0
            while i<len(prot) and j<len(lwords):
                if prot[i][0]<lwords[j]:
                elif prot[i][0]==lwords[j]:
                    prot[i][1]+=1
                    j+=1
                else:
                    prot.insert(i,(lwords[j],1))
                    j+=1
                i+=1
            for k in range(j,len(lwords)):
                prot.append((lwords[k],1))
        prot = [(w,p/n_docs) for (w,p) in prot]

        yield key, prot

    def steps(self):
        return [MRStep(mapper_init=self.load_data, mapper=self.assign_prototype,
                       reducer=self.aggregate_prototype)
            ]


if __name__ == '__main__':
    MRKmeansStep.run()
