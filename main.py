#! /usr/bin/env python3

from mrjob.job import MRJob
from mrjob.step import MRStep

class PageRank(MRJob):

    D = {}
    
    def mapper(self, _, line):
        # Split line
        line = line.split()

        i = int(line[0])
        j = int(line[1])

        yield None, ((i, 1), (j, 1))

    def reducer(self, _, values):
        # Create tuple indexed by i, and with value : list of (j, nj) where j->i and nj is the number of links of j, ni is the number of links of i, w_i0 is the initial weight of i, w_k is the weight of i at the kth iteration
        d = {}
        inv_d = {}

        for (i, _), (j, _) in values:

            #index i : list of [i->j]
            if i not in d:
                d[i] = []

            d[i].append((j))

            if j not in inv_d:
                inv_d[j] = []
            
            inv_d[j].append(i)
        
        # on a donc un dico qui index i : list de i->j ie la liste des cites que i pointe
        # et un dico qui index i: j->i ie la liste des sites qui pointent vers i
        # creons maintenant un dico finale telque : {i : ni, w_i0, w_k, inv_d[i]}

        #count unique index in both d and inv_d
        N = len(set(d.keys()) | set(inv_d.keys()))
        
        for i in d:
            
            ni = len(d[i])
            w_i0 = 1/N
            w_k = 1/N
            jtoi = inv_d.get(i, [])

            self.D[i] = (ni, w_i0, w_k, jtoi, N)

        # yield the tuple
        for i in self.D:
            yield i, self.D[i]

    def reducer2(self, i, values):
        """ Calcul du poids w_k+1, step qui sera répété 10 fois """
        for ni, w_i0, w_k, jtoi, N in values:

            w_kp1 = 0
            
            for j in jtoi:
                #recupere le site j
                nj, _, w_jk, _, N = self.D.get(j, (1, 1/N, 1/N, [], N))
                w_kp1 += 0.85* w_jk/nj #if nj != 0 else 0

            w_kp1 += 0.15/N

            self.D[i] = (ni, w_i0, w_kp1, jtoi, N)

        yield i, self.D[i]

    def reducer4(self, i, values):
        # yield i, w_k
        for ni, w_i0, w_k, jtoi, N in values:
            yield None, (w_k, i)
        
    def reducer5(self, _, values):
        # yield i, w_k with w_k sorted descending
        ordered_val = sorted(values, reverse=True)[:10]
        for w_k, i in ordered_val:
            yield i, w_k

    


    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)] + [MRStep(reducer=self.reducer2)]*10 + [MRStep(reducer=self.reducer4)] + [MRStep(reducer=self.reducer5)]

if __name__ == '__main__':
    PageRank.run()