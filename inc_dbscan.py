
#!/usr/bin/env python
#-*-coding:utf-8-*-

import math

class IncDBSCAN(object):

    def __init__(self, Eps, MinPts):
        self.Eps = Eps
        self.MinPts = MinPts
        self.dataset = []
        self.clusterId = []
        self.numOfNeighbors = []
        self.nextClusterId = 1
        self.numOfMemberInCluster = {}
    
    def _retrieve_neighbors(self, point):
        '''
        >>> dbscaner = IncDBSCAN(3.0, 3)
        >>> dbscaner.dataset = [[0.0,0.0], [3.0,4.0], [1.0,1.0], [2.0,3.0], [0.3,0.4]]
        >>> print dbscaner._retrieve_neighbors(0)
        [0, 2, 4]
        '''
        neighbors = []
        for i in range(len(self.dataset)):
            if self._isNeighbor(i, point):
                neighbors.append(i)

        return neighbors


    @staticmethod
    def distanceFunc(p1, p2):
        '''
        >>> p1 = [3, 4]
        >>> p2 = [0, 0]
        >>> IncDBSCAN.distanceFunc(p1, p2)
        5.0
        >>> p1 = [5, 6]
        >>> p2 = [2, 2]
        >>> IncDBSCAN.distanceFunc(p1, p2)
        5.0
        >>> p1 = [7, 10, 13]
        >>> p2 = [4, 14, 25]
        >>> IncDBSCAN.distanceFunc(p1, p2)
        13.0
        '''
        squareOfDiff = [(p1[i] - p2[i])**2 for i in range(len(p1))]
        return math.sqrt(sum(squareOfDiff))

    def _updateNumOfNeighbors(self, neighbors):
        '''
        >>> dbscaner = IncDBSCAN(1, 3)
        '''
        for PtId in neighbors:
            self.numOfNeighbors[PtId] += 1

        self.numOfNeighbors[-1] = len(neighbors)
    
    def _isNeighbor(self, PtId1, PtId2):
        if self.distanceFunc(self.dataset[PtId1], self.dataset[PtId2]) < self.Eps:
            return True
        else:
            return False
    
    def _expand(self, seeds):
        for PtId in seeds:
            neighbors = self._retrieve_neighbors(PtId)
            for NeighId in neighbors:
                if self.clusterId[NeighId] == 0:
                    self.clusterId[NeighId] = self.clusterId[PtId]


    def inc_cluster(self, p):

        self.dataset.append(p)
        self.clusterId.append(0)
        neighbors = self._retrieve_neighbors(len(self.dataset)-1)
        self.numOfNeighbors.append(len(neighbors))
        UpSeeds = []
        if len(neighbors) >= self.MinPts:
            UpSeeds.append(len(self.dataset)-1)

        self._updateNumOfNeighbors(neighbors)
        clusterSetOfUpSeeds = set()
        noiseInNeighbor = {}
        for PtId in neighbors:
            if self.numOfNeighbors[PtId] == self.MinPts and PtId != len(self.dataset) - 1:
                UpSeeds.append(PtId)
                clusterSetOfUpSeeds.add(self.clusterId[PtId])

        end = len(UpSeeds)
        alreadyIn = set(UpSeeds)
        for i in range(0,end):
            currentNeighbors = self._retrieve_neighbors(UpSeeds[i])
            for PtId in currentNeighbors:
                if self.numOfNeighbors[PtId] >= self.MinPts+1 and PtId not in alreadyIn:
                    UpSeeds.append(PtId)
                    alreadyIn.add(PtId)
                    clusterSetOfUpSeeds.add(self.clusterId[PtId])
                if self.clusterId[PtId] == 0:
                    if UpSeeds[i] not in noiseInNeighbor:
                        noiseInNeighbor[UpSeeds[i]] = set()
                    noiseInNeighbor[UpSeeds[i]].add(PtId)


        #Noise
        if len(UpSeeds) == 0:
            for PtId in neighbors:
                if self.numOfNeighbors[PtId] >= self.MinPts:
                    self.clusterId[-1] = self.clusterId[PtId]
                    break

        elif len(clusterSetOfUpSeeds) == 1: 
            clusterInUpSeeds = clusterSetOfUpSeeds.pop()
            #Creation
            if clusterInUpSeeds == 0:
                visited = [0 for i in range(len(UpSeeds))]
                for i in range(len(UpSeeds)-1):
                    if visited[i] == 0:
                        visited[i] = 1
                        self.clusterId[UpSeeds[i]] = self.nextClusterId
                        self.nextClusterId += 1
                        for j in range(i+1, len(UpSeeds)):
                            if visited[j] == 0 and self._isNeighbor(UpSeeds[i], UpSeeds[j]):
                                visited[j] = 1
                                self.clusterId[UpSeeds[j]] = self.clusterId[UpSeeds[i]]
                    
            #Absorption
            else:
                self.clusterId[-1] = clusterInUpSeeds
                for PtId in UpSeeds:
                    if PtId in noiseInNeighbor:
                        for noiseId in noiseInNeighbor[PtId]:
                            self.clusterId[noiseId] = clusterInUpSeeds
        else:
            #Merge

            if 0 in clusterSetOfUpSeeds:
                visited = [0 for i in range(len(UpSeeds))]
                for i in range(len(UpSeeds)-1):
                    if visited[i] == 0 and self.clusterId[UpSeeds[i]] == 0:
                        visited[i] = 1
                        self.clusterId[UpSeeds[i]] = self.nextClusterId
                        self.nextClusterId += 1
                        for j in range(i+1, len(UpSeeds)):
                            if visited[j] == 0 and self.clusterId[UpSeeds[j]]== 0 \
                                    and self._isNeighbor(UpSeeds[i], UpSeeds[j]):
                                visited[j] = 1
                                self.clusterId[UpSeeds[j]] = self.clusterId[UpSeeds[i]]

            for i in range(0, len(UpSeeds)-1):
                for j in range(i+1, len(UpSeeds)):
                    if self._isNeighbor(UpSeeds[i], UpSeeds[j]) \
                        and self.clusterId[UpSeeds[i]] != self.clusterId[UpSeeds[j]]:
                        self._merge(self.clusterId[UpSeeds[i]],self.clusterId[UpSeeds[j]])

            self.clusterId[-1] = self.clusterId[UpSeeds[0]]

        #Expand
        for Pt in noiseInNeighbor:
            for noisePt in noiseInNeighbor[Pt]:
                if self.clusterId[noisePt] == 0:
                    self.clusterId[noisePt] = self.clusterId[Pt]
        

    def _merge(self, clusterId1, clusterId2):
        '''
        >>> dbscaner = IncDBSCAN(3, 3)
        >>> dbscaner.clusterId = [1,1,1,3,3,3,2,2,2,1,4]
        >>> dbscaner._merge(1,3)
        >>> print dbscaner.clusterId
        [1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 4]
        >>> dbscaner._merge(1,1)
        >>> print dbscaner.clusterId
        [1, 1, 1, 1, 1, 1, 2, 2, 2, 1, 4]
        '''
        if clusterId1 == clusterId2:
            return 
        for i in range(len(self.clusterId)):
            if self.clusterId[i] == clusterId2:
                self.clusterId[i] = clusterId1 


if __name__ == "__main__":

    import doctest 
    doctest.testmod()
    dbscaner = IncDBSCAN(2.9, 3)
    counter = 0
    with open('data', 'r') as f:
        for line in f:
            if counter == 22: 
                break
            dataitem = map(float, line.strip().split())
            dbscaner.inc_cluster(dataitem)
            counter += 1

    for i in range(0, len(dbscaner.dataset)):
        print dbscaner.clusterId[i], dbscaner.dataset[i], dbscaner.numOfNeighbors[i]


