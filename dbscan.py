#!/usr/bin/env python
#-*-coding:utf-8-*-

import math


class DBSCAN(object):
    
    def __init__(self, Eps=1.0, MinPts=3):
        self.Eps = Eps
        self.MinPts = MinPts
    
    def setEps(self, Eps):
        self.Eps = Eps

    def setMinPts(self, MinPts):
        self.MinPts = MinPts
    
    @staticmethod
    def distanceFunc(p1, p2):    
        squareOfDiff = [(p1[i]-p2[i])**2 for i in range(len(p1))]
        return math.sqrt(sum(squareOfDiff)) 
    
    #Retrieve the neighbors of (dataitem) in dataset
    def _retrieve_neighbors(self, dataid, dataset):
        #TO DO
        result = []
        for i in range(len(dataset)):
            if self.distanceFunc(dataset[i], dataset[dataid]) < self.Eps:
                result.append(i)

        return result

    def cluster(self, dataset):
        
        numOfDataitems = len(dataset)
        clusterResult = [-1 for i in range(numOfDataitems)]
        newClusterId = 1
        def _expand_cluster(objId):

            neighbors = self._retrieve_neighbors(objId, dataset)
            if len(neighbors) < self.MinPts:
                clusterResult[objId] = 0
            else:
                seeds = []
                for point in neighbors:
                    clusterResult[point] = newClusterId
                    if point != objId:
                        seeds.append(point)
                        
                while len(seeds) != 0:
                    currentObjId = seeds[-1]
                    currentNeighbors = self._retrieve_neighbors(currentObjId, dataset)
                    if len(currentNeighbors) >= self.MinPts:
                        for neighborId in currentNeighbors:
                            if clusterResult[neighborId] <= 0:
                                clusterResult[neighborId] = newClusterId 
                                seeds.append(neighborId)
                    seeds.pop()
            

        for i in range(numOfDataitems):
            if clusterResult[i] == -1:
                _expand_cluster(i)
                newClusterId += 1

        return clusterResult
    

if __name__ == "__main__":

    dataset = []
    dbscaner = DBSCAN(5.0, 4)
    with open("data",'r') as f:
        for line in f:
            dataset.append(map(float, line.strip().split()))

    for ele in dataset:
        print ele
    clusterResult = dbscaner.cluster(dataset)
    
    for i in range(len(clusterResult)):
        print clusterResult[i], dataset[i]


