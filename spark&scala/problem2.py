from pyspark import SparkContext

sc = SparkContext()
Path = "hdfs://localhost:9000/user/bxing/Points.csv"
points = sc.textFile(Path)


## Get the cellID
def getCellID(x, y):
    cell_x = int((x - 1) / 20) + 1
    cell_y = int((y - 1) / 20) + 1
    return cell_x + 500 * (500 - cell_y)


## clockwise
def getNeighbour(id):
    upLeft = id - 501
    up = id - 500
    upRight = id - 499
    right = id + 1
    lowRight = id + 501
    low = id + 500
    lowLeft = id + 499
    left = id - 1

    ## boundary condition
    if (id == 0):
        res = [right, lowRight, low]
        return res
    if (id == 500):
        res = [low, lowLeft, left]
        return res
    if (id == 249501):
        res = [up, upRight, right]
        return res
    if (id == 250000):
        res = [upLeft, up, left]
        return res
    elif (id % 500 == 1):
        res = [up, upRight, right, lowRight, low]
        return res
    elif (id % 500 == 0):
        res = [upLeft, up, low, lowLeft, left]
        return res
    else:
        res = [upLeft, up, upRight, right, lowRight, low, lowLeft, left]
        return res


## (cellId, count of points) in countPerCell
point = points.map(lambda x: [int(x.split(",")[0]), int(x.split(",")[1])])
point.take(2)
countPerCell = point.map(lambda x: (getCellID(x[0], x[1]), 1)).reduceByKey(lambda x, y: x + y)
countPerCell.take(2)

## (cellId, count of Neighbours) count neighbours
neighboursCount = countPerCell.map(lambda x: [x[0], len(getNeighbour(x[0]))])
neighboursCount.take(2)


## (cellID, [neighbourID, neighbourcount])calculate the
def flatFunc(id):
    neisID = getNeighbour(id[0])
    for neiID in neisID:
        yield (neiID, id[0])


cellNeiCount = countPerCell.flatMap(flatFunc).leftOuterJoin(countPerCell)

## (cellID, count of points in all neighbours)
def totalCount(x):
    cellID = x[1][0]
    if (x[1][1] == None):
        totalCount = 0
    else:
        totalCount = x[1][1]
    return (cellID, totalCount)


cellNeiTotalCount = cellNeiCount.map(totalCount).reduceByKey(lambda x, y: x + y)

## (cellID, average count of neighbours)
cellNeiAvgCount = cellNeiTotalCount.join(neighboursCount).map(lambda x: (x[0], (x[1][0] / x[1][1])))

## get and sort the density
cellDensity = countPerCell.join(cellNeiAvgCount).map(lambda x: (x[0], x[1][0] / x[1][1])).sortBy(lambda x: -x[1])

## top 16
topK = sc.parallelize(cellDensity.take(16))
import json
file = open("taskb.txt", "w")
file.write(json.dumps(topK.collect()))
file.close()



## report properties for taskc
## (cellID, density-score, number of neighbours, neighbout-id, relative-density-score-of neighbours)
def firstpart(x):
    neisID = getNeighbour(x[0])
    for neiID in neisID:
        yield (neiID, (x[0], x[1], len(neisID)))


def secondpart(x):
    neiID = x[0]
    cellID = x[1][0][0]
    densityScore = x[1][0][1]
    neisCount = x[1][0][2]
    neisDensity = x[1][1]

    return cellID, densityScore, neisCount, neiID, neisDensity


res = topK.flatMap(firstpart).leftOuterJoin(cellDensity).map(secondpart).sortBy(lambda x: x[0])
# print(res.collect())
file = open("taskc.txt", "w")
file.write(json.dumps(res.collect()))
file.close()

## taskd
# part1

popCount = countPerCell.map(lambda x: reversed(x)).groupByKey()

def getCount(x):
    l = len(list(x[1]))
    return x[0], l, list(x[1])


popCount = popCount.map(getCount).sortBy(lambda x: x[0])

file = open("popCount.txt", "w")
file.write(json.dumps(popCount.collect()))
file.close()

def getPair(x):
    samePopPoints = list(x[2])
    l = len(samePopPoints)
    lst = []
    for i in range(l):
        for j in range(i + 1, l):
            if samePopPoints[j] in getNeighbour(samePopPoints[i]):
                tmp = [i, j]
                lst.append(tmp)
    if len(lst) > 0:
        return x[0], len(lst), lst
    else:
        return x[0], 0, []

popNeig = popCount.map(getPair).sortBy(lambda x: -x[1])

file = open("popNeig.txt", "w")
file.write(json.dumps(popNeig.collect()))
file.close()





