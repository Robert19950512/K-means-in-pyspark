import sys
import re
from pyspark import SparkContext
from pyspark import SparkConf
import math
import random

def closestPoint(point, centers, distanceType):
  index = 0
  returnIndex = index
  curMin = sys.float_info.max
  for center in centers:
    if distanceType == 1:
      if EuclideanDistance(point,center) < curMin:
        returnIndex = index
        curMin = EuclideanDistance(point, center)
    else :
      if GreatCircleDistance(point,center) < curMin:
        returnIndex = index
        curMin = GreatCircleDistance(point, center)
    index = index + 1
  return returnIndex

def centerDistance(oldcenters , newcenters, distanceType):
  aggregate = 0.0
  for i in range(0,len(oldcenters)):
    if distanceType == 1:
      aggregate = aggregate + EuclideanDistance(oldcenters[i],newcenters[i])
    else :
      aggregate = aggregate + GreatCircleDistance(oldcenters[i],newcenters[i])
  print aggregate
  return aggregate

def addPoints(point1, point2):
  return [point1[0] + point2[0] ,point1[1] + point2[1], point1[2], point1[3] + point2[3]]

def EuclideanDistance(point1, point2):
  return math.sqrt(pow(point1[0] - point2[0],2) + pow(point1[1] - point2[1],2))

def GreatCircleDistance(point1, point2):
  #point1[0] = math.radians(point1[0])
  #point1[1] = math.radians(point1[1])
  #point2[0] = math.radians(point2[0])
  #point2[1] = math.radians(point2[1])
  #difflat = abs(point1[0]-point2[0])
  #n1 = math.pow(math.cos(math.radians(point2[0])) * math.sin(diffLong),2)
  #n2 = math.pow(math.cos(math.radians(point1[0])) * math.sin(math.radians(point2[0])) - math.sin(math.radians(point1[0])) * math.cos(math.radians(point2[0])) * math.cos(diffLong),2)
  #d1 = math.sin(math.radians(point1[0])) * math.sin(math.radians(point2[0]))
  #d2 = math.cos(math.radians(point1[0])) * math.cos(math.radians(point2[0])) * math.cos(diffLong)
  #n1 = math.pow(math.cos(point2[0]) * math.sin(diffLong),2)
  #n2 = math.pow(math.cos(point1[0]) * math.sin(point2[0]) - math.sin(point1[0]) * math.cos(point2[0]) * math.cos(diffLong),2)
  #d1 = math.sin(point1[0]) * math.sin(point2[0])
  #d2 = math.cos(point1[0]) * math.cos(point2[0]) * math.cos(diffLong)
  #numerater = math.sqrt(n1 + n2)
  #denominator = d1 + d2
  #return math.atan(numerater / denominator)
  diffLong = abs(math.radians(point2[1])-math.radians(point1[1]))
  difflat = abs(math.radians(point2[0]) - math.radians(point1[0]))
  return 2 * math.asin(math.sqrt(pow(math.sin(difflat / 2),2) + math.cos(math.radians(point1[0])) * math.cos(math.radians(point2[0])) * math.pow(math.sin(diffLong / 2),2)))

if __name__ == "__main__":
  if len(sys.argv) < 4:
    print >> sys.stderr, "Usage: kmeans <file> measure_type cluster_num"
    exit(-1)

  sconf = SparkConf().setAppName("k-means algorithm").set("spark.ui.port","4141")
  sc = SparkContext(conf=sconf)
  k = int(sys.argv[3])
  convergeDist = 0.01
  measure = 0
  if sys.argv[2] == "EuclideanDistance":
    measure = 1
  else:
    measure = 2

  sc.setCheckpointDir("hdfs:/user/cloudera/RDDcheckpoints")
  rawData = sc.textFile(sys.argv[1])
  rawData = rawData.map(lambda record: [float(record.split()[0]),float(record.split()[1]),0])
  #print sc.textFile(sys.argv[1]).take(2)
  #rawData = sc.textFile(sys.argv[1]).map(lambda record: [float(record.split()[0]),float(record.split()[1]), 0]).filter(lambda record : record[0] >= 25 and record[0] <= 45 and record[1] <= -65 and record[1] >= -135).persist()
  # initialzie first k centers to be the top k elements in my raw Data
  centers = rawData.take(k)
  oldcenters = []
  checkPointCount = 0
  while oldcenters == [] or centerDistance(oldcenters, centers, measure) > convergeDist:
    print "new iteration"
    if oldcenters != []:
      print centerDistance(oldcenters, centers, measure)
    newData = rawData.map(lambda record: [record[0], record[1], closestPoint(record,centers,measure),1])
    oldcenters = centers
    centersRDD = newData.keyBy(lambda record: record[2]).reduceByKey(addPoints) \
      .map(lambda (key,points): (key,[points[0] / points[3], points[1] / points[3]], key, 1)) \
      .sortByKey(ascending=True, numPartitions=1).values()
    checkPointCount = checkPointCount + 1
    if checkPointCount == 5:
      checkPointCount = 0
      newData.checkpoint()
      centersRDD.checkpoint()
    centers = centersRDD.collect()
    pointsRdd = newData.map(lambda record:[record[0],record[1],record[2]])
  centersRDD.saveAsTextFile("file:/home/cloudera/Desktop/testcenters")
  pointsRdd.saveAsTextFile("file:/home/cloudera/Desktop/clusterPoints")
  sc.stop()
