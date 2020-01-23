# K-means-in-pyspark
iterative k-means algorithm using Pyspark

input should be the format of : 'latitude longtitude'
usage: kmeans filePath measure_type cluster_num

where filePath is the dataset path, measure_type is either "EuclideanDistance" or "GreatCircleDistance", cluster_num is the number of clusters you want.
if duplicate points within dataset, the algorithm might crash.
