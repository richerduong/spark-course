from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSpentByCustomer")
sc = SparkContext(conf = conf)

def parseLine(line):
  fields = line.split(',')
  customerID = int(fields[0])
  dollarAmount = float(fields[2])
  return (customerID, dollarAmount)

lines = sc.textFile("file:///Users/steam/repos/spark-course/customer-orders.csv")
parsedLines = lines.map(parseLine)
totalByCustomer = parsedLines.reduceByKey(lambda x, y: x + y)
results = totalByCustomer.map(lambda x: (x[1], x[0])).sortByKey().collect()
for result in results:
  print(result)