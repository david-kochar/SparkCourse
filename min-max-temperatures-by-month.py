from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationIDDate = fields[0] + fields[1]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationIDDate, entryType, temperature)

lines = sc.textFile("file:///SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: x[1] in ["TMAX", "TMIN"]) #"PRCP" not in x[1]
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
temps   = minTemps.join(maxTemps)
results = temps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1][0]) + "\t{:.2f}F".format(result[1][1]))