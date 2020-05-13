from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinMaxTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    date      = str(fields[1])
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9/5) + 32
    return (stationID, date, entryType, temperature)

lines = sc.textFile("file:///SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
allTemps = parsedLines.filter(lambda x: "PRCP" not in x[1])
stationDailyTemps = allTemps.map(lambda x: ((x[0], x[1]), (x[3], x[3])))
stationDailyMinMaxTemps = stationDailyTemps.reduceByKey(lambda x, y: (min(x[0], y[0]), max(x[1],y[1])))
results = stationDailyMinMaxTemps.collect()

for result in results:
    print(result[0][0] + "  " + result[0][1] + "\t{:.2f}F".format(result[1][0]) + "\t{:.2f}F".format(result[1][1]))
