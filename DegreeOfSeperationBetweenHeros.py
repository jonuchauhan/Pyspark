from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("DegreeOfSeperationBetweenHeros")
sc = SparkContext(conf=conf)

startCharacterID = 5988  # SpiderMan
targetCharacterID = 5485     # ADAM 3,031 (who?)

counter = sc.accumulator(0)


def convertingNodes(lines):
    fields = lines.split()
    character = int(fields[0])

    connections = []

    for connection in fields[1:]:
        connections.append(int(connection))
        color = "White"
        Distance = 9999
    if int(character) == startCharacterID:
        color = "Grey"
        Distance = 0

    return (character, (connections, Distance, color))


def creatrdd():
    input = sc.textFile("C:/GitHub/PYSPARK/Pyspark/Data/MarvelGraph - Copy.txt")
    return input.map(convertingNodes)


def bfsmap(node):
    startCharacterID = node[0]
    nodeconnections = node[1]
    connections = nodeconnections[0]
    color = nodeconnections[2]
    distance = nodeconnections[1]

    results = []
    if color == "Grey":
        for conn in connections:
            newcharacterid = int(conn)
            newdistance = distance + 1
            if conn == targetCharacterID:
                counter.add(newdistance)
            newentry = (newcharacterid, ([], newdistance, "Grey"))
            results.append(newentry)
        color = 'BLACK'
    results.append((startCharacterID, (connections, distance, color)))
    return results


iterationrdd = creatrdd()

for iteration in range(0, 30):
    print("Running BFS iteration# " + str(iteration + 1))

    mapped = iterationrdd.flatMap(bfsmap)


    print("Processing " + str(mapped.count()) + " values.")

    if counter.value > 0:
        print("Hit the target character! From " + str(counter.value) \
              + " different direction(s).")
        break
