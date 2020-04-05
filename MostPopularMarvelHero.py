from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("MostPopularMarvelhero")
sc = SparkContext(conf=conf)


def MarvelHeroName():
    with open("C:/GitHub/PYSPARK/Pyspark/Data/Marvelnames.txt", encoding='utf-8') as f:
        heros = {}
        for lines in f:
            hero_id = int(lines.split('\"')[0])
            heroname = lines.split('\"')[1].encode('utf-8').decode('utf-8')
            heros[hero_id] = heroname
    return heros


def countofoccurance(lines):
    hero_id = int(lines.split()[0])
    occurance = len(lines.split()) - 1
    return (hero_id, occurance)


MarvelOccurInput = sc.textFile("C:/GitHub/PYSPARK/Pyspark/Data/MarvelGraph.txt")
marvel_heros_name = sc.broadcast(MarvelHeroName())
maximumOc = MarvelOccurInput.map(countofoccurance).reduceByKey(lambda x, y: (x + y)).map(
    lambda x: (x[1], marvel_heros_name.value[x[0]])).max()

print(maximumOc[1], "Appeared  " , maximumOc[0], "Times in marvel history ")
