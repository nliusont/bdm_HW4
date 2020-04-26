from pyspark import SparkContext
import sys

def createIndex(geojson):
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(geojson).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None

def processTrips(pid, records):
    import csv
    import pyproj
    import shapely.geometry as geom
    # Create an R-tree index
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    boro_idx, boro = createIndex('boroughs.geojson')  
    nei_idx, neighbo = createIndex('neighborhoods.geojson')    

    # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}

    for row in reader:
        try:
            if row[9] == 'NULL':
                continue
            pickup = geom.Point(proj(float(row[5]), float(row[6])))
            borough = findZone(pickup, boro_idx, boro)

            dropoff = geom.Point(proj(float(row[9]), float(row[10])))
            neighborhood = findZone(dropoff, nei_idx, neighbo)

            boro_name= boro['boro_name'][borough]
            neighbo_name = neighbo['neighborhood'][neighborhood]
            combined = (boro_name, neighbo_name)
            counts[combined] = counts.get(combined, 0) + 1
        except:
            pass
    return counts.items()




with open('output.csv', 'w', header=False) as f:
    wr = csv.writer(f)
    wr.writerows(counts) 

if __name__ == "__main__":
    sc = SparkContext()
    inputcsv = sys.argv[1]
    rdd = sc.textFile(inputcsv)
    counts = rdd.mapPartitionsWithIndex(processTrips) \
                .reduceByKey(lambda x,y: x+y) \
                .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                .sortBy(lambda x: x[1][1], ascending=False) \
                .reduceByKey(lambda x,y: x+y) \
                .sortByKey() \
                .map(lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4], x[1][5])) \
                .collect()
    counts.saveAsTextFile(sys.argv[2])

