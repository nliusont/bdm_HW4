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
    nei_idx, neighbo = createIndex('neighborhoods.geojson')    
    
    # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    
    for row in reader:
        try:
            pickup = geom.Point(proj(float(row[5]), float(row[6])))
            pickup_neighborhood = findZone(pickup, nei_idx, neighbo)

            dropoff = geom.Point(proj(float(row[9]), float(row[10])))
            dropoff_neighborhood = findZone(dropoff, nei_idx, neighbo)

            boro_name = neighbo['borough'][pickup_neighborhood]
            neighbo_name = neighbo['neighborhood'][dropoff_neighborhood]
            
            combined = (boro_name, neighbo_name)
            counts[combined] = counts.get(combined, 0) + 1
        except:
            pass
    return counts.items()

if __name__ == "__main__":
    sc = SparkContext()
    inputcsv = sys.argv[1]
    rdd = sc.textFile(inputcsv)
    counts = rdd.mapPartitionsWithIndex(processTrips) \
                .reduceByKey(lambda x,y: x+y) \
                .sortBy(lambda x: x[1], ascending=False, numPartitions=1) \
                .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                .reduceByKey(lambda x,y: x+y) \
                .map(lambda x: (x[0], (x[1][0:6]))) \
                .sortByKey() \
                .map(lambda x: (x[0]) + ',' + str(x[1][0]) + ',' + str(x[1][1]) + ',' + str(x[1][2]) + ',' + str(x[1][3]) + ',' + str(x[1][4]) + ',' + str(x[1][5]))

    counts.saveAsTextFile(sys.argv[2])

