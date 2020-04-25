
def main(sc)

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
        boro_idx, boro = createIndex('tmp/bdm/boroughs.geojson')  
        nei_idx, neighbo = createIndex('tmp/bdm/neighborhoods.geojson')    
        
        # Skip the header
        if pid==0:
            next(records)
        reader = csv.reader(records)
        counts = {}
        
        for row in reader:
            if row[9] == 'NULL':
                continue
            pickup = geom.Point(proj(float(row[6]), float(row[7])))
            borough = findZone(pickup, boro_idx, boro)
            
            dropoff = geom.Point(proj(float(row[10]), float(row[11])))
            neighborhood = findZone(dropoff, nei_idx, neighbo)
            
            try:
                if borough * neighborhood > 0:
                    boro_name= boro['boro_name'][borough]
                    neighbo_name = neighbo['neighborhood'][neighborhood]
                    combined = (boro_name, neighbo_name)
                    counts[combined] = counts.get(combined, 0) + 1
                else:
                    pass
            except:
                pass
        return counts.items()

    def sort(counts):
        bk_counts = [item for item in counts if item[0][0] == 'Brooklyn']
        mn_counts = [item for item in counts if item[0][0] == 'Manhattan']
        bx_counts = [item for item in counts if item[0][0] == 'Bronx']
        qn_counts = [item for item in counts if item[0][0] == 'Queens']
        si_counts = [item for item in counts if item[0][0] == 'Staten Island']

        bx_counts = sorted(bx_counts, key=lambda t: (t[1]), reverse = True)[0:3]
        bk_counts = sorted(bk_counts, key=lambda t: (t[1]), reverse = True)[0:3]
        mn_counts = sorted(mn_counts, key=lambda t: (t[1]), reverse = True)[0:3]
        qn_counts = sorted(qn_counts, key=lambda t: (t[1]), reverse = True)[0:3]
        si_counts = sorted(si_counts, key=lambda t: (t[1]), reverse = True)[0:3]
        
        total = bx_counts + bk_counts + mn_counts + qn_counts + si_counts
        return total

    def format_csv(triplist):
        count = 0
        line = []
        csv_lines = []
        for record in triplist:
            if count == 0:
                line.append(record[0][0])
                line.append(record[0][1])
                line.append(record[1])
                count += 1
            else:
                line.append(record[0][1])
                line.append(record[1])
                count += 1
            if count == 3:
                csv_lines.append(line)
                line = []
                count = 0
        return csv_lines


    rdd = sc.textFile('tmp/bdm/yellow_tripdata_2011-05.csv')
    counts = rdd.mapPartitionsWithIndex(processTrips) \
             .reduceByKey(lambda x,y: x+y) \
             .sortBy(lambda x: x[1]) \
             .collect()

    sorted_counts = sort(counts)
    csv_lines = format_csv(sorted_counts)

    import csv
    with open('output.csv', 'w', header=False) as f:
        wr = csv.writer(f)
        wr.writerows(csv_lines) 

if __name__ == "__main__":
    from pyspark import SparkContext
    from pyspark.sql.session import SparkSession
    sc = SparkContext()
    spark = SparkSession(sc)
    main(sc)

