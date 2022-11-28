# reading.py
#################################################
# to execute use this script:
# $ ./spark-submit --packages "com.github.astrolabsoftware:spark-fits_2.12:1.0.0" --master spark://saul-pc:7077 /media/saul/Saul\'s\ File/UCSP/BigData/Labs/SparkFits/reading.py -inputpath /media/saul/Saul\'s\ File/UCSP/BigData/Labs/SparkFits/spark-fits/src/test/resources/test_file.fits
##################################################

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType

import argparse

def quiet_logs(sc, log_level="ERROR"):
    """
    Set the level of log in Spark.
    Parameters
    ----------
    sc : SparkContext
        The SparkContext for the session
    log_level : String [optional]
        Level of log wanted: INFO, WARN, ERROR, OFF, etc.
    """
    ## Get the logger
    logger = sc._jvm.org.apache.log4j

    ## Set the level
    level = getattr(logger.Level, log_level, "INFO")

    logger.LogManager.getLogger("org"). setLevel(level)
    logger.LogManager.getLogger("akka").setLevel(level)

def addargs(parser):
    """ Parse command line arguments for readfits """

    ## Arguments
    parser.add_argument(
        '-inputpath', dest='inputpath',
        required=True,
        help='Path to a FITS file or a directory containing FITS files')

    parser.add_argument(
        '-log_level', dest='log_level',
        default="ERROR",
        help='Level of log for Spark. Default is ERROR.')


if __name__ == "__main__":
    """
    Read the data from a FITS file using Spark,
    and show the first rows and the schema.
    """
    parser = argparse.ArgumentParser(
        description='Distribute the data of a FITS file.')
    addargs(parser)
    args = parser.parse_args(None)

    spark = SparkSession\
        .builder\
        .getOrCreate()

    ## Set logs to be quiet
    quiet_logs(spark.sparkContext, log_level=args.log_level)

    gal = spark.read.format("fits")\
            .option("hdu", 1)\
            .load(args.inputpath)\
            .select("RA", "Dec")
    
    minmax = gal.select(F.min("RA"), F.max("RA"))
    minRA = minmax[0]
    maxRA = minmax[1]
    Nbins = 100
    dRA = (maxRA - minRA) / Nbins

    binRA = gal.select(gal.RA,\
            (( gal.RA-minRA-dRA/2)/dRA).astype("int")\
            .alias("bin"))

    h = binRA.groupBy("bin")\
        .count()\
        .orderBy(F.asc("bin"))
    
    pd= h.select("bin",\
        (minRA+dRA/2+h.bin*dRA).alias("RAbin") ,\
        "count")\
        .drop("bin")\
        .toPandas()

    binNum = F.udf(lambda z: int ((z-minRA-dRA/2)/dRA))
    RAbin = gal.select(gal.RA,\
    binNum(gal.RA)\
        .alias("bin"))

    @pandas_udf ("float", PandasUDFType.SCALAR)
    def binNumber(z):
        return pd.Series((z-zmin)/dz)
    zbin=gal.select (gal.z,\
    binNumber("z").astype("int")\
        .alias("bin"))