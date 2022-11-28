# reading.py
#################################################
# to execute use this script:
# $ ./spark-submit --packages "com.github.astrolabsoftware:spark-fits_2.12:1.0.0" --master spark://saul-pc:7077 /media/saul/Saul\'s\ File/UCSP/BigData/Labs/SparkFits/reading.py -inputpath /media/saul/Saul\'s\ File/UCSP/BigData/Labs/SparkFits/spark-fits/src/test/resources/test_file.fits
##################################################

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
    
    gal.printSchema()
    
    gal.show()
    
    print("number of entries: ", gal.count())
    
    gal.describe(["RA", "Dec"]).show()