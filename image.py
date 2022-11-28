# image.py
#################################################
# to execute use this script:
# $ ./spark-submit --packages "com.github.astrolabsoftware:spark-fits_2.12:1.0.0" --master spark://saul-pc:7077 /media/saul/Saul\'s\ File/UCSP/BigData/Labs/SparkFits/image.py -inputpath /media/saul/Saul\'s\ File/UCSP/BigData/Labs/SparkFits/spark-fits/src/test/resources/test_file.fits
##################################################
## Let's convert our data into healpix index
import healpy as hp
import numpy as np
## Let's gather the result per-pixel (hit count), and plot the resulting map
import matplotlib.pyplot as pl
from pyspark.sql import SparkSession

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

def dec2theta(dec):
    """
    Convert Declination into Theta
    Parameters
    ----------
    dec : Double
        Declination angle (in radian)
    Returns
    ----------
    theta : Double
        Theta angle (in radian)
    """
    return np.pi / 2. - dec

def ra2phi(ra):
    """
    Convert RA into Phi
    Parameters
    ----------
    ra : Double
        Right Ascension angle (in radian)
    Returns
    ----------
    phi : Double
        Phi angle (in radian)
    """
    return ra

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
    nside = 16
    ## Pyspark DataFrames do not have "map" method - so you need
    ## to convert it to RDD first.
    rdd = gal.select(gal["RA"], gal["Dec"])\
        .rdd\
        .map(lambda x: hp.ang2pix(nside, dec2theta(x[1]), ra2phi(x[0])))
    ## Show the first 10 pixels
    print(rdd.take(10))

    hitcount = rdd.map(lambda x: (x, 1))
    myPartialMap = hitcount.countByKey()

    myMap = np.zeros(12 * nside**2)
    myMap[list(myPartialMap.keys())] = list(myPartialMap.values())

    ## Fake rotation of the coordinate system of the map for visu
    hp.mollview(myMap, coord="CG")
    pl.show()