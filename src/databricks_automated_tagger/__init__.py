from pyspark.sql.functions import expr, col, udf 
from pyspark.sql.functions import count, desc, first
from functools import reduce
from datetime import datetime
from pyspark.sql.types import StringType
import pyspark as spark
import spacy

nlp = spacy.load("en_core_web_sm")
