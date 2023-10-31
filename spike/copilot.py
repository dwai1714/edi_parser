import functools
import os
import subprocess
import time

import os
import time

import pandas
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

def force_async(fn):
    from concurrent.futures import ThreadPoolExecutor
    import asyncio

    pool = ThreadPoolExecutor()

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        future = pool.submit(fn, *args, **kwargs)
        return asyncio.wrap_future(future)  # make it awaitable

    return wrapper


# @force_async
def execute_transform(input_file_name, output_file_name):

    print("file name is " + input_file_name)

    print(
        subprocess.Popen(
            f"""java -jar /Users/dc/db_load_test/spike/edireader-json-basic-5.6.2.jar {input_file_name} {output_file_name} --format=no""",
            shell=True,
            stdout=subprocess.PIPE,
        ).stdout.read()
    )

def finalizer_function():
    subprocess.Popen(
        f"""rm -rf /Users/dc/edi_parser/spike/outputs/*""",
        shell=True,
        stdout=subprocess.PIPE,
    )
    import glob
    file_list = glob.iglob("/Users/dc/edi_parser/spike/inputs/data/ASC_X12/005010/Technical_Reports/Type_3/Finals/Examples" + '**/**', recursive=True)
    file_list = [f for f in file_list if os.path.isfile(f)]
    print("Number of files " + str(len(file_list)))
    for input_file in file_list:
        head, tail = os.path.split(input_file)
        execute_transform(input_file,f"/Users/dc/edi_parser/spike/outputs/{tail}.json")

def parse_single_file(input_file_name):
    head, tail = os.path.split(input_file_name)
    execute_transform(input_file_name, f"/Users/dc/edi_parser/spike/dc_output/{tail}.json")

if __name__ == "__main__":
    start_time = time.monotonic()

    spark = (
        SparkSession.builder.appName("redact")
        .master("local[*]")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.driver.memory", "28g")
        .config('spark.jars.packages',
                'org.postgresql:postgresql:42.2.9.jre7')
        .getOrCreate()
    )
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    df = spark.read.json("/Users/dc/edi_parser/spike/outputs/X221-multiple-claims-single-check.edi.json")
    df.printSchema()
    df.select("interchanges").show()