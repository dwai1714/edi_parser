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

    import glob
    file_list = glob.iglob("/Users/dc/edi_parser/spike/data_based_on_type/837P" + '**/**', recursive=True)
    file_list = [f for f in file_list if os.path.isfile(f)]
    print("Number of files " + str(len(file_list)))
    for input_file in file_list:
        head, tail = os.path.split(input_file)
        execute_transform(input_file,f"/Users/dc/edi_parser/spike/outputs/837P/{tail}.json")

def parse_single_file(input_file_name):
    head, tail = os.path.split(input_file_name)
    execute_transform(input_file_name, f"/Users/dc/edi_parser/spike/dc_output/{tail}.json")

if __name__ == "__main__":
    start_time = time.monotonic()
    # subprocess.Popen(
    #     f"""rm -rf /Users/dc/edi_parser/spike/outputs/277/*""",
    #     shell=True,
    #     stdout=subprocess.PIPE,
    # )
    # finalizer_function()

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
    df = spark.read.json("/Users/dc/edi_parser/spike/outputs/837P")
    df.printSchema()
    df.select("interchanges").show()