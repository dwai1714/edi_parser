import functools
import glob
import json
import os
import subprocess
import time
import shutil

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
def get_root_path():
    return ROOT_DIR

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
            f"""java -jar {get_root_path()}/edireader-json-basic-5.6.2.jar {input_file_name} {output_file_name} --format=no""",
            shell=True,
            stdout=subprocess.PIPE,
        ).stdout.read()
    )

def read_directory(dir_name):
    dir_list = os.listdir(dir_name)
    print(dir_list)
    return dir_list


def finalizer_function():
    import glob
    dir_name = f"{get_root_path()}/data_based_on_type/"
    dir_list = read_directory(dir_name)
    for dir in dir_list:
        file_list = glob.iglob(f"{get_root_path()}/data_based_on_type/{dir}" + '**/**', recursive=True)
        file_list = [f for f in file_list if os.path.isfile(f)]
        print("Number of files " + str(len(file_list)))
        for input_file in file_list:
            head, tail = os.path.split(input_file)
            out_path = f"{get_root_path()}/outputs/{dir}/"
            if not os.path.exists(out_path):
                os.makedirs(out_path)
            execute_transform(input_file,f"{get_root_path()}/outputs/{dir}/{tail}.json")

def validate_if_json(file_name):
    with open(file_name) as json_file:
        try:
            json.load(json_file)
        except ValueError as e:
            print("JSON object issue: %s" % e)
            error_tup = (file_name, e)
            return error_tup
def list_bad_files(dir_name):
    error_file_list = []
    dir_list = read_directory(dir_name)
    out_path = f"{get_root_path()}/bad_json"
    if not os.path.exists(out_path):
        os.makedirs(out_path)
    for dir in dir_list:
        file_list = glob.iglob(f"{get_root_path()}/outputs/{dir}" + '**/**', recursive=True)
        file_list = [f for f in file_list if os.path.isfile(f)]
        for input_file in file_list:
            error_tup = validate_if_json(input_file)
            if error_tup is not None:
                error_file_list.append(error_tup)
                shutil.copy(input_file, f"{get_root_path()}/bad_json")

    return error_file_list


def parse_single_file(input_file_name):
    head, tail = os.path.split(input_file_name)
    execute_transform(input_file_name, f"{get_root_path()}/dc_output/{tail}.json")

if __name__ == "__main__":
    start_time = time.monotonic()
    dir_name = f"{get_root_path()}/outputs/"
    print(
        subprocess.Popen(
            f"""rm -rf {dir_name}/*.json""",
            shell=True,
            stdout=subprocess.PIPE,
        ).stdout.read()
    )
    bad_json_dir = f"{get_root_path()}/bad_json/"
    print(
        subprocess.Popen(
            f"""rm -rf {bad_json_dir}/*.json""",
            shell=True,
            stdout=subprocess.PIPE,
        ).stdout.read()
    )
    finalizer_function()
    print(list_bad_files(dir_name))
    # validate_if_json("/Users/dc/edi_parser/spike/outputs/277/X364-example-two.edi.json")
    # finalizer_function()

    # spark = (
    #     SparkSession.builder.appName("redact")
    #     .master("local[*]")
    #     .config("spark.sql.codegen.wholeStage", "false")
    #     .config("spark.driver.memory", "28g")
    #     .config('spark.jars.packages',
    #             'org.postgresql:postgresql:42.2.9.jre7')
    #     .getOrCreate()
    # )
    # spark.conf.set("spark.sql.shuffle.partitions", "200")
    # df = spark.read.json("/Users/dc/edi_parser/spike/outputs/837P")
    # df.printSchema()
    # df.select("interchanges").show()