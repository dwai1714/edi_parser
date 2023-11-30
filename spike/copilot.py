import functools
import glob
import json
import os
import subprocess
import time
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

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
            execute_transform(input_file, f"{get_root_path()}/outputs/{dir}/{tail}.json")


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
                destination = os.path.join(get_root_path(), "bad_json", os.path.basename(os.path.dirname(input_file)))
                os.makedirs(destination, exist_ok=True)
                shutil.copy(input_file, destination)

    return error_file_list


def parse_single_file(input_file_name):
    head, tail = os.path.split(input_file_name)
    execute_transform(input_file_name, f"{get_root_path()}/dc_output/{tail}.json")


def find_arrays(input_str):
    list_of_arrays = []

    line = input_str.replace(':array', '\n:array')
    line_list = line.split('\n')
    for i in line_list[:-1]:
        line_without_del = i.replace(":array<struct<", "")
        new_list = line_without_del.split(",")
        array_elem = new_list.pop()
        list_of_arrays.append(array_elem)
    print(list_of_arrays)


def loop_and_explode(dataframe, col_name):
    output_df = dataframe.withColumn(col_name, explode(col_name))
    output_df = output_df.select("*", f"{col_name}.*")
    output_df = output_df.drop(col_name)
    return output_df


def create_dataframe(spark, path):
    input_df = spark.read.json(f"{get_root_path()}/{path}")
    output_df = input_df.withColumn("interchanges", explode("interchanges"))
    output_df = output_df.select("interchanges.*")
    output_df.show(1000, False)

    array_list = ['functional_groups', 'transactions', 'segments', 'HL-2000A_loop', 'HL-2000B_loop', 'CLM-2300_loop',  ]
                  #,'LX-2400_loop' , 'NM1-2310A_loop','SBR-2320_loop', 'NM1-2330A_loop', 'NM1-2330B_loop', 'HL-2000C_loop', 'CLM-2300_loop', 'LX-2400_loop', 'NM1-2310A_loop', 'SBR-2320_loop', 'NM1-2330A_loop', 'NM1-2330B_loop', 'NM1-2010CA_loop', 'NM1-2010BA_loop', 'NM1-2010BB_loop', 'NM1-2010AA_loop', 'NM1-1000A_loop', 'NM1-1000B_loop']

    for array_elem in array_list:
        output_df = loop_and_explode(output_df, array_elem)
    output_df = loop_and_explode(output_df, "LX-2400_loop")

    output_df.printSchema()
    output_df.show(1000, False)


if __name__ == "__main__":
    start_time = time.monotonic()
    # input_str = "[('interchanges', 'array<struct<ISA_01_AuthorizationQualifier:string,ISA_02_AuthorizationInformation:string,ISA_03_SecurityQualifier:string,ISA_04_SecurityInformation:string,ISA_05_SenderQualifier:string,ISA_06_SenderId:string,ISA_07_ReceiverQualifier:string,ISA_08_ReceiverId:string,ISA_09_Date:string,ISA_10_Time:string,ISA_11_RepetitionSeparator:string,ISA_12_Version:string,ISA_13_InterchangeControlNumber:string,ISA_14_AcknowledgmentRequested:string,ISA_15_TestIndicator:string,functional_groups:array<struct<GS_01_FunctionalIdentifierCode:string,GS_02_ApplicationSenderCode:string,GS_03_ApplicationReceiverCode:string,GS_04_Date:string,GS_05_Time:string,GS_06_GroupControlNumber:string,GS_07_ResponsibleAgencyCode:string,GS_08_Version:string,transactions:array<struct<ST_01_TransactionSetIdentifierCode:string,ST_02_TransactionSetControlNumber:string,ST_03_ImplementationConventionReference:string,segments:array<struct<BHT_01:string,BHT_02:string,BHT_03:string,BHT_04:string,BHT_05:string,BHT_06:string,HL-2000A_loop:array<struct<HL-2000B_loop:array<struct<CLM-2300_loop:array<struct<AMT_01:string,AMT_02:string,CL1_01:string,CL1_02:string,CL1_03:string,CLM_01:string,CLM_02:string,CLM_05:struct<CLM_05_01:string,CLM_05_02:string,CLM_05_03:string>,CLM_07:string,CLM_08:string,CLM_09:string,DTP_01:string,DTP_02:string,DTP_03:string,HCP_01:string,HCP_02:string,HCP_04:string,HCP_13:string,HI_01:struct<HI_01_01:string,HI_01_02:string,HI_01_03:string,HI_01_04:string,HI_01_05:string>,HI_02:struct<HI_02_01:string,HI_02_02:string,HI_02_03:string,HI_02_04:string>,HI_03:struct<HI_03_01:string,HI_03_02:string,HI_03_03:string,HI_03_04:string>,HI_04:struct<HI_04_01:string,HI_04_02:string,HI_04_03:string,HI_04_04:string>,LX-2400_loop:array<struct<DTP_01:string,DTP_02:string,DTP_03:string,LX_01:string,SV2_01:string,SV2_02:struct<SV2_02_01:string,SV2_02_02:string>,SV2_03:string,SV2_04:string,SV2_05:string>>,NM1-2310A_loop:array<struct<NM1_01:string,NM1_02:string,NM1_03:string,NM1_04:string,NM1_05:string,NM1_08:string,NM1_09:string,PRV_01:string,PRV_02:string,PRV_03:string,REF_01:string,REF_02:string>>,REF_01:string,REF_02:string,SBR-2320_loop:array<struct<NM1-2330A_loop:array<struct<N3_01:string,N4_01:string,N4_02:string,N4_03:string,NM1_01:string,NM1_02:string,NM1_03:string,NM1_04:string,NM1_05:string,NM1_08:string,NM1_09:string>>,NM1-2330B_loop:array<struct<NM1_01:string,NM1_02:string,NM1_03:string,NM1_08:string,NM1_09:string>>,OI_03:string,OI_06:string,SBR_01:string,SBR_02:string,SBR_03:string,SBR_04:string,SBR_09:string>>>>,HL-2000C_loop:array<struct<CLM-2300_loop:array<struct<AMT_01:string,AMT_02:string,CL1_01:string,CL1_02:string,CL1_03:string,CLM_01:string,CLM_02:string,CLM_05:struct<CLM_05_01:string,CLM_05_02:string,CLM_05_03:string>,CLM_07:string,CLM_08:string,CLM_09:string,DTP_01:string,DTP_02:string,DTP_03:string,HCP_01:string,HCP_02:string,HCP_03:string,HCP_04:string,HI_01:struct<HI_01_01:string,HI_01_02:string,HI_01_03:string,HI_01_04:string>,HI_02:struct<HI_02_01:string,HI_02_02:string>,LX-2400_loop:array<struct<DTP_01:string,DTP_02:string,DTP_03:string,HCP_01:string,HCP_02:string,HCP_03:string,LX_01:string,SV2_01:string,SV2_02:struct<SV2_02_01:string,SV2_02_02:string>,SV2_03:string,SV2_04:string,SV2_05:string>>,NM1-2310A_loop:array<struct<NM1_01:string,NM1_02:string,NM1_03:string,NM1_04:string,NM1_08:string,NM1_09:string>>,REF_01:string,REF_02:string,SBR-2320_loop:array<struct<NM1-2330A_loop:array<struct<NM1_01:string,NM1_02:string,NM1_03:string,NM1_04:string,NM1_08:string,NM1_09:string>>,NM1-2330B_loop:array<struct<NM1_01:string,NM1_02:string,NM1_03:string,NM1_08:string,NM1_09:string>>,OI_03:string,OI_06:string,SBR_01:string,SBR_02:string,SBR_04:string,SBR_09:string>>>>,HL_01:string,HL_02:string,HL_03:string,HL_04:string,NM1-2010CA_loop:array<struct<DMG_01:string,DMG_02:string,DMG_03:string,N3_01:string,N4_01:string,N4_02:string,N4_03:string,NM1_01:string,NM1_02:string,NM1_03:string,NM1_04:string,REF_01:string,REF_02:string>>,PAT_01:string>>,HL_01:string,HL_02:string,HL_03:string,HL_04:string,NM1-2010BA_loop:array<struct<DMG_01:string,DMG_02:string,DMG_03:string,N3_01:string,N4_01:string,N4_02:string,N4_03:string,NM1_01:string,NM1_02:string,NM1_03:string,NM1_04:string,NM1_05:string,NM1_08:string,NM1_09:string>>,NM1-2010BB_loop:array<struct<NM1_01:string,NM1_02:string,NM1_03:string,NM1_08:string,NM1_09:string,REF_01:string,REF_02:string>>,SBR_01:string,SBR_02:string,SBR_03:string,SBR_09:string>>,HL_01:string,HL_03:string,HL_04:string,NM1-2010AA_loop:array<struct<N3_01:string,N4_01:string,N4_02:string,N4_03:string,NM1_01:string,NM1_02:string,NM1_03:string,NM1_08:string,NM1_09:string,REF_01:string,REF_02:string>>,PRV_01:string,PRV_02:string,PRV_03:string>>,NM1-1000A_loop:array<struct<NM1_01:string,NM1_02:string,NM1_03:string,NM1_08:string,NM1_09:string,PER_01:string,PER_02:string,PER_03:string,PER_04:string>>,NM1-1000B_loop:array<struct<NM1_01:string,NM1_02:string,NM1_03:string,NM1_08:string,NM1_09:string>>>>>>>>>>')]"
    # find_arrays(input_str)
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


    spark = (
        SparkSession.builder.appName("edi")
        .master("local[*]")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "8g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    path = "outputs/837I"
    create_dataframe(spark, path)
