import functools
import os
import subprocess
import time


def force_async(fn):
    from concurrent.futures import ThreadPoolExecutor
    import asyncio

    pool = ThreadPoolExecutor()

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        future = pool.submit(fn, *args, **kwargs)
        return asyncio.wrap_future(future)  # make it awaitable

    return wrapper


@force_async
def execute_transform(input_file_name, output_file_name):
    print("file name is " + input_file_name)
    print(
        subprocess.Popen(
            f"""java -jar /Users/dc/db_load_test/spike/edireader-json-basic-5.6.2.jar {input_file_name} {output_file_name} --format=yes""",
            shell=True,
            stdout=subprocess.PIPE,
        ).stdout.read()
    )

def finalizer_function():
    import glob
    file_list = glob.iglob("/Users/dc/edi_berryworks/spike/inputs" + '**/**', recursive=True)
    file_list = [f for f in file_list if os.path.isfile(f)]
    print("Number of files " + str(len(file_list)))
    for input_file in file_list:
        head, tail = os.path.split(input_file)
        execute_transform(input_file,f"/Users/dc/edi_berryworks/spike/outputs/{tail}.json")



start_time = time.monotonic()
finalizer_function()
end_time = time.monotonic()
print(f"Time taken: {end_time - start_time} seconds")