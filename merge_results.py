from math import ceil
from framework import add_dict
import glob
import sys
import subprocess
import cloudpickle
import zlib

result_files = glob.glob("results/results_*.pkl")


def read_results(filename):

    with open(filename, "rb") as file:
        results = cloudpickle.loads(zlib.decompress(file.read()))
    return results


def write_results(filename, d):
    with open(filename, "wb") as file:
        file.write(zlib.compress(cloudpickle.dumps(d)))


results = {}
errors = []
for i, result_file in enumerate(result_files[:]):
    result = read_results(result_file)
    partial_results = {
        k: v for k, v in result["results"].items() if not k.startswith("root://")
    }
    try:
        results = add_dict(results, partial_results)
    except Exception as e:
        print("\n\n")
        print("Error mergin job", i)
        print(e)

    errors += result["errors"]


proc = subprocess.Popen("rm results/results_job_*", shell=True)
proc.wait()
write_results("results/results_pre_err.pkl", {"results": results, "errors": []})

print(results.keys())

print("Errors", len(errors))
for error in errors:
    print(error["dataset"], error["error"])

if len(errors) == 0:
    print("Nothing to do")
    sys.exit(0)

new_chunks = list(
    map(lambda chunk: {k: v for k, v in chunk.items() if k != "error"}, errors)
)

chunks_per_job = 10
njobs = ceil(len(new_chunks) / chunks_per_job)
print("Chunks per job", chunks_per_job)
print("Number of jobs", njobs)
jobs = []
for i in range(njobs):
    start = i * chunks_per_job
    stop = min((i + 1) * chunks_per_job, len(new_chunks))
    if start >= stop:
        break
    print(start, stop)
    jobs.append(new_chunks[start:stop])

print(len(jobs))


# resubmit
folders = []
proc = subprocess.Popen("rm -r condor/job_*; cp script_worker.py condor/", shell=True)
proc.wait()
# path = os.path.abspath(".")
for i, job in enumerate(jobs):
    folder = f"condor/job_{i}"
    proc = subprocess.Popen(
        f"mkdir -p {folder}; ",
        shell=True,
    )
    proc.wait()

    with open(f"{folder}/chunks_job.pkl", "wb") as file:
        file.write(zlib.compress(cloudpickle.dumps(job)))

    folders.append(folder.split("/")[-1])

with open("condor/submit.jdl") as file:
    txt = file.read().split("\n")

line_index = txt.index(list(filter(lambda k: k.startswith("queue"), txt))[0])
txt[line_index] = f'queue 1 Folder in {", ".join(folders)}'
# print("\n".join(txt))

with open("condor/submit.jdl", "w") as file:
    file.write("\n".join(txt))

proc = subprocess.Popen("cd condor/; condor_submit submit.jdl; cd -", shell=True)
proc.wait()
