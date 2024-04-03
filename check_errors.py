import glob
import sys
import zlib
import cloudpickle
import tabulate
import os

batchFolder = "condor"
errs = glob.glob("{}/*/err.txt".format(batchFolder))
files = glob.glob("{}/*/chunks*.pkl".format(batchFolder))

errsD = list(map(lambda k: "/".join(k.split("/")[:-1]), errs))
filesD = list(map(lambda k: "/".join(k.split("/")[:-1]), files))
# print(files)
notFinished = list(set(filesD).difference(set(errsD)))


tabulated = []
tabulated.append(["Total jobs", "Finished jobs", "Running jobs"])
tabulated.append(
    [
        len(files),
        "\033[92m " + str(len(errs)) + "\033[00m",
        "\033[93m " + str(len(notFinished)) + "\033[00m",
    ]
)

# print('queue 1 Folder in ' + ' '.join(list(map(lambda k: k.split('/')[-1], notFinished))))
normalErrs = """real
user
sys
"""
normalErrs = normalErrs.split("\n")
normalErrs = list(map(lambda k: k.strip(" ").strip("\t"), normalErrs))
normalErrs = list(filter(lambda k: k != "", normalErrs))

toResubmit = []


def normalErrsF(k):
    for s in normalErrs:
        if s in k:
            return True
        elif k.startswith(s):
            return True
    return False


for err in errs:
    with open(err) as file:
        lines = file.read()
    txt = lines.split("\n")
    # txt = list(filter(lambda k: k not in normalErrs, txt))
    txt = list(filter(lambda k: not normalErrsF(k), txt))
    txt = list(filter(lambda k: k.strip() != "", txt))
    if len(txt) > 0:
        print("Found unusual error in")
        print(err)
        print("\n")
        print("\n".join(txt))
        print("\n\n")
        toResubmit.append(err)
toResubmit = list(map(lambda k: "".join(k.split("/")[-2]), toResubmit))
print("To resubmit", toResubmit)
print("queue 1 Folder in ", ', '.join(toResubmit))

print(tabulate.tabulate(tabulated, headers="firstrow", tablefmt="fancy_grid"))

sys.exit()
pathResults = "/gwdata/users/gpizzati/condor_processor"
results = list(
    map(lambda k: pathResults + "/results/results_" + k + ".pkl", toResubmit)
)
full_redo = []
new_chunks = []


def read_chunks(filename):
    with open(filename, "rb") as file:
        chunks = cloudpickle.loads(zlib.decompress(file.read()))
    return chunks


string = "queue 1 Folder in "
for result, job in zip(results, toResubmit):
    if not os.path.exists(result):
        print("Should rerun whole job", job)
        string += job + ", "
        # new_chunks += read_chunks("condor/" + job + "/chunks_job.pkl")
    else:
        print("collecting only erred chunks for job", job)
        new_chunks += read_chunks(result)["errors"]

print(string[:-2])
print(len(new_chunks))
