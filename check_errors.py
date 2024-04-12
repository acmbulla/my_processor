import glob
import json
import sys
import zlib
import cloudpickle
import tabulate
import os

from framework import add_dict

with open("test.json") as file:
    jobs_dict = json.load(file)


batchFolder = "condor"
errs = glob.glob("{}/*/err.txt".format(batchFolder))
files = glob.glob("{}/*/chunks*.pkl".format(batchFolder))

errsD = list(map(lambda k: "/".join(k.split("/")[:-1]), errs))
filesD = list(map(lambda k: "/".join(k.split("/")[:-1]), files))
# print(errsD_ind)
# print(filesD_ind)


# print(finished_dict)
# print(total_dict)

# print('queue 1 Folder in ' + ' '.join(list(map(lambda k: k.split('/')[-1], notFinished))))
normalErrs = """real
user
sys
invalid value encountered in scalar divide
btag_norm
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

print(toResubmit)

pathResults = "/gwdata/users/gpizzati/condor_processor/results"
start = len(toResubmit)
for i in range(len(filesD)):
    if not os.path.exists(f"{pathResults}/results_job_{i}.pkl") and f"job_{i}" not in toResubmit:
        toResubmit.append(f"job_{i}")

print("Did not produce output file for", " ".join(toResubmit[start:]))


# print("To resubmit", toResubmit)
print('\n\n')
print("queue 1 Folder in ", ", ".join(toResubmit))
print('\n\n')


total_done = list(map(lambda k: k.split("/")[-2], errs))  # includes finished and erred
success = list(set(total_done).difference(toResubmit))
erred = toResubmit
total = list(map(lambda k: k.split("/")[-2], files))  # includes all
assert set(total) == set([f"job_{i}" for i in range(300)])
success_ind = sorted(list(map(lambda k: int(k.split("_")[-1]), success)))
erred_ind = sorted(list(map(lambda k: int(k.split("_")[-1]), erred)))
total_ind = sorted(list(map(lambda k: int(k.split("_")[-1]), total)))
total_dict = {}
success_dict = {}
erred_dict = {}
for i in total_ind:
    total_dict = add_dict(total_dict, jobs_dict[i]["datasets"])
    if i in success_ind:
        success_dict = add_dict(success_dict, jobs_dict[i]["datasets"])
    if i in erred_ind:
        erred_dict = add_dict(erred_dict, jobs_dict[i]["datasets"])


tabulated = []
tabulated.append(
    ["Total jobs", "Finished correctly jobs", "Erred jobs", "Running jobs"]
)
tabulated.append(
    [
        len(total),
        "\033[92m " + str(len(success)) + "\033[00m",
        "\033[91m " + str(len(erred)) + "\033[00m",
        "\033[93m " + str(len(total) - len(success) - len(erred)) + "\033[00m",
    ]
)
print(tabulate.tabulate(tabulated, headers="firstrow", tablefmt="fancy_grid"))


def format(string, color):
    prefix = "\033["
    postfix = "\033[00m"
    if color == "red":
        prefix += "91m "
    elif color == "yellow":
        prefix += "93m "
    elif color == "green":
        prefix += "92m "
    return prefix + str(string) + postfix


tabulated = [["Dataset", "Done", "Err", "Total"]]
for dset in sorted(list(total_dict)):
    # fraction_done = f"{finished_dict.get(dset, 0)} / {total_dict[dset]}"
    # status = finished_dict.get(dset, 0) / total_dict[dset]
    # if status < th_red:
    #     fraction_done = format_red(fraction_done)
    # elif status >= th_red and status < 1.0:
    #     fraction_done = format_yellow(fraction_done)
    # elif status == 1.0:
    #     fraction_done = format_green(fraction_done)
    success_val = success_dict.get(dset, 0)
    erred_val = erred_dict.get(dset, 0)
    total_val = total_dict.get(dset, 0)
    color = 0
    if erred_val == 0 and success_val == total_val:
        color = "green"
    elif erred_val > 0:
        color = "red"
    else:
        color = "yellow"
    tabulated.append(
        [
            format(dset, color),
            format(success_val, "green"),
            format(erred_val, "red"),
            total_val,
        ]
    )
print(tabulate.tabulate(tabulated, headers="firstrow", tablefmt="fancy_grid"))

sys.exit()

# Old code
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
