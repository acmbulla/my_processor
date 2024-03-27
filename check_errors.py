import glob
import tabulate

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

print(tabulate.tabulate(tabulated, headers="firstrow", tablefmt="fancy_grid"))
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
