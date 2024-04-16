import json
import subprocess
import sys
import cloudpickle
import zlib
import os
from math import ceil


def split_chunks(l, n):
    """
    Splits list l of chunks into n jobs with approximately equals sum of values
    see  http://stackoverflow.com/questions/6855394/splitting-list-in-chunks-of-balanced-weight
    """
    result = [[] for i in range(n)]
    sums = {i: 0 for i in range(n)}
    c = 0
    for e in l:
        for i in sums:
            if c == sums[i]:
                result[i].append(e)
                break
        sums[i] += e["weight"]
        c = min(sums.values())
    return result


"""
{
  "clipper.hcms.it": {
    "free": 110
  },
  "pccms01.hcms.it": {
    "free": 58
  },
  "pccms02.hcms.it": {
    "free": 58
  },
  "pccms03.hcms.it": {
    "free": 58
  },
  "pccms04.hcms.it": {
    "free": 58
  },
  "pccms08.hcms.it": {
    "free": 14
  },
  "pccms11.hcms.it": {
    "free": 30,
    "spalluotto@hcms.it": 6
  },
  "pccms12.hcms.it": {
    "free": 62
  },
  "pccms13.hcms.it": {
    "free": 62
  },
  "pccms14.hcms.it": {
    "free": 62
  }
}
"""
if __name__ == "__main__":
    # machines = [
    #     "pccms04.hcms.it",
    #     "pccms08.hcms.it",
    #     "pccms12.hcms.it",
    #     "pccms14.hcms.it",
    # ]
    with open("data/common/forms.json", "r") as file:
        forms = json.load(file)
    with open("data/chunks.json", "r") as file:
        new_chunks = json.load(file)

    for i, chunk in enumerate(new_chunks):
        new_chunks[i]["read_form"] = forms[chunk["read_form"]]
        dset = chunk["dataset"]
        if chunk.get("is_data", False):
            new_chunks[i]["weight"] = 1
        elif "Zjj" == dset:
            new_chunks[i]["weight"] = 8
        elif "DY" == dset:
            new_chunks[i]["weight"] = 8
        elif "top" in dset.lower() or "ttto" in dset.lower() or "ST_s" in dset:
            new_chunks[i]["weight"] = 3
        else:
            print("No weight", dset)
            new_chunks[i]["weight"] = 1

    # Select only chunks we want to run on
    max_chunks_per_dataset = 300
    max_chunks = {}
    do_chunks = []
    for i, chunk in enumerate(new_chunks):
        dset = chunk["dataset"]
        # if dset != "DY":
        #     continue
        fname = chunk["filename"]
        # print(dset)
        if "/store/data" in fname:
            if "Run2018B" not in fname:
                # print('skip', fname.split('/stor/data')[-1])
                continue
            # print(fname)
            do_chunks.append(i)
            continue
        if dset not in max_chunks:
            max_chunks[dset] = 1
        else:
            if max_chunks[dset] >= max_chunks_per_dataset:
                continue
            else:
                max_chunks[dset] += 1
        do_chunks.append(i)
    do_chunks = list(set(do_chunks))

    new_chunks2 = []
    for i in do_chunks:
        new_chunks2.append(new_chunks[i])
    new_chunks = new_chunks2

    # for i in range(len(new_chunks)):
    #     new_chunks[i]["i"] = i

    print("N chunks", len(new_chunks))
    print(sorted(list(set(list(map(lambda k: k["dataset"], new_chunks))))))
    tot_chunks_old = {}
    for chunk in new_chunks:
        dset = chunk["dataset"]
        if dset in tot_chunks_old:
            tot_chunks_old[dset] += chunk["stop"] - chunk["start"]
        else:
            tot_chunks_old[dset] = chunk["stop"] - chunk["start"]

    # # Split chunks equally in number between jobs
    # chunks_per_job = 20
    # # chunks_per_job = 10
    # njobs = ceil(len(new_chunks) / chunks_per_job)
    # # njobs = 1
    # print("Chunks per job", chunks_per_job)
    # print("Number of jobs", njobs)
    # # sys.exit()
    # jobs = []
    # for i in range(njobs):
    #     start = i * chunks_per_job
    #     stop = min((i + 1) * chunks_per_job, len(new_chunks))
    #     if start >= stop:
    #         break
    #     # print(start, stop)
    #     jobs.append(new_chunks[start:stop])

    # print(len(jobs))

    njobs = 300
    jobs = split_chunks(new_chunks, njobs)
    _jobs = []
    tot_chunks = {}
    for i, job in enumerate(jobs):
        _job = {"sum": 0, "nelements": 0, "datasets": {}}
        for chunk in jobs[i]:
            _job["sum"] += chunk["weight"]
            _job["nelements"] += 1
            dset = chunk["dataset"]
            if dset in tot_chunks:
                tot_chunks[dset] += chunk["stop"] - chunk["start"]
            else:
                tot_chunks[dset] = chunk["stop"] - chunk["start"]
            if dset in _job["datasets"]:
                _job["datasets"][dset] += 1
            else:
                _job["datasets"][dset] = 1
        # _job["datasets"] = list(set(_job["datasets"]))
        _jobs.append(_job)

    # print(_jobs)
    with open("test.json", "w") as file:
        json.dump(_jobs, file, indent=2)

    print(json.dumps(tot_chunks, indent=2))
    print(json.dumps(tot_chunks_old, indent=2))
    # sys.exit()

    folders = []
    pathPython = os.path.abspath(".")
    pathResults = "/eos/user/a/abulla/HiggsMuMu"

    proc = subprocess.Popen(
        f"rm -r condor_backup {pathResults}/results_backup; mv condor condor_backup; mv {pathResults}/results {pathResults}/results_backup",
        shell=True,
    )
    proc.wait()

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

    proc = subprocess.Popen("cp script_worker.py condor/", shell=True)
    proc.wait()

    txtsh = "#!/bin/bash\n"
    txtsh += "export X509_USER_PROXY=/afs/cern.ch/user/a/abulla/.proxy\n"

    txtsh += "source /afs/cern.ch/user/a/abulla/my_processor/start.sh\n"
    # txtsh += "source /gwpool/users/gpizzati/mambaforge/etc/profile.d/mamba.sh\n"
    # txtsh += "mamba activate test_uproot\n"

    # txtsh += f"export PYTHONPATH={pathPython}:$PYTHONPATH\n"
    txtsh += "echo 'which python'\n"
    txtsh += "which python\n"
    txtsh += "time python script_worker.py\n"
    txtsh += f"cp results.pkl {pathResults}/results/results_${1}.pkl\n"
    txtsh += f"ls {pathResults}/results/results_${1}.pkl\n"
    print(txtsh)
    with open("condor/run.sh", "w") as file:
        file.write(txtsh)

    # proc = subprocess.Popen(
    #     f"cp script_worker.py {folder}/; cp condor/run.sh {folder}/;cp data/cfg.json {folder}/;",
    #     shell=True,
    # )
    # sys.exit()

    txtjdl = "universe=vanilla\n"
    txtjdl = "universe = vanilla \n"
    txtjdl += "executable = run.sh\n"
    txtjdl += "arguments = $(Folder)\n"

    txtjdl += "should_transfer_files = YES\n"
    txtjdl += "transfer_input_files = $(Folder)/chunks_job.pkl, script_worker.py, ../data/cfg.json\n"
    txtjdl += "output = $(Folder)/out.txt\n"
    txtjdl += "error  = $(Folder)/err.txt\n"
    txtjdl += "log    = $(Folder)/log.txt\n"
    txtjdl += "request_cpus=1\n"
    # txtjdl += (
    #     "Requirements = "
    #     + " || ".join([f'(machine == "{machine}")' for machine in machines])
    #     + "\n"
    # )
    # txtjdl += 'Requirements = (machine == "pccms03.hcms.it") || (machine == "pccms04.hcms.it") || (machine == "pccms14.hcms.it")\n'
    queue = "workday"
    txtjdl += f'+JobFlavour = "{queue}"\n'

    txtjdl += f'queue 1 Folder in {", ".join(folders)}\n'
    with open("condor/submit.jdl", "w") as file:
        file.write(txtjdl)

    command = f"mkdir -p {pathResults}/results; cd condor/; chmod +x run.sh; cd -"
    command = f"mkdir -p {pathResults}/results; cd condor/; chmod +x run.sh; condor_submit submit.jdl; cd -"
    proc = subprocess.Popen(command, shell=True)
    proc.wait()
