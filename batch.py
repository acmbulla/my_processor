import json
import subprocess
import cloudpickle
import zlib
import os
from math import ceil

if __name__ == "__main__":
    with open("data/common/forms.json", "r") as file:
        forms = json.load(file)
    with open("data/chunks.json", "r") as file:
        new_chunks = json.load(file)

    for i, chunk in enumerate(new_chunks):
        new_chunks[i]["read_form"] = forms[chunk["read_form"]]

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
    print(max_chunks)
    print(do_chunks)
    do_chunks = list(set(do_chunks))

    # # sys.exit()
    new_chunks2 = []
    for i in do_chunks:
        new_chunks2.append(new_chunks[i])
    new_chunks = new_chunks2

    print("N chunks", len(new_chunks))
    chunks_per_job = 20
    # chunks_per_job = 10
    njobs = ceil(len(new_chunks) / chunks_per_job)
    # njobs = 1
    print("Chunks per job", chunks_per_job)
    print("Number of jobs", njobs)
    # sys.exit()
    jobs = []
    for i in range(njobs):
        start = i * chunks_per_job
        stop = min((i + 1) * chunks_per_job, len(new_chunks))
        if start >= stop:
            break
        # print(start, stop)
        jobs.append(new_chunks[start:stop])

    print(len(jobs))
    # sys.exit()

    folders = []
    pathPython = os.path.abspath(".")
    pathResults = "/gwdata/users/gpizzati/condor_processor"

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
    txtsh += "export X509_USER_PROXY=/gwpool/users/gpizzati/.proxy\n"

    txtsh += "source /gwpool/users/gpizzati/mambaforge/etc/profile.d/conda.sh\n"
    txtsh += "source /gwpool/users/gpizzati/mambaforge/etc/profile.d/mamba.sh\n"
    txtsh += "mamba activate test_uproot\n"

    txtsh += f"export PYTHONPATH={pathPython}:$PYTHONPATH\n"
    txtsh += "echo 'which python'\n"
    txtsh += "which python\n"
    txtsh += "time python script_worker.py\n"
    txtsh += f"cp results.pkl {pathResults}/results/results_${1}.pkl\n"
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
    queue = "workday"
    txtjdl += f'+JobFlavour = "{queue}"\n'

    txtjdl += f'queue 1 Folder in {", ".join(folders)}\n'
    with open("condor/submit.jdl", "w") as file:
        file.write(txtjdl)

    command = f"mkdir -p {pathResults}/results; cd condor/; chmod +x run.sh; cd -"
    command = f"mkdir -p {pathResults}/results; cd condor/; chmod +x run.sh; condor_submit submit.jdl; cd -"
    proc = subprocess.Popen(command, shell=True)
    proc.wait()
