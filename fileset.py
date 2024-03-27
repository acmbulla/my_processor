import json
from coffea.dataset_tools import rucio_utils
import uproot
import dask


def get_files():
    Samples = {}

    Samples["DoubleMuon_Run2018A-UL2018-v1"] = {
        "nanoAOD": "/DoubleMuon/Run2018A-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
    }
    Samples["DoubleMuon_Run2018B-UL2018-v2"] = {
        "nanoAOD": "/DoubleMuon/Run2018B-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
    }
    Samples["DoubleMuon_Run2018C-UL2018-v1"] = {
        "nanoAOD": "/DoubleMuon/Run2018C-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
    }
    Samples["DoubleMuon_Run2018D-UL2018-v2"] = {
        "nanoAOD": "/DoubleMuon/Run2018D-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD"
    }

    Samples["EGamma_Run2018A-UL2018-v1"] = {
        "nanoAOD": "/EGamma/Run2018A-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
    }
    Samples["EGamma_Run2018B-UL2018-v1"] = {
        "nanoAOD": "/EGamma/Run2018B-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
    }
    Samples["EGamma_Run2018C-UL2018-v1"] = {
        "nanoAOD": "/EGamma/Run2018C-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
    }
    Samples["EGamma_Run2018D-UL2018-v1"] = {
        "nanoAOD": "/EGamma/Run2018D-UL2018_MiniAODv2_NanoAODv9-v3/NANOAOD"
    }

    Samples["MuonEG_Run2018A-UL2018-v1"] = {
        "nanoAOD": "/MuonEG/Run2018A-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
    }
    Samples["MuonEG_Run2018B-UL2018-v1"] = {
        "nanoAOD": "/MuonEG/Run2018B-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
    }
    Samples["MuonEG_Run2018C-UL2018-v1"] = {
        "nanoAOD": "/MuonEG/Run2018C-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
    }
    Samples["MuonEG_Run2018D-UL2018-v1"] = {
        "nanoAOD": "/MuonEG/Run2018D-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
    }

    Samples["SingleMuon_Run2018A-UL2018-v2"] = {
        "nanoAOD": "/SingleMuon/Run2018A-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD"
    }
    Samples["SingleMuon_Run2018B-UL2018-v2"] = {
        "nanoAOD": "/SingleMuon/Run2018B-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD"
    }
    Samples["SingleMuon_Run2018C-UL2018-v2"] = {
        "nanoAOD": "/SingleMuon/Run2018C-UL2018_MiniAODv2_NanoAODv9-v2/NANOAOD"
    }
    Samples["SingleMuon_Run2018D-UL2018-v1"] = {
        "nanoAOD": "/SingleMuon/Run2018D-UL2018_MiniAODv2_NanoAODv9-v1/NANOAOD"
    }

    Samples["DYJetsToLL_M-50"] = {
        "nanoAOD": "/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM"
    }
    Samples["EWKZ2Jets_ZToLL_M-50_MJJ-120"] = {
        "nanoAOD": "/EWK_LLJJ_MLL-50_MJJ-120_TuneCP5_13TeV-madgraph-pythia8_dipole/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM"
    }

    Samples["ST_tW_top"] = {
        "nanoAOD": "/ST_tW_top_5f_inclusiveDecays_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM"
    }
    Samples["ST_tW_antitop"] = {
        "nanoAOD": "/ST_tW_antitop_5f_inclusiveDecays_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v2/NANOAODSIM"
    }
    Samples["ST_t-channel_antitop"] = {
        "nanoAOD": "/ST_t-channel_antitop_4f_InclusiveDecays_TuneCP5_13TeV-powheg-madspin-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM"
    }
    Samples["ST_t-channel_top"] = {
        "nanoAOD": "/ST_t-channel_top_4f_InclusiveDecays_TuneCP5_13TeV-powheg-madspin-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM"
    }
    Samples["ST_s-channel"] = {
        "nanoAOD": "/ST_s-channel_4f_leptonDecays_TuneCP5_13TeV-amcatnlo-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM"
    }
    Samples["TTTo2L2Nu"] = {
        "nanoAOD": "/TTTo2L2Nu_TuneCP5_13TeV-powheg-pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM"
    }

    files = {}
    for sampleName in Samples:
        # if "DoubleMuon" not in sampleName:
        #     continue
        files[sampleName] = {"query": Samples[sampleName]["nanoAOD"], "files": {}}

    return files


@dask.delayed
def get_filename_nevents(file):
    # file is actually a list of all replicas
    max_events = 20_000_000
    for _file in file:
        try:
            f = uproot.open(_file, handler=uproot.source.xrootd.XRootDSource)
            num_entries = f["Events"].num_entries
            f.close()
            nevents = min(num_entries, max_events)
            if nevents < max_events:
                return (_file, nevents)
        except Exception:
            continue
    return (file[0], max_events)


def get_cluster(local=True):
    if not local:
        from dask_jobqueue import HTCondorCluster
        import socket

        cluster = HTCondorCluster(
            cores=1,
            memory="2 GB",  # hardcoded
            disk="100 MB",
            death_timeout="60",
            nanny=False,
            scheduler_options={
                # 'port': n_port,
                "dashboard_address": 8887,
                "host": socket.gethostname(),
            },
            job_extra_directives={
                "log": "dask_out/dask_job_output.log",
                "output": "dask_out/dask_job_output.out",
                "error": "dask_out/dask_job_output.err",
                "should_transfer_files": "Yes",
                "when_to_transfer_output": "ON_EXIT",
            },
            job_script_prologue=[
                "export PATH=/gwpool/users/gpizzati/mambaforge/bin:$PATH",
                "mamba activate test_uproot",
                "export X509_USER_PROXY=/gwpool/users/gpizzati/.proxy",
                "export XRD_RUNFORKHANDLER=1",
            ],
        )
        cluster.scale(100)

        return cluster
    else:
        from distributed import LocalCluster

        cluster = LocalCluster(
            n_workers=10,
            threads_per_worker=1,
            memory_limit="2GB",  # hardcoded
            dashboard_address=":8887",
        )
        return cluster


if __name__ == "__main__":
    cluster = get_cluster(local=False)

    client = cluster.get_client()
    client.wait_for_workers(10)

    files = get_files()
    print(files)
    rucio_client = rucio_utils.get_rucio_client()
    for dname in files:
        dataset = files[dname]["query"]
        print("Checking", dname, "files with query", dataset)
        try:
            (
                outfiles,
                outsites,
                sites_counts,
            ) = rucio_utils.get_dataset_files_replicas(
                dataset,
                allowlist_sites=[],
                blocklist_sites=["T2_FR_IPHC", "T2_ES_IFCA"],
                # regex_sites=[],
                regex_sites=r"T[123]_(FR|IT|BE|CH|ES|UK)_\w+",
                # regex_sites = r"T[123]_(DE|IT|BE|CH|ES|UK|US)_\w+",
                mode="full",  # full or first. "full"==all the available replicas
                client=rucio_client,
            )
        except Exception as e:
            print(f"\n[red bold] Exception: {e}[/]")

        # files[dname]["files"] = list(map(lambda k: k[0], outfiles))
        files[dname]["files"] = [get_filename_nevents(file) for file in outfiles]
    files = dask.compute(files)
    files = files[0]

    with open("data/files_all2.json", "w") as file:
        json.dump(files, file, indent=2)
