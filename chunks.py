import json
from math import ceil


def create_datasets():

    import json

    with open("data/files_all2.json", "r") as file:
        files = json.load(file)

    datasets = {
        "Zjj": {
            "files": files["EWKZ2Jets_ZToLL_M-50_MJJ-120"]["files"][:],
            "xs": 1.719,
        },
        "DY": {
            "files": files["DYJetsToLL_M-50"]["files"][:],
            "xs": 6077.22,
        },
        "TTTo2L2Nu": {
            "files": files["TTTo2L2Nu"]["files"][:],
            "xs": 10,
        },
        "ST_s-channel": {
            "files": files["ST_s-channel"]["files"][:],
            "xs": 10,
        },
        "ST_t-channel_top": {
            "files": files["ST_t-channel_top"]["files"][:],
            "xs": 10,
        },
        "ST_t-channel_antitop": {
            "files": files["ST_t-channel_antitop"]["files"][:],
            "xs": 10,
        },
        "ST_tW_antitop": {
            "files": files["ST_tW_antitop"]["files"][:],
            "xs": 10,
        },
        "ST_tW_top": {
            "files": files["ST_tW_top"]["files"][:],
            "xs": 10,
        },
    }

    for dataset in datasets:
        datasets[dataset]["read_form"] = "mc"

    # DataTrig = {
    #     "MuonEG": "events.EleMu",
    #     "DoubleMuon": "(~events.EleMu) & events.DoubleMu",
    #     "SingleMuon": "(~events.EleMu) & (~events.DoubleMu) & events.SingleMu",
    #     "EGamma": "(~events.EleMu) & (~events.DoubleMu) & (~events.SingleMu) & (events.SingleEle | events.DoubleEle)",
    # }
    DataTrig = {
        "DoubleMuon": "events.DoubleMu",
        "SingleMuon": "(~events.DoubleMu) & events.SingleMu",
        "EGamma":     "(~events.DoubleMu) & (~events.SingleMu) & (events.SingleEle | events.DoubleEle)",
    }

    # for dataset in ["DoubleMuon", "EGamma", "MuonEG", "SingleMuon"]:
    for dataset in ["DoubleMuon", "EGamma", "SingleMuon"]:
        _files = []
        for filesName in [k for k in list(files.keys()) if dataset in k]:
            _files += files[filesName]["files"]
        datasets[dataset] = {
            "files": _files,
            "trigger_sel": DataTrig[dataset],
            "read_form": "data",
            "is_data": True,
        }

    return datasets


def split_chunks(num_entries):
    max_events = 20_000_000
    chunksize = 100_000
    max_events = min(num_entries, max_events)
    nIterations = ceil(max_events / chunksize)
    file_results = []
    for i in range(nIterations):
        start = min(num_entries, chunksize * i)
        stop = min(num_entries, chunksize * (i + 1))
        if start >= stop:
            break
        file_results.append([start, stop])
    return file_results


def create_chunks(datasets):
    chunks = []
    for dataset in datasets:
        files_dict = dict(datasets[dataset]["files"])
        dataset_dict = {k: v for k, v in datasets[dataset].items() if k != "files"}
        for file in files_dict:
            steps = split_chunks(files_dict[file])
            for start, stop in steps:
                d = {
                    "dataset": dataset,
                    "filename": file,
                    "start": start,
                    "stop": stop,
                    **dataset_dict,
                }
                chunks.append(d)
    return chunks


if __name__ == "__main__":

    datasets = create_datasets()
    with open("data/dataset.json", "w") as file:
        json.dump(datasets, file, indent=2)

    chunks = create_chunks(datasets)
    with open("data/chunks.json", "w") as file:
        json.dump(chunks, file, indent=2)
