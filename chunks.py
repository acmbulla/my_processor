import dask
import json
import uproot


def create_datasets():

    import json

    main_folder = "/gwpool/users/gpizzati/"
    with open(main_folder + "files_all.json", "r") as file:
        files = json.load(file)

    with open(main_folder + "test_processor/my_processor/data/forms.json", "r") as file:
        forms = json.load(file)

    datasets = {
        "Zjj": {
            "files": files["Zjj"]["files"][:],
            "xs": 1.719,
            "read_form": forms["mc"],
        },
        "DY": {
            "files": files["DY_inc"]["files"][:],
            "xs": 6077.22,
            "read_form": forms["mc"],
        },
    }

    # datasets = {}
    DataTrig = {
        "MuonEG": "events.EleMu",
        "DoubleMuon": "(~events.EleMu) & events.DoubleMu",
        "SingleMuon": "(~events.EleMu) & (~events.DoubleMu) & events.SingleMu",
        "EGamma": "(~events.EleMu) & (~events.DoubleMu) & (~events.SingleMu) & (events.SingleEle | events.DoubleEle)",
    }

    for dataset in ["DoubleMuon", "EGamma", "MuonEG", "SingleMuon"]:
        # _files = files[dataset]['files'][:]
        _files = []
        for filesName in [
            k for k in list(files.keys()) if k.startswith(dataset + "_Run2018B")
        ]:
            _files += files[filesName]["files"]
        datasets[dataset] = {
            "files": _files,
            "trigger_sel": DataTrig[dataset],
            "read_form": forms["data"],
            "is_data": True,
        }
    return datasets


from math import ceil


@dask.delayed
def preprocess_file(file, **kwargs):
    f = uproot.open(file, handler=uproot.source.xrootd.XRootDSource)
    num_entries = f["Events"].num_entries
    f.close()
    max_events = 10_000_000
    chunksize = 100_000
    max_events = min(num_entries, max_events)
    nIterations = ceil(max_events / chunksize)
    file_results = []
    for i in range(nIterations):
        # print('Iteration', i)
        # file_results.append(big_process(file, start=chunksize*i, stop=chunksize*(i+1), read_form=read_form))
        start = min(num_entries, chunksize * i)
        stop = min(num_entries, chunksize * (i + 1))
        if start == stop:
            break
        # file_results.append({'filename': file, 'start': start, 'stop': stop, **kwargs})
        file_results.append([start, stop])
    return file_results


# In[26]:


def preprocess(datasets):
    chunks = {}
    for dataset in datasets:
        dataset_meta = {k: v for k, v in datasets[dataset].items() if k != "files"}
        chunks[dataset] = {"files": {}, "metadata": dataset_meta}

        # dataset_dict = datasets[dataset]
        # read_form = dataset_dict['read_form']
        # dataset_dict = {i: dataset_dict[i] for i in dataset_dict if i != 'files'}

        for file in datasets[dataset]["files"]:
            # chunks += preprocess_file(file, dataset=dataset, **dataset_dict)
            chunks[dataset]['files'][file] = {
                "object_path": "Events",
                "steps": preprocess_file(file),
            }
    return chunks


# In[ ]:


if __name__ == "__main__":
    from distributed import LocalCluster

    cluster = LocalCluster(
        n_workers=10,
        threads_per_worker=1,
        memory_limit="2GB",  # hardcoded
        # dashboard_address=":8887",
    )
    # from time import sleep

    # sleep(10)

    client = cluster.get_client()

    datasets = create_datasets()
    print(datasets)
    chunks = preprocess(datasets)

    chunks = dask.compute(chunks)

    chunks = chunks[0]
    print(chunks)

    with open("data/chunks.json", "w") as file:
        json.dump(chunks, file)
