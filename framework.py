import uproot
import awkward as ak
from copy import deepcopy
import sys
import time
import gc


def read_array(tree, branch_name, start, stop):
    interp = tree[branch_name].interpretation
    interp._forth = True
    return tree[branch_name].array(
        interp,
        entry_start=start,
        entry_stop=stop,
        decompression_executor=uproot.source.futures.TrivialExecutor(),
        interpretation_executor=uproot.source.futures.TrivialExecutor(),
    )


def read_events(filename, start=0, stop=100, read_form={}):
    print("start reading")
    f = uproot.open(filename, handler=uproot.source.xrootd.XRootDSource, num_workers=2)
    tree = f["Events"]
    start = min(start, tree.num_entries)
    stop = min(stop, tree.num_entries)
    if start >= stop:
        return ak.Array([])
    events = {}
    form = deepcopy(read_form)
    for coll in form:
        # print(coll)
        d = {}
        coll_branches = form[coll].pop("branches")
        # print(coll_branches)

        if len(coll_branches) == 0:
            # print(f"Collection {coll} is not a collection but a signle branch")
            events[coll] = read_array(tree, coll, start, stop)
            continue

        for branch in coll_branches:
            d[branch] = read_array(tree, coll + "_" + branch, start, stop)

        if len(d.keys()) == 0:
            print("did not find anything for", coll, file=sys.stderr)
            continue

        events[coll] = ak.zip(d, **form[coll])
        del d

    # f.close()
    print("created events")
    _events = ak.zip(events, depth_limit=1)
    del events
    return _events


def add_dict(d1, d2):
    if isinstance(d1, dict):
        d = {}
        common_keys = set(list(d1.keys())).intersection(list(d2.keys()))
        for key in common_keys:
            d[key] = add_dict(d1[key], d2[key])
        for key in d1:
            if key in common_keys:
                continue
            d[key] = d1[key]
        for key in d2:
            if key in common_keys:
                continue
            d[key] = d2[key]

        return d
    else:
        return d1 + d2


def add_dict_iterable(iterable):
    tmp = -99999
    for it in iterable:
        if tmp == -99999:
            tmp = it
        else:
            tmp = add_dict(tmp, it)
    return tmp


# import dask
# @dask.delayed
def big_process(process, filename, start, stop, read_form, **kwargs):
    t_start = time.time()
    events = read_events(filename, start=start, stop=stop, read_form=read_form)
    t_reading = time.time() - t_start
    if len(events) == 0:
        return {}
    results = process(events, **kwargs)
    t_total = time.time() - t_start
    results[f"{filename}_{start}"] = {"total": t_total, "read": t_reading}
    del events
    gc.collect()
    return results
