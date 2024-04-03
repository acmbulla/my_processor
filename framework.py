import zlib
import cloudpickle
import uproot
import awkward as ak
from copy import deepcopy
import sys
import time
import gc
from functools import wraps

from variation import Variation


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

    branches = [k.name for k in tree.branches]

    events = {}
    form = deepcopy(read_form)

    for coll in form:
        d = {}
        coll_branches = form[coll].pop("branches")

        if len(coll_branches) == 0:
            if coll in branches:
                events[coll] = read_array(tree, coll, start, stop)
            continue

        for branch in coll_branches:
            branch_name = coll + "_" + branch
            if branch_name in branches:
                d[branch] = read_array(tree, branch_name, start, stop)

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


def read_chunks(filename):
    with open(filename, "rb") as file:
        chunks = cloudpickle.loads(zlib.decompress(file.read()))
    return chunks


def get_columns(events):
    columns = []
    for column_base in ak.fields(events):
        column_suffixes = ak.fields(events[column_base])
        if len(column_suffixes) == 0:
            columns.append((column_base,))
        else:
            for column_suffix in column_suffixes:
                columns.append((column_base, column_suffix))
    return columns


def vary(func):
    @wraps(func)
    def wrapper_decorator(events: ak.Array, variations: Variation, *args, **kwargs):

        # Make a backup copy for events
        originalEvents = ak.copy(events)

        # Save all current variations, will loop over them callind the function
        variations_dict = deepcopy(variations.variations_dict)

        original_fields = get_columns(events)

        # Run the nominal
        new_events, new_variations = func(events, variations, *args, **kwargs)

        # Take all the new varied columns (new columns created, e.g. Jet_pt_JES_up)
        new_variations_dict = deepcopy(new_variations.variations_dict)
        new_varied_cols = []
        for new_variation_name in new_variations_dict:
            if new_variation_name not in variations_dict:
                # this is a new variation -> add all varied cols
                new_varied_cols += list(
                    map(lambda k: k[1], new_variations_dict[new_variation_name])
                )
            else:
                # some new columns might have been added to this variation
                # only register the new ones
                orig_nom_cols = list(
                    map(lambda k: k[0], variations_dict[new_variation_name])
                )
                nom_cols = list(
                    map(lambda k: k[0], new_variations_dict[new_variation_name])
                )
                varied_cols = list(
                    map(lambda k: k[1], new_variations_dict[new_variation_name])
                )
                new_nom_cols = list(set(nom_cols).difference(orig_nom_cols))
                for new_nom_col in new_nom_cols:
                    i = nom_cols.index(new_nom_col)
                    new_varied_cols.append(varied_cols[i])

        nom_fields = get_columns(new_events)
        new_fields = list(set(nom_fields).difference(original_fields))
        new_fields = list(set(nom_fields).difference(new_varied_cols))

        for variation in variations_dict:
            events = ak.copy(originalEvents)

            for switch in variations[variation]:
                if len(switch) == 2:
                    print(switch)
                    variation_dest, variation_source = switch
                    events[variation_dest] = events[variation_source]

            varied_events, _ = func(events, variations, *args, **kwargs)

            # copy all the varied columns here
            for new_field in new_fields:
                varied_field = Variation.format_varied_column(variation, new_field)
                new_events[varied_field] = ak.copy(varied_events[new_field])
            # and register them
            new_variations.add_columns_for_variation(variation, new_fields)

        return new_events, new_variations

    return wrapper_decorator
