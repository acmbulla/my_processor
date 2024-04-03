import hist
import numpy as np
import cloudpickle
import zlib
import matplotlib.pyplot as plt


pathResults = "/gwdata/users/gpizzati/condor_processor/results"
with open(f"{pathResults}/results_merged.pkl", "rb") as file:
    results = cloudpickle.loads(zlib.decompress(file.read()))

results = results["results"]


xss = {
    "Zjj": 2.78,
    "DY": 6077.22,
    "TTTo2L2Nu": 87.310,
    "ST_t-channel_top": 44.07048,
    "ST_t-channel_antitop": 26.2278,
    "ST_tW_antitop": 35.85,
    "ST_tW_top": 35.85,
    "ST_s-channel": 3.34368,
}

datasets = {
    "Data": {
        "samples": [
            "DoubleMuon",
            "SingleMuon",
            "EGamma",
            "MuonEG",
        ],
        "is_data": True,
    },
    "Zjj": {
        "samples": ["Zjj"],
    },
    "DY": {
        "samples": ["DY"],
    },
    "Top": {
        "samples": [
            "TTTo2L2Nu",
            "ST_s-channel",
            "ST_t-channel_top",
            "ST_t-channel_antitop",
            "ST_tW_antitop",
            "ST_tW_top",
        ],
    },
}

lumi = 58.89


def renorm(h, xs, sumw):
    scale = xs * 1000 * lumi / sumw
    # print(scale)
    _h = h.copy()
    a = _h.view(True)
    a.value = a.value * scale
    a.variance = a.variance * scale * scale
    return _h


def fold(h):
    # fold first axis
    print(h.shape)
    _h = h.copy()
    a = _h.view(True)

    a.value[1, :, :] = a.value[1, :, :] + a.value[1, :, :]
    a.value[0, :, :] = 0

    a.value[-2, :, :] = a.value[-2, :, :] + a.value[-1, :, :]
    a.value[-1, :, :] = 0

    a.variance[1, :, :] = a.variance[1, :, :] + a.variance[1, :, :]
    a.variance[0, :, :] = 0

    a.variance[-2, :, :] = a.variance[-2, :, :] + a.variance[-1, :, :]
    a.variance[-1, :, :] = 0

    # a.value[1] = a.value[1] + a.value[0]
    # a.value[-2] = a.value[-2] + a.value[-1]
    # a.value[0] = 0
    # a.value[-1] = 0
    return _h


def get_histo(h, region, variation):
    if variation == "stat":
        return np.sqrt(h[:, hist.loc(region), hist.loc("nom")].variances())
    return h[:, hist.loc(region), hist.loc(variation)].values()


def get_variations(h):
    axis = h.axes[2]
    variation_names = [axis.value(i) for i in range(len(axis.centers))]
    return variation_names


region = "mm"
variable = "mll"
histos = {}

# mc = 0
# data = 0
# syst_up = 0
# syst_down = 0

# sum_data = 0
# sum_mc = 0

for histoName in datasets:
    for sample in datasets[histoName]["samples"]:
        if sample not in results:
            print("Skipping", sample)
            continue

        h = results[sample]["h"][variable]

        # renorm mcs
        if not datasets[histoName].get("is_data", False):
            h = renorm(h, xss[sample], results[sample]["sumw"])
            h = fold(h)
        # sys.exit()

        # print(h)

        # nom = fold(nom)

        # val = np.sum(nom.values(True))

        # print(sample, val)
        # if not datasets[histoName].get("is_data", False):
        #     if isinstance(sum_mc, int):
        #         sum_mc = val
        #     else:
        #         sum_mc += val
        # else:
        #     if isinstance(sum_data, int):
        #         sum_data = val
        #     else:
        #         sum_data += val
        histo = {}
        variation_names = get_variations(h)
        for variation_name in variation_names:
            if variation_name == "nom":
                continue
            histo[variation_name] = h[
                :, hist.loc(region), hist.loc(variation_name)
            ].values()

        nom = h[:, hist.loc(region), hist.loc("nom")].values()
        histo["nom"] = nom

        stat = np.sqrt(h[:, hist.loc(region), hist.loc("nom")].variances())
        histo["stat_up"] = nom + stat
        histo["stat_down"] = nom - stat

        if histoName not in histos:
            histos[histoName] = {}  # "nominal": nom.copy()}
            for vname in histo:
                histos[histoName][vname] = histo[vname]
        else:
            # print("merging", histoName, sample)
            # histos[histoName]["nominal"] += nom
            for vname in histo:
                histos[histoName][vname] += histo[vname]

        # if not datasets[histoName].get("is_data", False):
        #     if isinstance(mc, int):
        #         mc = nom.values().copy()
        #     else:
        #         mc += nom.values()
        #     print("merging", "mc", mc)
        # else:
        #     if isinstance(data, int):
        #         data = nom.values().copy()
        #     else:
        #         data += nom.values()
        #     print("merging", "data", data)

        # if not datasets[histoName].get("is_data", False):
        #     # Stat
        #     stat = get_histo(h, region, "stat")
        #     stat = h[:, hist.loc(region), hist.loc("nom")]
        #     syst_up += np.square(stat)
        #     syst_down += np.square(stat)

        #     # Syst
        #     variation_names = get_variations(h)
        #     variation_names = list(
        #         filter(lambda k: k.lower().endswith("up"), variation_names)
        #     )  # or k.endswith('up')
        #     variation_names = list(map(lambda k: k[: -len("_up")], variation_names))
        #     # print("\n\n")
        #     # print(variation_names)

print(histos)

# print(np.sqrt(syst_up) / mc)
sys.exit()

mc = 0
data = 0
for histoName in histos:
    h = histos[histoName]  # [:, sum, hist.loc(region), hist.loc('nom')]
    if not datasets[histoName].get("is_data", False):
        if isinstance(mc, int):
            mc = h.copy()
        else:
            mc += h.copy()
    else:
        if isinstance(data, int):
            data = h.copy()
        else:
            data += h.copy()
print(data.values() / mc.values())

# for dataset in ["DoubleMuon", "SingleMuon", "EGamma", "MuonEG"]:
#     if dataset not in results:
#         continue
#     h = results[dataset]["h"].copy()
#     if "data" not in histos:
#         histos["data"] = h
#     else:
#         histos["data"] += h

# lumi = 58.89
# for dataset in [
#     "Zjj",
#     "DY",
# ]:
#     scale = 6077.22 * 1000 * lumi / results["DY"]["sumw"]
#     print(scale)
#     h = results[dataset]["h"].copy()
#     a = h.view(True)
#     a.value = a.value * scale
#     a.variance = a.variance * scale * scale
#     histos[dataset] = h

# print(get_histo(histos["data"]) / get_histo(histos["DY"]))


# plt.plot()
# nom =
# stat = np.sqrt(h[:, :, hist.loc("mm"), hist.loc("nom")].project("mjj").variances())
# plt.
# print('Stat')
# print(stat/nom)
# variations = ["JES_HF_up", "JES_HF_down"]#, "btag_hfup", "btag_hfdown"]
# for variation in variations:
#     var = h[:, :, hist.loc("mm"), hist.loc(variation)].project("mjj").values()
#     print(variation)
#     print(var / nom)
#     print(abs(var / nom - 1) < stat/nom)
