import hist
import cloudpickle
import zlib
import numpy as np
import correctionlib
import correctionlib.convert
import correctionlib.schemav2 as cs
import gzip
import json

pathResults = "/gwdata/users/gpizzati/condor_processor/results"
with open(f"{pathResults}/results_merged.pkl", "rb") as file:
    results = cloudpickle.loads(zlib.decompress(file.read()))

results = results["results"]


xss = {
    "Zjj": 2.78,
    "DY": 6077.22,
    "DY_inc": 6077.22,
    "DY_hard": 6077.22,
    "DY_PU": 6077.22,
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
            # "MuonEG",
        ],
        "is_data": True,
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
    "DY": {
        "samples": ["DY_inc"],
    },
    # "DY_PU": {
    #     "samples": ["DY_PU"],
    # },
    # "DY_hard": {
    #     "samples": ["DY_hard"],
    # },
    "Zjj": {
        "samples": ["Zjj"],
    },
}

lumi = 7066.552169 / 1000


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
    # print(h.shape)
    _h = h.copy()
    a = _h.view(True)

    a.value[1, :] = a.value[1, :] + a.value[0, :]
    a.value[0, :] = 0

    a.value[-2, :] = a.value[-2, :] + a.value[-1, :]
    a.value[-1, :] = 0

    a.variance[1, :] = a.variance[1, :] + a.variance[0, :]
    a.variance[0, :] = 0

    a.variance[-2, :] = a.variance[-2, :] + a.variance[-1, :]
    a.variance[-1, :] = 0

    # a.value[1] = a.value[1] + a.value[0]
    # a.value[-2] = a.value[-2] + a.value[-1]
    # a.value[0] = 0
    # a.value[-1] = 0
    return _h


def get_variations(h):
    axis = h.axes[1]
    variation_names = [axis.value(i) for i in range(len(axis.centers))]
    return variation_names


region = "sr_mm"
variable = "mll"
regions = ["top_cr", "vv_cr", "sr"]
# regions = ["sr_inc", "sr_0j", "sr_1j", "sr_2j", "sr_geq_2j"]
regions = ["sr_inc"]
# regions = ["sr_geq_2j"]
categories = ["ee", "mm"]
regions = [f"{region}_{category}" for region in regions for category in categories]
variables = [
    "ptll",
]

print("Start converting histograms")

histos = {}
for region in regions:
    histos[region] = {}
    for variable in variables:
        _histos = {}
        axis = 0
        for histoName in datasets:
            for sample in datasets[histoName]["samples"]:
                if sample not in results:
                    print("Skipping", sample)
                    continue

                h = results[sample]["h"][variable][:, hist.loc(region), :].copy()

                # renorm mcs
                if not datasets[histoName].get("is_data", False):
                    h = renorm(h, xss[sample], results[sample]["sumw"])
                h = fold(h)

                # if histoName == 'Zjj':
                #     print(np.sum(h[:, hist.loc('nom')].values(True)))

                if isinstance(axis, int):
                    axis = h.axes[0]  # .copy()
                histo = {}
                variation_names = get_variations(h)
                # for variation_name in variation_names:
                #     if variation_name == "nom":
                #         continue
                #     histo[variation_name] = h[:, hist.loc(variation_name)].values()

                nom = h[:, hist.loc("nom")].values()
                histo["nom"] = nom

                # stat = np.sqrt(h[:, hist.loc("nom")].variances())
                # histo["stat_up"] = nom + stat
                # histo["stat_down"] = nom - stat

                if histoName not in _histos:
                    _histos[histoName] = {}  # "nominal": nom.copy()}
                    for vname in histo:
                        _histos[histoName][vname] = histo[vname]
                else:
                    for vname in histo:
                        _histos[histoName][vname] += histo[vname]
        histos[region][variable] = {"histos": _histos, "axis": axis}

print("Done convrting histograms")

region = "sr_inc_mm"

csets = []
for region in regions:
    h_mm = histos[region]["ptll"]["histos"]["Data"]["nom"].copy()
    for histo in ["Top", "Zjj"]:
        h_mm = h_mm - histos[region]["ptll"]["histos"][histo]["nom"].copy()
    correction = h_mm / histos[region]["ptll"]["histos"]["DY"]["nom"]
    edges = (
        results["DY_inc"]["h"]["ptll"][:, hist.loc(region), hist.loc("nom")]
        .axes[0]
        .edges
    )
    # h = hist.Hist(hist.axis.Variable(edges, name="ptll"), hist.storage.Double())
    # a = h.view(True)
    # a[1:-1] = correction
    # a[0] = correction[0]
    # a[-1] = correction[-1]
    # print(h)

    name = "ptll_rwgt_dy_" + region.split("_")[-1]
    cset = cs.Correction(
        name=name,
        version=1,
        inputs=[cs.Variable(name="ptll", type="real", description="ptll")],
        output=cs.Variable(name="weight", type="real", description="rwgt"),
        data=cs.Binning(
            nodetype="binning",
            input="ptll",
            edges=edges,
            content=correction,
            flow="clamp",
        ),
    )
    # cset = correctionlib.convert.from_histogram(h)
    csets.append(cset)

cset = correctionlib.schemav2.CorrectionSet(
    schema_version=2, description="", corrections=csets
)


with gzip.open("data/ptll_dy_rwgt.json.gz", "wt") as fout:
    fout.write(cset.json(exclude_unset=True))


ceval_ptll = correctionlib.CorrectionSet.from_file(
    "/gwpool/users/gpizzati/test_processor/my_processor/data/ptll_dy_rwgt.json.gz"
)
print(ceval_ptll["ptll_rwgt_dy_ee"].evaluate(10.0))
