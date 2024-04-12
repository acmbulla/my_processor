from multiprocessing import Pool
import time
import hist
from copy import deepcopy
import numpy as np
import cloudpickle
import zlib
import matplotlib.pyplot as plt
import mplhep as hep
import matplotlib as mpl
import subprocess

mpl.use("Agg")
plt.style.use(hep.style.CMS)

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
    "DY_PU": {
        "samples": ["DY_PU"],
    },
    "DY_hard": {
        "samples": ["DY_hard"],
    },
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
#regions = ["sr_inc", "sr_0j", "sr_1j", "sr_2j", "sr_geq_2j"]
regions = ["sr_inc", "sr_geq_2j"]
# regions = ["sr_geq_2j"]
categories = ["ee", "mm"]
regions = [f"{region}_{category}" for region in regions for category in categories]
variables = ["mll", "mjj", "ptj1", "ptl1", "detajj"]
# variables = ["mll", "ptl1", "detall", "ptl2", "ptj1", "ptj2"]
variables = [
    "njet",
    "njet_50",
    "mjj",
    "detajj",
    "dphijj",
    "ptj1",
    "ptj2",
    "etaj1",
    "etaj2",
    "phij1",
    "phij2",
    # leptons
    "mll",
    "ptll",
    "detall",
    "dphill",
    "ptl1",
    "ptl2",
    "etal1",
    "etal2",
    "phil1",
    "phil2",
    "dR_l1_jets",
    "dR_l2_jets",
    "dR_l1_l2"
]

print("Start converting histograms")

histos = {}
for region in regions:
    histos[region] = {}
    for variable in variables:
        _histos = {}
        axis = 0
        for histoName in datasets:
            if 'sr_inc' in region and ('hard' in histoName or 'PU' in histoName):
                continue
            if '2j' in region and histoName == 'DY':
                continue
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
                for variation_name in variation_names:
                    if variation_name == "nom":
                        continue
                    histo[variation_name] = h[:, hist.loc(variation_name)].values()

                nom = h[:, hist.loc("nom")].values()
                histo["nom"] = nom

                stat = np.sqrt(h[:, hist.loc("nom")].variances())
                histo["stat_up"] = nom + stat
                histo["stat_down"] = nom - stat

                if histoName not in _histos:
                    _histos[histoName] = {}  # "nominal": nom.copy()}
                    for vname in histo:
                        _histos[histoName][vname] = histo[vname]
                else:
                    for vname in histo:
                        _histos[histoName][vname] += histo[vname]
        # print(region, _histos.keys())
        histos[region][variable] = {"histos": _histos, "axis": axis}

print("Done converting histograms")


cmap_petroff = ["#5790fc", "#f89c20", "#e42536", "#964a8b", "#9c9ca1", "#7a21dd"]
colors = {
    "Zjj": cmap_petroff[2],
    "DY": cmap_petroff[0],
    "DY_hard": cmap_petroff[0],
    "DY_PU": cmap_petroff[3],
    "Top": cmap_petroff[1],
}


def plot(_histos, region, variable):
    print("Doing ", region, variable)
    # start = time.time()
    axis = _histos[region][variable]["axis"]
    histos = _histos[region][variable]["histos"]
    mc = 0
    mc_err_up = 0
    mc_err_down = 0
    for histoName in histos:
        if histoName == "Data":
            continue
        for vname in histos[histoName]:
            if vname == "nom":
                vals = histos[histoName][vname].copy()
                if isinstance(mc, int):
                    mc = vals
                else:
                    mc += vals
                continue
            if vname.endswith("down"):
                continue
            if vname.endswith("central"):
                continue
            if "JES" in vname and "total" in vname.lower():  # .lower():
                continue
            # if "ele_reco" in vname:
            #     continue
            variation_name = vname[: -len("up")]

            nom = histos[histoName]["nom"]
            up = histos[histoName][variation_name + "up"]
            do = histos[histoName][variation_name + "down"]
            up_is_up = up > nom
            #
            # this one above is an array of booleans
            #
            dup2 = np.square(up - nom)
            ddo2 = np.square(do - nom)
            mc_err_up += np.where(up_is_up, dup2, ddo2)
            mc_err_down += np.where(up_is_up, ddo2, dup2)
    # print(histos["Data"]["nom"] / mc)

    mc_err_up = np.sqrt(mc_err_up)
    mc_err_down = np.sqrt(mc_err_down)
    mc = np.where(mc >= 1e-5, mc, 1e-5)
    # print('time after sum mc', time.time()-start)
    # start = time.time()

    ###

    x = axis.centers
    edges = axis.edges
    mc1 = 0
    fig, ax = plt.subplots(
        2, 1, sharex=True, gridspec_kw={"height_ratios": [3, 1]}
    )  # figsize=(5,5), dpi=200)
    fig.tight_layout(pad=-0.5)
    hep.cms.label(
        region, data=True, lumi=round(lumi, 2), ax=ax[0], year="Run-II"
    )  # ,fontsize=16)
    # for i, histoName in enumerate(["Top", "DY", "Zjj", "Data"]):
    for i, histoName in enumerate(histos.keys()):
        y = histos[histoName]["nom"]
        integral = round(np.sum(y), 2)
        if histoName == "Data":
            yup = histos[histoName]["stat_up"] - y
            ydown = y - histos[histoName]["stat_down"]
            ax[0].errorbar(
                x, y, yerr=(ydown, yup), fmt="ko", label="Data" + f" [{integral}]"
            )
            continue
        color = colors[histoName]
        if histoName == "Zjj":
            ax[0].stairs(y, edges, zorder=10, linewidth=3, color=color)

        if isinstance(mc1, int):
            mc1 = y.copy()
        else:
            mc1 += y
        ax[0].stairs(
            mc1,
            edges,
            label=histoName + f" [{integral}]",
            fill=True,
            zorder=-i,
            color=color,
        )
    # print('time loop stairs', time.time()-start)

    unc = np.max([mc_err_up, mc_err_down], axis=0)
    unc = round(np.sum(unc + mc), 2)
    unc_up = round(np.sum(mc_err_up) / np.sum(mc) * 100, 2)
    unc_down = round(np.sum(mc_err_down) / np.sum(mc) * 100, 2)
    # ax[0].stairs(mc+mc_err_up, edges, baseline=mc-mc_err_down, fill=True, alpha=0.2, label=f"Syst [$\pm${unc}%]")
    # ax[0].stairs(mc+mc_err_up, edges, baseline=mc-mc_err_down, fill=True, alpha=0.2, label=f"Syst [-{unc_down}, +{unc_up}]%")
    unc_dict = dict(
        fill=True, hatch="///", color="darkgrey", facecolor="none", zorder=9
    )
    ax[0].stairs(
        mc + mc_err_up,
        edges,
        baseline=mc - mc_err_down,
        label=f"Syst [-{unc_down}, +{unc_up}]%",
        **unc_dict,
    )
    integral = round(np.sum(mc), 2)
    ax[0].stairs(mc, edges, label=f"Tot MC [{integral}]", color="darkgrey", linewidth=3)
    ax[0].set_yscale("log")
    ax[0].legend()
    ax[0].set_ylim(1, np.max(mc) * 1e2)

    ratio_err_up = mc_err_up / mc
    ratio_err_down = mc_err_down / mc
    ax[1].stairs(
        1 + ratio_err_up,
        edges,
        baseline=1 - ratio_err_down,
        fill=True,
        color="lightgray",
    )
    ydata = histos["Data"]["nom"]
    ydata_up = histos["Data"]["stat_up"] - ydata
    ydata_down = ydata - histos["Data"]["stat_down"]
    ratio = ydata / mc
    ratio_data_up = ydata_up / mc
    ratio_data_down = ydata_down / mc
    # print(ratio_data_up)
    ax[1].errorbar(x, ratio, (ratio_data_down, ratio_data_up), fmt="ko")
    ax[1].plot(edges, np.ones_like(edges), color="black", linestyle="dashed")
    ax[1].set_ylim(0.7, 1.3)
    ax[1].set_xlabel(variable)
    # print('time before fig save', time.time()-start)
    fig.savefig(
        f"plots/{region}_{variable}.png",
        facecolor="white",
        pad_inches=0.1,
        bbox_inches="tight",
    )
    plt.close()
    # print('time after fig save', time.time()-start)


d = deepcopy(hep.style.CMS)

d["font.size"] = 12
d["figure.figsize"] = (5, 5)

plt.style.use(d)

print("Doing plots")

proc = subprocess.Popen("mkdir -p plots", shell=True)
proc.wait()

args = []
for region in regions:
    for variable in variables:
        # if 'inc' in region:
        #     continue
        args.append((histos, region, variable))

with Pool(10) as pool:
    # for arg in args:
    results = [pool.apply_async(plot, arg) for arg in args]
    for result in results:
        result.get()

# for region in regions:
#     for variable in variables:
#         plot(histos, region, variable)
