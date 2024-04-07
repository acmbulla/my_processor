import hist
import awkward as ak
import numpy as np
import uproot
import vector
import coffea
import correctionlib
from coffea.lumi_tools import LumiMask


from framework import add_dict, big_process
from modules.basic_selections import lumi_mask, pass_flags, pass_trigger
from modules.jet_sel import cleanJet, jetSel
from modules.lepton_sel import createLepton, leptonSel
from modules.prompt_gen import prompt_gen_match_leptons

from modules.theory_unc import theory_unc
from modules.trigger_sf import trigger_sf
from modules.lepton_sf import lepton_sf
from modules.jme import getJetCorrections, correct_jets
from modules.puid_sf import puid_sf
from modules.btag_sf import btag_sf
from modules.puweight import puweight_sf
from modules.rochester import correctRochester, getRochester

import cloudpickle
import zlib
import sys
import json
import traceback as tb


vector.register_awkward()

print("uproot version", uproot.__version__)
print("awkward version", ak.__version__)

print("coffea version", coffea.__version__)


with open("cfg.json") as file:
    cfg = json.load(file)

ceval_puid = correctionlib.CorrectionSet.from_file(cfg["puidSF"])
ceval_btag = correctionlib.CorrectionSet.from_file(cfg["btagSF"])
ceval_puWeight = correctionlib.CorrectionSet.from_file(cfg["puWeights"])
ceval_lepton_sf = correctionlib.CorrectionSet.from_file(cfg["leptonSF"])
jec_stack = getJetCorrections(cfg)
rochester = getRochester(cfg)


def process(events, **kwargs):
    dataset = kwargs["dataset"]
    trigger_sel = kwargs.get("trigger_sel", "")
    isData = kwargs.get("is_data", False)

    variations = {}
    variations["nom"] = [()]

    if isData:
        events["weight"] = ak.ones_like(events.run)
    else:
        events["weight"] = events.genWeight

    if isData:
        lumimask = LumiMask(cfg["lumiMask"])
        events = lumi_mask(events, lumimask)

    sumw = ak.sum(events.weight)
    nevents = ak.num(events.weight, axis=0)

    # pass trigger and flags
    events = pass_trigger(events, cfg["tgr_data"])
    events = pass_flags(events, cfg["flags"])

    events = events[events.pass_flags & events.pass_trigger]

    if isData:
        # each data DataSet has its own trigger_sel
        events = events[eval(trigger_sel)]

    events = jetSel(events)

    events = createLepton(events)
    # events["Lepton"] = events.Lepton[events.Lepton.pt > 10]
    events = leptonSel(events)
    events["Lepton"] = events.Lepton[events.Lepton.isLoose]
    events = events[ak.num(events.Lepton) >= 2]
    events = events[events.Lepton[:, 0].pt >= 8]

    if not isData:
        events = prompt_gen_match_leptons(events)

    # FIXME should clean from only tight / loose?
    events = cleanJet(events)

    # # Require at least two loose leptons and loose jets
    # events = events[
    #     (ak.num(events.Lepton, axis=1) >= 2) & (ak.num(events.Jet, axis=1) >= 2)
    # ]

    # MCCorr
    # Should load SF and corrections here

    # Correct Muons with rochester
    events = correctRochester(events, isData, rochester)

    if not isData:
        # puWeight
        events, variations = puweight_sf(events, variations, ceval_puWeight, cfg)

        # add trigger SF
        events, variations = trigger_sf(events, variations, cfg)

        # add LeptonSF
        events, variations = lepton_sf(events, variations, ceval_lepton_sf, cfg)

        # FIXME add Muon Scale
        # FIXME add Electron Scale
        # FIXME add MET?

        # Jets corrections

        # JEC + JER + JES
        events, variations = correct_jets(events, variations, jec_stack)

        # puId SF
        events, variations = puid_sf(events, variations, ceval_puid)

        # btag SF
        events, variations = btag_sf(events, variations, ceval_btag, cfg)

        # Theory unc.
        events, variations = theory_unc(events, variations, cfg)

    # Define histograms
    axis = {
        "njet": hist.axis.Regular(10, 0, 10, name="njet"),
        "mjj": hist.axis.Regular(30, 200, 1500, name="mjj"),
        "detajj": hist.axis.Regular(30, -10, 10, name="detajj"),
        "dphijj": hist.axis.Regular(30, -np.pi, np.pi, name="dphijj"),
        "ptj1": hist.axis.Regular(30, 30, 150, name="ptj1"),
        "ptj2": hist.axis.Regular(30, 30, 150, name="ptj2"),
        "mll": hist.axis.Regular(20, 91 - 15, 91 + 15, name="mll"),
        "detall": hist.axis.Regular(20, -10, 10, name="detall"),
        "dphill": hist.axis.Regular(30, -np.pi, np.pi, name="dphill"),
        "ptl1": hist.axis.Regular(30, 15, 150, name="ptl1"),
        "ptl2": hist.axis.Regular(30, 15, 150, name="ptl2"),
    }

    regions = {
        # "top_cr": 0,
        # "vv_cr": 0,
        # "sr": 0,
        "sr_inc": 0,
        "sr_0j": 0,
        "sr_1j": 0,
        "sr_2j": 0,
        "sr_geq_2j": 0,
    }

    default_axis = [
        hist.axis.StrCategory(
            [f"{region}_{category}" for region in regions for category in ["ee", "mm"]],
            name="category",
        ),
        hist.axis.StrCategory(sorted(list(variations.keys())), name="syst"),
    ]

    results = {}
    results = {dataset: {"sumw": sumw, "nevents": nevents, "h": 0}}
    if dataset == "DY":
        results = {}
        for dataset_name in ["DY_hard", "DY_PU", "DY_inc"]:
            results[dataset_name] = {"sumw": sumw, "nevents": nevents, "h": 0}

    for dataset_name in results:
        histos = {}
        for variable in axis:
            histos[variable] = hist.Hist(
                axis[variable],
                *default_axis,
                hist.storage.Weight(),
            )

        results[dataset_name]["h"] = histos

    originalEvents = ak.copy(events)
    jet_pt_backup = ak.copy(events.Jet.pt)

    # FIXME support subsamples (mainly for Fake and DY hard)
    # FIXME add FakeW

    print("Doing variations")
    for variation in sorted(list(variations.keys())):
        events = ak.copy(originalEvents)
        assert ak.all(events.Jet.pt == jet_pt_backup)

        print(variation)
        for switch in variations[variation]:
            if len(switch) == 2:
                print(switch)
                variation_dest, variation_source = switch
                events[variation_dest] = events[variation_source]

        # resort Leptons
        lepton_sort = ak.argsort(events[("Lepton", "pt")], ascending=False, axis=1)
        events["Lepton"] = events.Lepton[lepton_sort]

        # l2tight
        events = events[(ak.num(events.Lepton, axis=1) >= 2)]

        eleWP = cfg["eleWP"]
        muWP = cfg["muWP"]

        comb = ak.ones_like(events.run) == 1.0
        for ilep in range(2):
            comb = comb & (
                events.Lepton[:, ilep]["isTightElectron_" + eleWP]
                | events.Lepton[:, ilep]["isTightMuon_" + muWP]
            )
        events = events[comb]

        # Jet real selections

        # resort Jets
        jet_sort = ak.argsort(events[("Jet", "pt")], ascending=False, axis=1)
        events["Jet"] = events.Jet[jet_sort]

        events["Jet"] = events.Jet[events.Jet.pt >= 30]
        # events = events[(ak.num(events.Jet[events.Jet.pt >= 30], axis=1) >= 2)]
        events["njet"] = ak.num(events.Jet, axis=1)
        # Define categories

        events["ee"] = (
            events.Lepton[:, 0].pdgId * events.Lepton[:, 1].pdgId
        ) == -11 * 11
        events["mm"] = (
            events.Lepton[:, 0].pdgId * events.Lepton[:, 1].pdgId
        ) == -13 * 13

        if not isData:
            # Require the two leading lepton to be prompt gen matched (!fakes)
            events = events[
                events.Lepton[:, 0].promptgenmatched
                & events.Lepton[:, 1].promptgenmatched
            ]

        # Analysis level cuts
        leptoncut = events.ee | events.mm

        # third lepton veto
        leptoncut = leptoncut & (
            ak.fill_none(
                ak.mask(
                    ak.all(events.Lepton[:, 2:].pt < 10, axis=-1),
                    ak.num(events.Lepton) >= 3,
                ),
                True,
                axis=0,
            )
        )

        # Cut on pt of two leading leptons
        leptoncut = (
            leptoncut & (events.Lepton[:, 0].pt > 25) & (events.Lepton[:, 1].pt > 15)
        )

        events = events[leptoncut]

        # # BTag

        # btag_cut = (
        #     (events.Jet.pt > 30)
        #     & (abs(events.Jet.eta) < 2.5)
        #     & (events.Jet.btagDeepFlavB > cfg["btagMedium"])
        # )
        # events["bVeto"] = ak.num(events.Jet[btag_cut]) == 0
        # events["bTag"] = ak.num(events.Jet[btag_cut]) >= 1

        if not isData:
            # Load all SFs
            events["PUID_SF"] = ak.prod(events.Jet.PUID_SF, axis=1)
            events["btagSF"] = ak.prod(events.Jet.btagSF_deepjet_shape, axis=1)
            events["weight"] = (
                events.weight * events.btagSF * events.PUID_SF * events.puWeight
            )

        # Variable definitions

        jets = ak.pad_none(events.Jet, 2)

        # Dijet
        events["mjj"] = ak.fill_none((jets[:, 0] + jets[:, 1]).mass, -9999)
        events["detajj"] = ak.fill_none(jets[:, 0].deltaeta(jets[:, 1]), -9999)
        events["dphijj"] = ak.fill_none(jets[:, 0].deltaphi(jets[:, 1]), -9999)

        # Single jet
        events["ptj1"] = ak.fill_none(jets[:, 0].pt, -9999)
        events["ptj2"] = ak.fill_none(jets[:, 1].pt, -9999)

        # Dilepton
        events["mll"] = (events.Lepton[:, 0] + events.Lepton[:, 1]).mass
        events["detall"] = events.Lepton[:, 0].deltaeta(events.Lepton[:, 1])
        events["dphill"] = events.Lepton[:, 0].deltaphi(events.Lepton[:, 1])

        # Single lepton
        events["ptl1"] = events.Lepton[:, 0].pt
        events["ptl2"] = events.Lepton[:, 1].pt

        # Apply cuts

        # # Preselection
        # events = events[(events.mjj > 200)]

        # Actual cuts for regions
        # regions["top_cr"] = events.bTag & (abs(events.mll - 91) >= 15)
        # regions["vv_cr"] = events.bVeto & (abs(events.mll - 91) >= 15)
        # regions["sr"] = events.bVeto & (abs(events.mll - 91) < 15)
        regions["sr_inc"] = abs(events.mll - 91) < 15
        regions["sr_0j"] = (abs(events.mll - 91) < 15) & (events.njet == 0)
        regions["sr_1j"] = (abs(events.mll - 91) < 15) & (events.njet == 1)
        regions["sr_2j"] = (abs(events.mll - 91) < 15) & (events.njet == 2)
        regions["sr_geq_2j"] = (abs(events.mll - 91) < 15) & (events.njet >= 2)

        events[dataset] = ak.ones_like(events.run) == 1.0

        if dataset == "DY":
            jet_genmatched = (jets.genJetIdx >= 0) & (
                jets.genJetIdx < ak.num(events.GenJet)
            )
            both_jets_gen_matched = ak.fill_none(
                jet_genmatched[:, 0] & jet_genmatched[:, 1], False
            )
            events["DY_hard"] = both_jets_gen_matched
            events["DY_PU"] = ~both_jets_gen_matched
            events["DY_inc"] = events[dataset]

        # Fill histograms
        for dataset_name in results:
            for variable in histos:
                for region in regions:
                    for category in ["ee", "mm"]:
                        mask = regions[region] & events[category] & events[dataset_name]
                        results[dataset_name]['h'][variable].fill(
                            events[variable][mask],
                            category=f"{region}_{category}",
                            syst=variation,
                            weight=events.weight[mask],
                        )

    return results


if __name__ == "__main__":
    with open("chunks_job.pkl", "rb") as file:
        new_chunks = cloudpickle.loads(zlib.decompress(file.read()))
    print("N chunks to process", len(new_chunks))

    results = {}
    errors = []
    for new_chunk in new_chunks:
        try:
            result = big_process(process=process, **new_chunk)
            results = add_dict(results, result)
        except Exception as e:
            print("\n\nError for chunk", new_chunk, file=sys.stderr)
            nice_exception = "".join(tb.format_exception(None, e, e.__traceback__))
            print(nice_exception, file=sys.stderr)
            errors.append(dict(**new_chunk, error=nice_exception))

    print("Results", results)
    print("Errors", errors)
    with open("results.pkl", "wb") as file:
        file.write(
            zlib.compress(cloudpickle.dumps({"results": results, "errors": errors}))
        )
