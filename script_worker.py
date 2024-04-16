import hist
import awkward as ak
import numpy as np
import uproot
import vector
import coffea
import correctionlib
from coffea.lumi_tools import LumiMask


from framework import add_dict, big_process
import variation as variation_module
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

# ceval_puid = correctionlib.CorrectionSet.from_file(cfg["puidSF"])
# ceval_btag = correctionlib.CorrectionSet.from_file(cfg["btagSF"])
# ceval_puWeight = correctionlib.CorrectionSet.from_file(cfg["puWeights"])
# ceval_lepton_sf = correctionlib.CorrectionSet.from_file(cfg["leptonSF"])
# jec_stack = getJetCorrections(cfg)
# rochester = getRochester(cfg)


def process(events, **kwargs):
    dataset = kwargs["dataset"]
    trigger_sel = kwargs.get("trigger_sel", "")
    isData = kwargs.get("is_data", False)

    variations = variation_module.Variation()
    variations.register_variation([], "nom")

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

    events = leptonSel(events)
    # Latinos definitions, only consider loose leptons
    # remove events where ptl1 < 8
    events["Lepton"] = events.Lepton[events.Lepton.isLoose]
    # Apply a skim!
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
    # events = correctRochester(events, isData, rochester)

    if not isData:
        # puWeight
        # events, variations = puweight_sf(events, variations, ceval_puWeight, cfg)

        # add trigger SF
        # events, variations = trigger_sf(events, variations, cfg)

        # add LeptonSF
        # events, variations = lepton_sf(events, variations, ceval_lepton_sf, cfg)

        # FIXME add Electron Scale
        # FIXME add MET corrections?

        # Jets corrections

        # JEC + JER + JES
        # events, variations = correct_jets(events, variations, jec_stack)

        # puId SF
        # events, variations = puid_sf(events, variations, ceval_puid)

        # btag SF
        events, variations = btag_sf(events, variations, ceval_btag, cfg)

        # Theory unc.
        events, variations = theory_unc(events, variations, cfg)

    # Define histograms
    axis = {
        "njet": hist.axis.Regular(10, 0, 10, name="njet"),
        "njet_50": hist.axis.Regular(10, 0, 10, name="njet_50"),
        "mjj": hist.axis.Regular(30, 200, 1500, name="mjj"),
        "detajj": hist.axis.Regular(30, -10, 10, name="detajj"),
        "dphijj": hist.axis.Regular(30, -np.pi, np.pi, name="dphijj"),
        "ptj1": hist.axis.Regular(30, 30, 150, name="ptj1"),
        "ptj1_check": hist.axis.Variable(
            [-9999.0, -10, 0] + list(np.linspace(30, 150, 31)), name="ptj1_check"
        ),
        "ptj2": hist.axis.Regular(30, 30, 150, name="ptj2"),
        "etaj1": hist.axis.Regular(30, -5, 5, name="etaj1"),
        "etaj2": hist.axis.Regular(30, -5, 5, name="etaj2"),
        "phij1": hist.axis.Regular(30, -np.pi, np.pi, name="phij1"),
        "phij2": hist.axis.Regular(30, -np.pi, np.pi, name="phij2"),
        "mll": hist.axis.Regular(20, 91 - 15, 91 + 15, name="mll"),
        "ptll": hist.axis.Regular(30, 0, 150, name="ptll"),
        "detall": hist.axis.Regular(20, -10, 10, name="detall"),
        "dphill": hist.axis.Regular(30, -np.pi, np.pi, name="dphill"),
        "ptl1": hist.axis.Regular(30, 15, 150, name="ptl1"),
        "ptl2": hist.axis.Regular(30, 15, 150, name="ptl2"),
        "etal1": hist.axis.Regular(30, -2.5, 2.5, name="etal1"),
        "etal2": hist.axis.Regular(30, -2.5, 2.5, name="etal2"),
        "phil1": hist.axis.Regular(30, -np.pi, np.pi, name="phil1"),
        "phil2": hist.axis.Regular(30, -np.pi, np.pi, name="phil2"),
        "dR_l1_jets": hist.axis.Regular(100, -1, 5, name="dR_l1_jets"),
        "dR_l2_jets": hist.axis.Regular(100, -1, 5, name="dR_l2_jets"),
        "dR_l1_l2": hist.axis.Regular(100, -1, 5, name="dR_l1_l2"),
    }

    regions = {
        # "top_cr": 0,
        # "vv_cr": 0,
        # "sr": 0,
        "sr_inc": 0,
        # "sr_0j": 0,
        # "sr_1j": 0,
        # "sr_2j": 0,
        "sr_geq_1j": 0,
        "sr_geq_2j": 0,
        "sr_geq_2j_bveto": 0,
        "sr_geq_2j_btag": 0,
    }

    btag_regions = {}

    default_axis = [
        hist.axis.StrCategory(
            [f"{region}" for region in regions],
            name="category",
        ),
        hist.axis.StrCategory(
            sorted(list(variations.get_variations_all())), name="syst"
        ),
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

    # FIXME add FakeW

    print("Doing variations")
    # for variation in sorted(list(variations.keys())):
    for variation in sorted(variations.get_variations_all()):
        events = ak.copy(originalEvents)
        assert ak.all(events.Jet.pt == jet_pt_backup)

        print(variation)
        # for switch in variations[variation]:
        for switch in variations.get_variation_subs(variation):
            if len(switch) == 2:
                print(switch)
                variation_dest, variation_source = switch
                events[variation_dest] = events[variation_source]

        # resort Leptons
        lepton_sort = ak.argsort(events[("Lepton", "pt")], ascending=False, axis=1)
        events["Lepton"] = events.Lepton[lepton_sort]

        # l2tight
        events = events[(ak.num(events.Lepton, axis=1) >= 2)]

        # eleWP = cfg["eleWP"]
        # muWP = cfg["muWP"]

        # comb = ak.ones_like(events.run) == 1.0
        # for ilep in range(2):
        #     comb = comb & (
        #         events.Lepton[:, ilep]["isTightElectron_" + eleWP]
        #         | events.Lepton[:, ilep]["isTightMuon_" + muWP]
        #     )
        # events = events[comb]

        # Jet real selections

        # resort Jets
        jet_sort = ak.argsort(events[("Jet", "pt")], ascending=False, axis=1)
        events["Jet"] = events.Jet[jet_sort]

        events['ptj1_check'] = ak.fill_none(ak.pad_none(events.Jet.pt, 1)[:, 0], -9999)

        # events["Jet"] = events.Jet[events.Jet.pt >= 30]
        # events = events[(ak.num(events.Jet[events.Jet.pt >= 30], axis=1) >= 2)]
        events["njet"] = ak.num(events.Jet, axis=1)
        events["njet_50"] = ak.num(events.Jet[events.Jet.pt >= 50], axis=1)
        # Define categories

        # events["ee"] = (
        #     events.Lepton[:, 0].pdgId * events.Lepton[:, 1].pdgId
        # ) == -11 * 11
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
        # leptoncut = events.ee | events.mm
        leptoncut = events.mm

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

        # BTag

        btag_cut = (
            (events.Jet.pt > 30)
            & (abs(events.Jet.eta) < 2.5)
            & (events.Jet.btagDeepFlavB > cfg["btagMedium"])
        )
        events["bVeto"] = ak.num(events.Jet[btag_cut]) == 0
        events["bTag"] = ak.num(events.Jet[btag_cut]) >= 1

        if not isData:
            # Load all SFs
            # FIXME should remove btagSF
            events["btagSF"] = ak.prod(
                events.Jet[events.Jet.pt >= 30].btagSF_deepjet_shape, axis=1
            )
            events["PUID_SF"] = ak.prod(events.Jet.PUID_SF, axis=1)
            events["RecoSF"] = events.Lepton[:, 0].RecoSF * events.Lepton[:, 1].RecoSF
            events["TightSF"] = (
                events.Lepton[:, 0].TightSF * events.Lepton[:, 1].TightSF
            )

            events["weight"] = (
                events.weight
                * events.puWeight
                * events.PUID_SF
                * events.RecoSF
                * events.TightSF
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
        events["etaj1"] = ak.fill_none(jets[:, 0].eta, -9999)
        events["etaj2"] = ak.fill_none(jets[:, 1].eta, -9999)
        events["phij1"] = ak.fill_none(jets[:, 0].phi, -9999)
        events["phij2"] = ak.fill_none(jets[:, 1].phi, -9999)

        # Dilepton
        events["mll"] = (events.Lepton[:, 0] + events.Lepton[:, 1]).mass
        events["ptll"] = (events.Lepton[:, 0] + events.Lepton[:, 1]).pt
        events["detall"] = events.Lepton[:, 0].deltaeta(events.Lepton[:, 1])
        events["dphill"] = events.Lepton[:, 0].deltaphi(events.Lepton[:, 1])

        # Single lepton
        events["ptl1"] = events.Lepton[:, 0].pt
        events["ptl2"] = events.Lepton[:, 1].pt
        events["etal1"] = events.Lepton[:, 0].eta
        events["etal2"] = events.Lepton[:, 1].eta
        events["phil1"] = events.Lepton[:, 0].phi
        events["phil2"] = events.Lepton[:, 1].phi

        events["dR_l1_jets"] = ak.fill_none(
            ak.min(events.Lepton[:, 0].deltaR(jets[:, :]), axis=1), -1
        )
        events["dR_l2_jets"] = ak.fill_none(
            ak.min(events.Lepton[:, 1].deltaR(jets[:, :]), axis=1), -1
        )
        events["dR_l1_l2"] = events.Lepton[:, 0].deltaR(events.Lepton[:, 1])

        # Apply cuts

        # Preselection

        # events = events[(events.mjj > 200)]
        # jets = jets[events.bVeto]
        # events = events[events.bVeto]

        # Actual cuts for regions
        # regions["top_cr"] = events.bTag & (abs(events.mll - 91) >= 15)
        # regions["vv_cr"] = events.bVeto & (abs(events.mll - 91) >= 15)
        # regions["sr"] = events.bVeto & (abs(events.mll - 91) < 15)
        regions["sr_inc"] = abs(events.mll - 125) < 30
        # regions["sr_0j"] = (abs(events.mll - 91) < 15) & (events.njet == 0)
        # regions["sr_1j"] = (abs(events.mll - 91) < 15) & (events.njet == 1)
        # regions["sr_2j"] = (abs(events.mll - 91) < 15) & (events.njet == 2)
        # regions["sr_geq_1j"] = (abs(events.mll - 91) < 15) & (events.njet >= 1)
        regions["sr_geq_2j"] = (abs(events.mll - 125) < 30) & (events.njet >= 2)
        regions["sr_geq_2j_bveto"] = (abs(events.mll - 125) < 30) & (events.njet >= 2)
        regions["sr_geq_2j_btag"] = (abs(events.mll - 125) < 30) & (events.njet >= 2)
        btag_regions["sr_geq_2j_bveto"] = "bVeto"
        btag_regions["sr_geq_2j_btag"] = "bTag"

        events[dataset] = ak.ones_like(events.run) == 1.0

        if dataset == "DY":
            gen_photons = (
                (events.GenPart.pdgId == 22)
                & ak.values_astype(events.GenPart.statusFlags & 1, bool)
                & (events.GenPart.status == 1)
                & (events.GenPart.pt > 15)
                & (abs(events.GenPart.eta) < 2.6)
            )
            gen_mask = ak.num(events.GenPart[gen_photons]) == 0
            jet_genmatched = (jets.genJetIdx >= 0) & (
                jets.genJetIdx < ak.num(events.GenJet)
            )
            both_jets_gen_matched = ak.fill_none(
                jet_genmatched[:, 0] & jet_genmatched[:, 1], False
            )
            events["DY_hard"] = gen_mask & both_jets_gen_matched
            events["DY_PU"] = gen_mask & ~both_jets_gen_matched
            events["DY_inc"] = gen_mask & events[dataset]
            # # DY ptll reweight
            # ceval_ptll = correctionlib.CorrectionSet.from_file(
            #     "/gwpool/users/gpizzati/test_processor/my_processor/data/ptll_dy_rwgt.json.gz"
            # )
            # events["rwgt_ptll_ee"] = ceval_ptll["ptll_rwgt_dy_ee"].evaluate(events.ptll)
            # events["rwgt_ptll_mm"] = ceval_ptll["ptll_rwgt_dy_mm"].evaluate(events.ptll)
            # events["weight"] = events.weight * ak.where(
            #     events.ee, events.rwgt_ptll_ee, events.rwgt_ptll_mm
            # )

        # Fill histograms
        for dataset_name in results:
            for region in regions:
                # Apply mask for specific region, category and dataset_name
                mask = regions[region] & events[category] & events[dataset_name]

                if not isData:
                    # Renorm for btag in region
                    sumw_before_btagsf = ak.sum(events[mask].weight)
                    events["weight_btag"] = events.weight * events.btagSF
                    sumw_after_btagsf = ak.sum(events[mask].weight_btag)
                    btag_norm = sumw_before_btagsf / sumw_after_btagsf
                    # print(dataset_name, region, cat, 'btag norm', btag_norm)
                    events["weight_btag"] = events.weight_btag * btag_norm
                else:
                    events["weight_btag"] = events.weight

                btag_cut = btag_regions.get(region, dataset_name)
                mask = mask & events[btag_cut]
                for variable in histos:
                    results[dataset_name]["h"][variable].fill(
                        events[variable][mask],
                        category=f"{region}",
                        syst=variation,
                        weight=events.weight_btag[mask],
                    )

    return results


if __name__ == "__main__":
    with open("chunks_job.pkl", "rb") as file:
        new_chunks = cloudpickle.loads(zlib.decompress(file.read()))
    print("N chunks to process", len(new_chunks))

    results = {}
    errors = []
    for new_chunk in new_chunks:
        # if new_chunk['dataset'] != 'DY': continue
        try:
            result = big_process(process=process, **new_chunk)
            results = add_dict(results, result)
        except Exception as e:
            print("\n\nError for chunk", new_chunk, file=sys.stderr)
            nice_exception = "".join(tb.format_exception(None, e, e.__traceback__))
            print(nice_exception, file=sys.stderr)
            errors.append(dict(**new_chunk, error=nice_exception))
        # break

    print("Results", results)
    print("Errors", errors)
    with open("results.pkl", "wb") as file:
        file.write(
            zlib.compress(cloudpickle.dumps({"results": results, "errors": errors}))
        )
