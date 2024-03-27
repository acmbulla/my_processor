import hist
import awkward as ak
import uproot
import vector
import coffea
import correctionlib
from coffea.lumi_tools import LumiMask


from framework import add_dict, big_process
from modules.basic_selections import lumi_mask, pass_flags, pass_trigger
from modules.jet_sel import cleanJet, jetSel
from modules.jme import getJetCorrections, correct_jets
from modules.lepton_sel import createLepton, leptonSel
from modules.prompt_gen import prompt_gen_match_leptons
from modules.puid import puid_sf
from modules.btag import btag_sf

import cloudpickle
import zlib
import sys
import json

from modules.puweight import puweight_sf

vector.register_awkward()

print("uproot version", uproot.__version__)
print("awkward version", ak.__version__)

print("coffea version", coffea.__version__)


with open("cfg.json") as file:
    cfg = json.load(file)

ceval_puid = correctionlib.CorrectionSet.from_file(cfg["puidSF"])
ceval_btag = correctionlib.CorrectionSet.from_file(cfg["btagSF"])
ceval_puWeight = correctionlib.CorrectionSet.from_file(cfg["puWeights"])


def process(events, **kwargs):
    dataset = kwargs["dataset"]
    trigger_sel = kwargs.get("trigger_sel", "")
    isData = kwargs.get("is_data", False)

    variations = {}
    variations["nom"] = (0,)

    # events = load_branches(events, read_form)

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
    events["Lepton"] = events.Lepton[events.Lepton.pt > 10]
    events = leptonSel(events)

    if not isData:
        events = prompt_gen_match_leptons(events)

    events = cleanJet(events)

    # l2tight
    events = events[
        (ak.num(events.Lepton, axis=1) >= 2) & (ak.num(events.Jet, axis=1) >= 2)
    ]

    # MCCorr
    # Should load SF and corrections here

    # FIXME add rochester

    if not isData:

        # FIXME add trigger SF

        # FIXME add LeptonSF

        # correct jets
        jec_stack = getJetCorrections(cfg)
        events, variations = correct_jets(events, variations, jec_stack)

        # puId SF
        events, variations = puid_sf(events, variations, ceval_puid)

        # btag SF
        events, variations = btag_sf(events, variations, ceval_btag, cfg)

        # puWeight
        events, variations = puweight_sf(events, variations, ceval_puWeight, cfg)
        events["weight"] = events.weight * events.puWeight

        # Theory unc.
        nVariations = len(events.LHEScaleWeight[0])
        for i, j in enumerate(
            [
                0,
                1,
                2,
                3,
                nVariations - 1,
                nVariations - 2,
                nVariations - 3,
                nVariations - 4,
            ]
        ):
            events[f"weight_qcdScale_{i}"] = events.weight * events.LHEScaleWeight[:, j]
            variations[f"QCDscale_{i}"] = (
                ("weight",),
                (f"weight_qcdScale_{i}",),
            )

        nVariations = len(events.LHEPdfWeight[0])
        for i, j in enumerate(range(nVariations)):
            events[f"weight_pdfWeight_{i}"] = events.weight * events.LHEPdfWeight[:, j]
            variations[f"PdfWeight_{i}"] = (
                ("weight",),
                (f"weight_pdfWeight_{i}",),
            )

    # Define histograms
    axis = {
        "mjj": hist.axis.Regular(30, 200, 1500, name="mjj"),
        "mll": hist.axis.Regular(20, 91 - 15, 91 + 15, name="mll"),
    }

    default_axis = [
        hist.axis.StrCategory(["ee", "mm"], name="category"),
        hist.axis.StrCategory(sorted(list(variations.keys())), name="syst"),
    ]

    histos = {}
    for variable in axis:
        histos[variable] = hist.Hist(
            axis[variable],
            *default_axis,
            hist.storage.Weight(),
        )

    originalEvents = events[:]

    print("Doing variations")
    for variation in sorted(list(variations.keys())):
        events = originalEvents[:]

        if len(variations[variation]) == 2:
            variation_dest, variation_source = variations[variation]
            events[variation_dest] = events[variation_source]

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

        # reapply jetSel to remove jets that have a low pt after jec + jer
        events = jetSel(events)
        events["Jet"] = events.Jet[events.Jet.pt >= 30]
        events = events[(ak.num(events.Jet, axis=1) >= 2)]

        events["ee"] = (
            events.Lepton[:, 0].pdgId * events.Lepton[:, 1].pdgId
        ) == -11 * 11
        events["mm"] = (
            events.Lepton[:, 0].pdgId * events.Lepton[:, 1].pdgId
        ) == -13 * 13

        if not isData:
            events = events[
                events.Lepton[:, 0].promptgenmatched
                & events.Lepton[:, 1].promptgenmatched
            ]

        # Analysis level cuts
        leptoncut = events.ee | events.mm
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
        leptoncut = (
            leptoncut & (events.Lepton[:, 0].pt > 25) & (events.Lepton[:, 1].pt > 15)
        )
        events = events[leptoncut]

        if not isData:
            events["PUID_SF"] = ak.prod(events.Jet.PUID_SF, axis=1)
            events["btagSF"] = ak.prod(events.Jet.btagSF_deepjet_shape, axis=1)
            events["weight"] = events.weight * events.btagSF * events.PUID_SF

        events["mjj"] = (events.Jet[:, 0] + events.Jet[:, 1]).mass
        events["mll"] = (events.Lepton[:, 0] + events.Lepton[:, 1]).mass
        events = events[(abs(events.mll - 91) < 15) & (events.mjj > 200)]

        for variable in histos:
            for category in ["ee", "mm"]:
                mask = events[category]
                histos[variable].fill(
                    events[variable][mask],
                    category=category,
                    syst=variation,
                    weight=events.weight[mask],
                )

    return {dataset: {"sumw": sumw, "nevents": nevents, "h": histos}}


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
            print(e, file=sys.stderr)
            errors.append(dict(**new_chunk, error=e))

    print("Results", results)
    print("Errors", errors)
    with open("results.pkl", "wb") as file:
        file.write(
            zlib.compress(cloudpickle.dumps({"results": results, "errors": errors}))
        )
