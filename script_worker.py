import hist

import awkward as ak

import uproot

import vector
import coffea

from framework import add_dict, big_process
from modules.basic_selections import lumi_mask, pass_flags, pass_trigger
from modules.jet_sel import cleanJet, jetSel
from modules.jme import getJetCorrections, correct_jets
from modules.lepton_sel import create_lepton, selectElectron, selectMuon
from modules.prompt_gen import prompt_gen_match_leptons
from modules.puid import puid_sf
from modules.btag import btag_sf

import correctionlib
from coffea.lumi_tools import LumiMask
import cloudpickle
import zlib
import sys
import json

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
    # read_form = kwargs.get("read_form", {})
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

    events[("Muon", "mass")] = ak.zeros_like(events.Muon.pt)
    events[("Electron", "mass")] = ak.zeros_like(events.Electron.pt)

    events = pass_trigger(events, cfg["tgr_data"])
    events = pass_flags(events, cfg["flags"])

    events = events[events.pass_flags & events.pass_trigger]

    if isData:
        events = events[eval(trigger_sel)]

    events = jetSel(events)

    events["Electron"] = selectElectron(events.Electron)
    events["Muon"] = selectMuon(events.Muon)
    events = create_lepton(events)
    events["Lepton"] = events.Lepton[events.Lepton.pt > 10]

    if not isData:
        events = prompt_gen_match_leptons(events)

    events = cleanJet(events)

    # l2tight
    events = events[
        (ak.num(events.Lepton, axis=1) >= 2) & (ak.num(events.Jet, axis=1) >= 2)
    ]

    # MCCorr
    # Should load SF and corrections here
    if not isData:
        # correct jets
        jec_stack = getJetCorrections(cfg)
        events, variations = correct_jets(events, variations, jec_stack)
        # reapply jetSel to remove jets that have a low pt after jec + jer
        events = jetSel(events)

        # puId SF
        events, variations = puid_sf(events, variations, ceval_puid)

        # btag SF
        events, variations = btag_sf(events, variations, ceval_btag, cfg)

        # puWeight
        events["puWeight"] = ceval_puWeight[
            "Collisions18_UltraLegacy_goldenJSON"
        ].evaluate(events.Pileup.nTrueInt, "nominal")
        events["weight"] = events.weight * events.puWeight

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

    # events = events[(ak.num(events.Lepton, axis=1) >= 2)]

    events["ee"] = (events.Lepton[:, 0].pdgId * events.Lepton[:, 1].pdgId) == -11 * 11
    events["mm"] = (events.Lepton[:, 0].pdgId * events.Lepton[:, 1].pdgId) == -13 * 13

    axis = [
        hist.axis.Regular(30, 200, 1500, name="mjj"),
        hist.axis.Regular(20, 91 - 15, 91 + 15, name="mll"),
        hist.axis.StrCategory(["ee", "mm"], name="category"),
        hist.axis.StrCategory(sorted(list(variations.keys())), name="syst"),
    ]

    h = hist.Hist(
        *axis,
        hist.storage.Weight(),
    )

    originalEvents = events[:]

    print("Doing variations")
    for variation in sorted(list(variations.keys())):
        events = originalEvents[:]
        # print(variation, events.weight[:10], file=sys.stderr)

        if len(variations[variation]) == 2:
            variation_dest, variation_source = variations[variation]
            events[variation_dest] = events[variation_source]

        events["Jet"] = events.Jet[events.Jet.pt >= 30]

        events = events[
            (ak.num(events.Lepton, axis=1) >= 2) & (ak.num(events.Jet, axis=1) >= 2)
        ]

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

        for category in ["ee", "mm"]:
            mask = events[category]
            h.fill(
                mjj=events.mjj[mask],
                mll=events.mll[mask],
                category=category,
                syst=variation,
                weight=events.weight[mask],
            )

    return {dataset: {"sumw": sumw, "nevents": nevents, "h": h}}


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
