from coffea.lookup_tools.correctionlib_wrapper import correctionlib_wrapper
import awkward as ak

btag_base_var = [
    "central",
    "lf",
    "hf",
    "hfstats1",
    "hfstats2",
    "lfstats1",
    "lfstats2",
    "cferr1",
    "cferr2",
]
btag_jes_var = [
    "jes",
    "jesAbsolute",
    "jesAbsolute_RPLME_YEAR",
    "jesBBEC1",
    "jesBBEC1_RPLME_YEAR",
    "jesEC2",
    "jesEC2_RPLME_YEAR",
    "jesFlavorQCD",
    "jesHF",
    "jesHF_RPLME_YEAR",
    "jesRelativeBal",
    "jesRelativeSample_RPLME_YEAR",
]


def btag_sf(events, variations, ceval_btag, cfg):
    wrap_c = correctionlib_wrapper(ceval_btag["deepJet_shape"])
    branch_names = []
    variation_names = []
    for variation in btag_base_var + btag_jes_var:
        variation = variation.replace("RPLME_YEAR", cfg["year"])
        branch_name = "btagSF_deepjet_shape"
        if variation != "central":
            for tag in ["up", "down"]:
                variation_names.append(tag + "_" + variation)
                branch_names.append(branch_name + "_" + variation + "_" + tag)
        else:
            variation_names.append(variation)
            branch_names.append(branch_name)

    for branch_name, variation_name in zip(branch_names, variation_names):
        mask = (abs(events.Jet.eta) < 2.5) & (events.Jet.pt > 15.0)
        if "cferr" in variation_name:
            mask = mask & (events.Jet.hadronFlavour == 4)
        else:
            mask = mask & (
                (events.Jet.hadronFlavour == 0) | (events.Jet.hadronFlavour == 5)
            )
        jets_btag = ak.mask(events.Jet, mask)
        btags = wrap_c(
            variation_name,
            jets_btag.hadronFlavour,
            abs(jets_btag.eta),
            jets_btag.pt,
            jets_btag.btagDeepFlavB,
        )
        btags = ak.fill_none(btags, 1.0)
        events[("Jet", branch_name)] = btags
        variation_name_nice = ("_").join(variation_name.split("_")[1:])
        tag = variation_name.split("_")[0]
        variation_name_nice = f"btag_{variation_name_nice}_{tag}"
        variations[variation_name_nice] = (
            ("Jet", "btagSF_deepjet_shape"),
            ("Jet", branch_name),
        )
    return events, variations
