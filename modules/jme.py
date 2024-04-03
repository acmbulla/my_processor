import awkward as ak
import numpy as np
from coffea.jetmet_tools import JECStack, CorrectedJetsFactory
from coffea.lookup_tools import extractor


def getJetCorrections(cfg):

    jec_stack_names = cfg["JME"]["jec_stack_names"]
    jec_stack_paths = cfg["JME"]["jec_stack_paths"]
    junc = cfg["JME"]["junc"]

    ext = extractor()
    for path in jec_stack_paths:
        ext.add_weight_sets(["* * " + path])

    ext.finalize()
    evaluator = ext.make_evaluator()

    jec_stack_names += list(filter(lambda k: junc in k, evaluator.keys()))
    jec_stack_names = list(filter(lambda k: k != junc, jec_stack_names))
    jec_inputs = {name: evaluator[name] for name in jec_stack_names}
    jec_stack = JECStack(jec_inputs)
    return jec_stack


def correct_jets(events, variations, jec_stack):
    events[("Jet", "trueGenJetIdx")] = ak.mask(
        events.Jet.genJetIdx,
        (events.Jet.genJetIdx >= 0) & (events.Jet.genJetIdx < ak.num(events.GenJet)),
    )
    name_map = jec_stack.blank_name_map
    name_map["JetPt"] = "pt"
    name_map["JetMass"] = "mass"
    name_map["JetEta"] = "eta"
    name_map["JetA"] = "area"

    jets = ak.with_name(
        events.Jet, ""
    )  # Remove Momentum4D methods because will add 'rho'

    jets["pt_raw"] = (1 - jets["rawFactor"]) * jets["pt"]
    jets["mass_raw"] = (1 - jets["rawFactor"]) * jets["mass"]
    jets["pt_gen"] = ak.values_astype(
        ak.fill_none(events.GenJet[events.Jet.trueGenJetIdx].pt, 0), np.float32
    )
    jets["rho"] = ak.broadcast_arrays(events.fixedGridRhoFastjetAll, jets.pt)[0]
    name_map["ptGenJet"] = "pt_gen"
    name_map["ptRaw"] = "pt_raw"
    name_map["massRaw"] = "mass_raw"
    name_map["Rho"] = "rho"
    # jets["_rho"] = ak.broadcast_arrays(events.fixedGridRhoFastjetAll, jets.pt)[0]
    # name_map["Rho"] = (
    #     "_rho"  # very important! Cannot name it rho otherwise vector will skrew things up
    # )

    jet_factory = CorrectedJetsFactory(name_map, jec_stack)

    corrected_jets = jet_factory.build(jets).compute()

    br = list(set(ak.fields(corrected_jets)))
    br = list(
        filter(lambda k: ("JES" in k) or "JER" in k, br)  # and "total" not in k.lower()
    )
    print(br)
    for variation in br:
        for tag in ["up", "down"]:
            for variable in ["pt", "mass"]:
                new_branch_name = f"{variable}_{variation}_{tag}"
                events[("Jet", new_branch_name)] = corrected_jets[
                    (variation, tag, variable)
                ]
                variation_key = f"{variation}_{tag}"
                if variation_key not in variations:
                    variations[variation_key] = [
                        (
                            ("Jet", variable),
                            ("Jet", new_branch_name),
                        )
                    ]
                else:
                    variations[variation_key].append(
                        (
                            ("Jet", variable),
                            ("Jet", new_branch_name),
                        )
                    )

    # events[("Jet", "pt_orig")] = corrected_jets.pt_orig

    events[("Jet", "pt")] = corrected_jets.pt
    events[("Jet", "mass")] = corrected_jets.mass

    # events[('Jet', 'trueGenJetIdx')] = None
    return events, variations
