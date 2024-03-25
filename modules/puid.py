import awkward as ak
from coffea.lookup_tools.correctionlib_wrapper import correctionlib_wrapper


def puid_sf(events, variations, ceval_puid):
    wrap_c = correctionlib_wrapper(ceval_puid["PUJetID_eff"])

    puId_shift = 1 << 2
    pass_puId = ak.values_astype(events.Jet.puId & puId_shift, bool)

    jet_genmatched = (events.Jet.genJetIdx >= 0) & (
        events.Jet.genJetIdx < ak.num(events.GenJet)
    )
    mask = jet_genmatched & pass_puId & (events.Jet.pt < 50.0)
    jets = ak.mask(events.Jet, mask)
    sf = wrap_c(jets.eta, jets.pt, "nom", "L")
    sf_up = wrap_c(jets.eta, jets.pt, "up", "L")
    sf_down = wrap_c(jets.eta, jets.pt, "down", "L")
    # effMC   = wrap_c(jets.eta, jets.pt, "MCEff", "L")

    sf = ak.fill_none(sf, 1.0)
    sf_up = ak.fill_none(sf_up, 1.0)
    sf_down = ak.fill_none(sf_down, 1.0)

    events[("Jet", "PUID_SF")] = sf
    events[("Jet", "PUID_SF_up")] = sf_up
    events[("Jet", "PUID_SF_down")] = sf_down
    variations['PUID_SF_up'] = (('Jet', 'PUID_SF'), ('Jet', 'PUID_SF_up'))
    variations['PUID_SF_down'] = (('Jet', 'PUID_SF'), ('Jet', 'PUID_SF_down'))

    return events, variations
