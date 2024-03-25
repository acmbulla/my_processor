import awkward as ak


def selectElectron(electron):
    return electron[
        (
            (abs(electron.eta) < 2.5)
            & (electron.mvaFall17V2Iso_WP90)
            & (electron.convVeto == 1)
            & (electron.pfRelIso03_all < 0.06)
        )
        & (
            (~(abs(electron.eta) < 1.479))
            | ((electron.dxy < 0.05) & (electron.dz < 0.1))
        )
        & (
            (~(abs(electron.eta) > 1.479))
            | ((electron.dxy < 0.1) & (electron.dz < 0.2))
        )
    ]


def selectMuon(muon):
    return muon[
        (
            (abs(muon.eta) < 2.4)
            & (muon.tightId == 1)
            & (muon.dz < 0.1)
            & (muon.pfRelIso04_all < 0.15)
        )
        & ((~(muon.pt <= 20.0)) | ((muon.dxy < 0.01)))
        & ((~(muon.pt > 20.0)) | ((muon.dxy < 0.02)))
    ]


def create_lepton(events):
    Lepton = ak.zip(
        {
            "pt": ak.concatenate([events.Electron.pt, events.Muon.pt], axis=1),
            "eta": ak.concatenate([events.Electron.eta, events.Muon.eta], axis=1),
            "phi": ak.concatenate([events.Electron.phi, events.Muon.phi], axis=1),
            "mass": ak.concatenate([events.Electron.mass, events.Muon.mass], axis=1),
            "pdgId": ak.concatenate([events.Electron.pdgId, events.Muon.pdgId], axis=1),
        },
        with_name="Momentum4D",
    )

    mu_mask = ak.fill_none(
        ak.mask(abs(Lepton.eta) < 2.4, abs(Lepton.pdgId) == 13), True, axis=-1
    )
    ele_mask = ak.fill_none(
        ak.mask(abs(Lepton.eta) < 2.5, abs(Lepton.pdgId) == 11), True, axis=-1
    )
    Lepton = Lepton[mu_mask & ele_mask]
    Lepton = Lepton[ak.argsort(Lepton.pt, ascending=False, axis=-1)]
    events["Lepton"] = Lepton
    return events
