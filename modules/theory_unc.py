def theory_unc(events, variations, cfg):
    doTheoryVariations = cfg.get("do_theory_variations", True)
    if doTheoryVariations:
        # QCD Scale
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

        # Pdf Weights
        nVariations = len(events.LHEPdfWeight[0])
        for i, j in enumerate(range(nVariations)):
            events[f"weight_pdfWeight_{i}"] = events.weight * events.LHEPdfWeight[:, j]
            variations[f"PdfWeight_{i}"] = (
                ("weight",),
                (f"weight_pdfWeight_{i}",),
            )
    return events, variations
