import os
import requests
import json
import subprocess

# Setup all the needed data

cfg = {
    "year": "2018",
    "tgr_data": {
        "EleMu": [
            "Mu23_TrkIsoVVL_Ele12_CaloIdL_TrackIdL_IsoVL",
            "Mu12_TrkIsoVVL_Ele23_CaloIdL_TrackIdL_IsoVL_DZ",
        ],
        "DoubleMu": ["Mu17_TrkIsoVVL_Mu8_TrkIsoVVL_DZ_Mass3p8"],
        "SingleMu": ["IsoMu24"],
        "DoubleEle": ["Ele23_Ele12_CaloIdL_TrackIdL_IsoVL"],
        "SingleEle": ["Ele32_WPTight_Gsf"],
    },
    "flags": [
        "goodVertices",
        "globalSuperTightHalo2016Filter",
        "HBHENoiseFilter",
        "HBHENoiseIsoFilter",
        "EcalDeadCellTriggerPrimitiveFilter",
        "BadPFMuonFilter",
        "BadPFMuonDzFilter",
        "ecalBadCalibFilter",
        "eeBadScFilter",
    ],
    "eleWP": "mvaFall17V1Iso_WP90",
    "muWP": "cut_Tight_HWWW",
    "puWeightsKey": "Collisions18_UltraLegacy_goldenJSON"
}


basedir = os.path.abspath(".") + "/data/2018/jme/"
os.makedirs(basedir, exist_ok=True)

jet_object = "AK4PFchs"
production_jec = "Summer19UL18_V5_MC"
production_jer = "Summer19UL18_JRV2_MC"
base_url_jec = "https://raw.githubusercontent.com/cms-jet/JECDatabase/master/textFiles/"
base_url_jer = "https://raw.githubusercontent.com/cms-jet/JRDatabase/master/textFiles/"

files = ["L1FastJet", "L2Relative", "L3Absolute", "L2L3Residual"]
files = [production_jec + "_" + file + "_" + jet_object for file in files] + [
    f"Regrouped_{production_jec}_UncertaintySources" + "_" + jet_object
]
postfixes = 4 * ["jec"] + ["junc"]
urls = [base_url_jec + production_jec + "/" + file + ".txt" for file in files]

files_jer = [
    production_jer + "_PtResolution_" + jet_object,
    production_jer + "_SF_" + jet_object,
]
postfixes += ["jr", "jrsf"]
urls += [base_url_jer + production_jer + "/" + file + ".txt" for file in files_jer]
files += files_jer
print(files)

print(urls)


paths = []
jec_names = []
junc = 0
for file, url, postfix in zip(files, urls, postfixes):
    new_filename = f"{file}.{postfix}.txt"
    if postfix == "junc":
        junc = file
    # print(url)

    with open(basedir + new_filename, "w") as f:
        f.write(requests.get(url).text)
    paths.append(basedir + new_filename)
    jec_names.append(file)

print(paths)
print(jec_names)
print(junc)
print("\n\n")
jme = {"jec_stack_names": jec_names, "jec_stack_paths": paths, "junc": junc}

cfg["JME"] = jme

files_to_copy = [
    "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/JME/2018_UL/jmar.json.gz",
    "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2018_UL/btagging.json.gz",
    "/cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/LUM/2018_UL/puWeights.json.gz",
]
keys = ["puidSF", "btagSF", "puWeights"]


basedir = os.path.abspath(".") + "/data/2018/clib/"
os.makedirs(basedir, exist_ok=True)

for key, file_to_copy in zip(keys, files_to_copy):
    fname = file_to_copy.split("/")[-1]
    cfg[key] = f"{basedir}{fname}"
    proc = subprocess.Popen(f"cp {file_to_copy} {basedir}", shell=True)
    proc.wait()

files_to_download = [
    "https://cms-service-dqmdc.web.cern.ch/CAF/certification/Collisions18/13TeV/Legacy_2018/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt"
]
keys = ["lumiMask"]

basedir = os.path.abspath(".") + "/data/2018/lumimasks/"
os.makedirs(basedir, exist_ok=True)

for key, file_to_download in zip(keys, files_to_download):
    fname = basedir + file_to_download.split("/")[-1]
    cfg[key] = fname
    with open(fname, "wb") as file:
        file.write(requests.get(file_to_download).content)

print(json.dumps(cfg, indent=2))

with open("data/cfg.json", "w") as file:
    json.dump(cfg, file, indent=2)
