import hist
import numpy as np
import cloudpickle
import zlib

with open("results.pkl", "rb") as file:
    results = cloudpickle.loads(zlib.decompress(file.read()))

results = results["results"]
scale = 6077.22 * 1000 / results["DY"]["sumw"]
print(scale)
h = results["DY"]["h"]
a = h.view(True)
a.value = a.value * scale
a.variance = a.variance * scale * scale
nom = h[:, :, hist.loc("mm"), hist.loc("nom")].project("mjj").values()
stat = np.sqrt(h[:, :, hist.loc("mm"), hist.loc("nom")].project("mjj").variances())
print('Stat')
print(stat/nom)
variations = ["JES_HF_up", "JES_HF_down", "btag_hfup", "btag_hfdown"]
for variation in variations:
    var = h[:, :, hist.loc("mm"), hist.loc(variation)].project("mjj").values()
    print(variation)
    print(var / nom)
    print(abs(var / nom - 1) < stat/nom)
