#!/usr/bin/env python
#
#   Plot fsk_demod statistic outputs.
#
#   Copyright (C) 2018  Mark Jessop <vk5qi@rfhead.net>
#   Released under GNU GPL v3 or later
#
import json
import os
import sys
import time
import subprocess
import numpy as np
import matplotlib.pyplot as plt

_filename = sys.argv[1]


_ebno = []
_fest1 = []
_fest2 = []
_ppm = []


_f = open(_filename,'r')

for _line in _f:

	if _line[0] != '{':
		continue

	try:
		_data = json.loads(_line)
	except Exception as e:
		print("Line parsing error: %s" % str(e))
		continue

	_ebno.append(_data['EbNodB'])
	_fest1.append(_data['f1_est'])
	_fest2.append(_data['f2_est'])
	_ppm.append(_data['ppm'])


plt.figure()

plt.plot(_ebno)
plt.xlabel("Sample Number")
plt.ylabel("Eb/N0 (dB)")
plt.title("Eb/N0")

plt.figure()

plt.plot(_fest1, label="f1 est")
plt.plot(_fest2, label="f2 est")
plt.legend()
plt.xlabel("Sample Number")
plt.ylabel("Frequency (Hz)")
plt.title("Frequency Estimator Outputs")


plt.figure()
plt.plot(_ppm)
plt.xlabel("Sample Number")
plt.ylabel("PPM")
plt.title("Demod PPM Estimate")

plt.show()