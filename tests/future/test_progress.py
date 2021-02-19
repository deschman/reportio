# -*- coding: utf-8 -*-


# %% Imports
# %%% Py3 Standard
import time

# %%% 3rd Party
import pytest
import dask.delayed as dd

# %%% User-Defined
from reportio.future.progress import ProgressBar


# %% Classes
class test_ProgressBar:
    def test_SampleProgram(self, fltDelay):
        time.sleep(fltDelay)
        return fltDelay

    def test_ProgressBar(self, ProgressBar):
        a = dd(self.test_SampleProgram)(3)
        b = dd(self.test_SampleProgram)(2)
        c = dd(self.test_SampleProgram)(a + b)
        with ProgressBar:
            c.compute()
