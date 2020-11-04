# -*- coding: utf-8 -*-


import pytest
import time
import dask.delayed as dd

from ...future.progress import ProgressBar


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
