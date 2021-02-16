# -*- coding: utf-8 -*-
"""Contains objects that will be implemented differently for future versions."""


# %% Imports
from reportio.future.tqdm.dask import TqdmCallback as ProgressBar

# %% Variables
__all__ = ['ProgressBar']
