# -*- coding: utf-8 -*-
"""Contains objects that will be implemented differently for future versions."""


from reportio.future.tqdm.dask import TqdmCallback as ProgressBar

__all__ = ['ProgressBar']
