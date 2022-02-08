# -*- coding: utf-8 -*-
"""Initializes future module of reportio."""


from reportio.future.tqdm.dask import TqdmCallback as ProgressBar


__all__ = ['ProgressBar']
