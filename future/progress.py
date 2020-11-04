# -*- coding: utf-8 -*-


from dask.callbacks import Callback
from tqdm.auto import tqdm

class ProgressBar(Callback):
    """Progress bar for utilization with dask. Will remove when implemented in
    tqdm in #278 (https://github.com/tqdm/tqdm/issues/278).

    Example
    -------
    >>> from dask import delayed
    >>> from reporting.future.progress import ProgressBar

    >>> def inc(x):
    ...     return x + 1

    >>> x = delayed(inc)(10)

    >>> with ProgressBar():
    ...     x.compute()
    bar here
    11
    """
    def _start_state(self, dsk, state):
        self._tqdm = tqdm(total=sum(len(state[k]) for k in ['ready', 'waiting', 'running', 'finished']))

    def _posttask(self, key, result, dsk, state, worker_id):
        self._tqdm.update(1)

    def _finish(self, dsk, state, errored):
        pass