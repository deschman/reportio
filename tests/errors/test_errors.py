# -*- coding: utf-8 -*-


# %% Imports
# %%% 3rd Party
import pytest

# %%% User-Defined
from reportio import errors


# %% Classes
class test_ReportError:
    def test_raise() -> None:
        with pytest.raises(errors.ReportError):
            raise errors.ReportError


class test_LogError:
    def test_raise() -> None:
        with pytest.raises(errors.LogError):
            raise errors.LogError


class test_ConfigError:
    def test_raise() -> None:
        with pytest.raises(errors.ConfigError):
            raise errors.ConfigError


class test_ReportNameError:
    def test_raise() -> None:
        with pytest.raises(errors.ReportNameError):
            raise errors.ReportNameError


class test_DBConnectionError:
    def test_raise() -> None:
        with pytest.raises(errors.DBConnectionError):
            raise errors.DBConnectionError


class test_UnexpectedDbType:
    def test_raise() -> None:
        with pytest.raises(errors.UnexpectedDbType):
            raise errors.UnexpectedDbType


class test_DatasetNameError:
    def test_raise() -> None:
        with pytest.raises(errors.DatasetNameError):
            raise errors.DatasetNameError


class test_EmptyReport:
    def test_raise() -> None:
        with pytest.raises(errors.EmptyReport):
            raise errors.EmptyReport
