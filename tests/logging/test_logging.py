# -*- coding: utf-8 -*-


# %% Imports
# %%% 3rd Party
import pytest

# %%% User-Defined
from reportio import logging


# %% Classes
class test_logging:

    # %%% Variables
    strTest = 'test'

    # %%% Functions
    def test_basicConfig(self, tmpdir):
        def test_existing(tmpdir):
            assert logging.basicConfig(tmpdir.mkdir("test").join("test.txt")
                                       ) == 'existing'

        def test_new():
            assert logging.basicConfig(tmpdir.mkdir("test").join("test.txt"),
                                       'w') == 'new'

    def test_getThreadScope(self):
        assert logging.getThreadScope() in [True, False]

    def test_debug(self, tmpdir, strTest):
        objFile = tmpdir.mkdir("test").join("test.txt")
        logging.basicConfig(objFile.name)
        logging.debug(strTest)
        assert strTest in objFile.read()

    def test_info(self, tmpdir, strTest):
        objFile = tmpdir.mkdir("test").join("test.txt")
        logging.basicConfig(objFile.name)
        logging.info(strTest)
        assert strTest in objFile.read()

    def test_warning(self, tmpdir, strTest):
        objFile = tmpdir.mkdir("test").join("test.txt")
        logging.basicConfig(objFile.name)
        logging.warning(strTest)
        assert strTest in objFile.read()

    def test_error(self, tmpdir, strTest):
        objFile = tmpdir.mkdir("test").join("test.txt")
        logging.basicConfig(objFile.name)
        logging.error(strTest)
        assert strTest in objFile.read()

    def test_critical(self, tmpdir, strTest):
        objFile = tmpdir.mkdir("test").join("test.txt")
        logging.basicConfig(objFile.name)
        logging.critical(strTest)
        assert strTest in objFile.read()

    @pytest.mark.parameterize(['DEBUG', 'INFO', 'WARNING', 'ERROR',
                               'CRITICAL'], 'strLevel')
    def test_log(self, tmpdir, strTest, strLevel):
        objFile = tmpdir.mkdir("test").join("test.txt")
        logging.basicConfig(objFile.name)
        logging.log(strTest, strLevel)
        assert strTest in objFile.read()

    def test_decLog(self):
        # TODO: write this once decLog is implemented
        assert True
