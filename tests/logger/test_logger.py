# -*- coding: utf-8 -*-


# %% Imports
# %%% 3rd Party
import pytest

# %%% User-Defined
from reportio import logger


# %% Classes
class test_logger:

    # %%% Variables
    strTest = 'test'

    # %%% Functions
    def test_basicConfig(self, tmpdir):
        def test_existing(tmpdir):
            assert logger.basicConfig(tmpdir.mkdir("test").join("test.txt")
                                      ) == 'existing'

        def test_new():
            assert logger.basicConfig(tmpdir.mkdir("test").join("test.txt"),
                                      'w') == 'new'

    def test_getThreadScope(self):
        assert logger.getThreadScope() in [True, False]

    def test_debug(self, tmpdir, strTest):
        objFile = tmpdir.mkdir("test").join("test.txt")
        logger.basicConfig(objFile.name)
        logger.debug(strTest)
        assert strTest in objFile.read()

    def test_info(self, tmpdir, strTest):
        objFile = tmpdir.mkdir("test").join("test.txt")
        logger.basicConfig(objFile.name)
        logger.info(strTest)
        assert strTest in objFile.read()

    def test_warning(self, tmpdir, strTest):
        objFile = tmpdir.mkdir("test").join("test.txt")
        logger.basicConfig(objFile.name)
        logger.warning(strTest)
        assert strTest in objFile.read()

    def test_error(self, tmpdir, strTest):
        objFile = tmpdir.mkdir("test").join("test.txt")
        logger.basicConfig(objFile.name)
        logger.error(strTest)
        assert strTest in objFile.read()

    def test_critical(self, tmpdir, strTest):
        objFile = tmpdir.mkdir("test").join("test.txt")
        logger.basicConfig(objFile.name)
        logger.critical(strTest)
        assert strTest in objFile.read()

    @pytest.mark.parameterize(['DEBUG', 'INFO', 'WARNING', 'ERROR',
                               'CRITICAL'], 'strLevel')
    def test_log(self, tmpdir, strTest, strLevel):
        objFile = tmpdir.mkdir("test").join("test.txt")
        logger.basicConfig(objFile.name)
        logger.log(strTest, strLevel)
        assert strTest in objFile.read()

    def test_decLog(self):
        # TODO: write this once decLog is implemented
        assert True
