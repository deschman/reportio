# -*- coding: utf-8 -*-
"""Contains Data object for use in template reports."""


# %% Imports
# %%% Py3 Standard
import os
from tempfile import NamedTemporaryFile
import multiprocessing
from abc import abstractmethod
from typing import List, Any

# %%% 3rd Party
import pandas as pd
import dask.distributed as dd
# Pending tqdm.dask module release
# from tqdm.tqdm.dask import TqdmCallback as ProgressBar

# %%% User Defined
from reportio.errors import DatasetNameError
from reportio.future import ProgressBar


# %% Variables
__all__ = ['Data']


class _Data(object):

    @abstractmethod
    def __init__(self) -> None:
        pass


# %% Classes
class Data(_Data):
    """Data object for use in template reports."""

    # %%% Private
    def __init__(self,
                 dataset_name: str,
                 log: callable,
                 client: dd.Client,
                 temporary_folder_location: str = '',
                 backup_folder_location: str = '',
                 sql: str = '',
                 connection: object = None) -> None:
        self.log: callable = log
        self._client = client
        self._temporary_folder_location: str = temporary_folder_location
        self._backup_folder_location: str = backup_folder_location
        self._dataset_name: str = dataset_name
        self._sql: str = sql
        self._connection: object = connection
        self._DataFrame: object = None
        self._File: object = None
        # self._get_data()
        # self._get_temp_file()

    def _get_data(self) -> None:
        """Retrieve .DataFrame using .sql and .connection."""
        def _get_data_1(
                backup_folder_location: str = self._backup_folder_location,
                dataset_name: str = self._dataset_name,
                log: callable = self.log,
                sql: str = self._sql,
                connection: object = self._connection) -> dd.Future:
            file_location: str = os.path.join(
                backup_folder_location, dataset_name + '.gz')
            if not os.path.isfile(file_location):
                log("Querying database")
                DataFrame = pd.read_sql(sql, connection)
            else:
                log("Reading backup file")
                DataFrame = pd.read_parquet(backup_folder_location)
            return DataFrame

        def _get_data_2() -> None:
            self._DataFrame = self._DataFrame.result()
            self.log("DataFrame retrieved")
            if len(self._DataFrame.index) == 0:
                self.log("Query was empty", 'WARNING')
        if self._DataFrame is not None:
            dd.wait(self._DataFrame)
        self._DataFrame = self._client.submit(_get_data_1)
        self._DataFrame.add_done_callback(_get_data_2)
        dd.fire_and_forget(self._DataFrame)

    def _get_temp_file(self) -> None:
        """
        Create .File and writes ._DataFrame to it.

        File will always use gzip compression and end in .gz. Overwriting may
        cause a critical error and data corruption. Wait for any previous file
        write to complete.
        """

        def _get_temp_file_1(
                temp_folder_location: str = self._temporary_folder_location,
                dataset_name: str = self._dataset_name,
                log: callable = self.log,
                client: dd.Client = self._client,
                DataFrame: object = self._DataFrame) -> None:
            try:
                file: object = NamedTemporaryFile(
                    dir=temp_folder_location,
                    suffix="__" + dataset_name + '.gz')
            except OSError as err:
                log(err, 'DEBUG')
                raise DatasetNameError(dataset_name)
            File: object = client.submit(
                DataFrame.to_parquet, file, compression='gzip')
            return File

        def _get_temp_file_2() -> None:
            self._File = self._File.result()
            self.log("File saved")
        if self._File is not None:
            dd.wait(self._File)
        self._File = self._client.submit(_get_temp_file_1)
        self._File.add_done_callback(_get_temp_file_2)
        dd.fire_and_forget(self._File)

    # %%% Public
    # %%%% Properties
    @property
    def dataset_name(self) -> str:
        """
        Name of .File.

        Changing this attribute will result in the creation of a new file.
        """
        return self._dataset_name

    @dataset_name.setter
    def _set_dataset_name(self, dataset_name) -> None:
        if os.path.isfile(self._File):
            self._File.close()
        self._dataset_name = dataset_name
        self._get_temp_file()

    @property
    def sql(self) -> str:
        """
        Query used to get data contained in .DataFrame and .File.

        Changing this property will result in the query being re-run and the
        creation of a new file.
        """
        return self._sql

    @sql.setter
    def _set_sql(self, sql) -> None:
        self._sql = sql
        self._get_data()
        self._get_temp_file()

    @property
    def connection(self) -> object:
        """
        Object used to connect to database.

        Changing will close the current connection immediately.
        """
        return self._connection

    @connection.setter
    def _set_connection(self, connection) -> None:
        if self._connection is not None:
            self._connection.close()
        self._connection = connection

    @property
    def DataFrame(self) -> pd.DataFrame:
        """Object holding data from query in RAM."""
        return self._DataFrame

    @DataFrame.setter
    def _set_DataFrame(self, DataFrame: pd.DataFrame) -> None:
        self._DataFrame = DataFrame
        self._get_temp_file()

    @property
    def File(self) -> object:
        """
        Object holding data from query on disk.

        Changing will delete the current file immediately.
        """
        return self._File

    @File.setter
    def _set_File(self, File: object) -> None:
        if self._File is not None:
            self._File.close()
        self._File = File

    # %%%% Processes
    def _cross_query(self,
                     dataframe_input: pd.DataFrame,
                     column_list: List[str],
                     sql_function: callable,
                     zero_value: Any = pd.NA,
                     final_columns: List[str] = None,
                     compress_columns: List[str] = None) -> object:
        """
        For experimental use.

        Append column_list to dataframe_input from data from another query.
        Wil dynamically build queries based on values in each row of
        dataframe_input and execute while utilizing dask for multithread
        scheduling.

        Parameters
        ----------
        dataframe_input : pandas.DataFrame
            DataFrame to be appended with data from queries.
        column_list : list
            Name of new columns to be added.
        sql_function : function
            Should have input for row of data from DataFrame.iterrows() and
            return query as string.
        zero_value : user-defined type, default is pandas.NA
            Value to be used if query returns no results.
        final_columns : list, optional
            Columns listed in result file.
        compress_columns : list, optional
            Column names to use as a temporary grouping. One query will be
            run for each unique record in this column list. This is untested.

        Returns
        -------
        dataframe_ouput : DataFrame
            dataframe_input with data populated in column_list from
            sql_function.
        """
        def process_chunk(dataframe_input: pd.DataFrame,
                          chunk_list: List[tuple],
                          column_list: List[Any] = column_list,
                          compress_columns: List[Any] = compress_columns
                          ) -> pd.DataFrame:
            # Initialize chunk dataframe
            chunk: pd.DataFrame = pd.DataFrame(columns=column_list)
            # Loop chunk list
            for index, row in chunk_list:
                # Check for partial population
                if not any([pd.isna(dataframe_input.at[index, c])
                            for c in column_list]):
                    chunk.append(
                        pd.DataFrame([list(dataframe_input.loc[index])],
                                     index=[index],
                                     columns=chunk.columns))
                else:
                    query_data: pd.DataFrame = pd.read_sql(sql_function(row),
                                                           self._connection)
                    if len(query_data.index) > 0:
                        for i in query_data.index:
                            chunk = chunk.append(
                                pd.DataFrame([list(query_data.loc[i])],
                                             index=[index],
                                             columns=chunk.columns))
                    else:
                        chunk.append(
                            pd.DataFrame(
                                [[zero_value] * len(column_list)],
                                index=[index],
                                columns=chunk.columns))
            if compress_columns is not None:
                # Initialize output DataFrame
                ungrouped_data: pd.DataFrame = pd.DataFrame(
                    columns=chunk.columns)
                # Duplicate current index
                chunk.reset_index()
                # Loop over input
                for _, r in chunk.iterrows():
                    # Iterate over grouped data
                    for i in range(len(r['index'])):
                        ungrouped_data = ungrouped_data.append(
                            pd.DataFrame([r[column_list]],
                                         index=[r['index'][i]],
                                         columns=column_list))
                        for c in compress_columns:
                            ungrouped_data.at[r['index'][i], c] = r[c][i]
                chunk = ungrouped_data
            return chunk
        # Add new columns if needed
        if any([c not in l for l in dataframe_input.columns for c in
                column_list]):
            for c in column_list:
                if c not in dataframe_input.columns:
                    dataframe_input[c] = pd.NA
        # Temporarily group data if needed (this may not work)
        if compress_columns is not None:
            dataframe_input = dataframe_input.groupby(
                compress_columns, as_index=False).agg(list)
        # Initialize chunk list, dataframe list
        chunk_list: List[tuple] = []
        dataframe_list: List[pd.DataFrame] = []
        # Loop dataframe
        for index_row in dataframe_input.iterrows():
            chunk_list.append(index_row)
            # Process in chunks equal to number of threads available
            if len(chunk_list) >= len(
                    dataframe_input.index) / multiprocessing.cpu_count():
                dataframe_list.append(
                    self._client.submit(
                        process_chunk, dataframe_input, chunk_list))
                chunk_list = []
        with ProgressBar():
            dataframe_list = self._client.gather(dataframe_list)
        # Initialize output dataframe
        dataframe_ouput = dataframe_input.copy()
        for df in dataframe_list:
            # Combine master df and chunk df, keeping chunk where not null
            dataframe_ouput.combine(df, lambda s1, s2: s1.combine(
                s2, lambda v1, v2: v2 if not pd.isna(v2) else v1),
                overwrite=False)
        if final_columns is not None:
            dataframe_ouput = dataframe_ouput.loc[:, final_columns]
        return dataframe_ouput

    def _merge_files(self,
                     file_name: str,
                     file_1: object,
                     file_2: object,
                     merge_column: str,
                     join_type: str = 'inner',
                     columns_1: List[str] = None,
                     columns_2: List[str] = None,
                     columns_final: List[str] = None) -> object:
        """
        Merge two parquet files into one.

        Parameters
        ----------
        file_name : string
            Output file name.
        file_1 : file
            Contains data from pandas.
        file_2 : file
            Contains data from pandas.
        merge_column : string
            Column used for merge.
        join_type : {'left', 'right', 'outer', 'inner'}, default 'inner'
            Merge type.
        columns_1 : list, default all columns
            Columns merged from first file.
        columns_2 : list, default all columns
            Columns merged from second file.
        columns_final : list, default all columns
            Columns listed in result file.

        Returns
        -------
        data : object, merged file
        """
        # Check for backup file
        backup_file_location: str = os.path.join(self._backup_folder_location,
                                                 file_name + '.gz')
        if os.path.isfile(backup_file_location):
            self.log("Reading backup file")
            temporary_dataframe: pd.DataFrame = pd.read_parquet(
                backup_file_location)
            if file_name != '':
                data: object = self._get_temp_file(file_name,
                                                   temporary_dataframe)
        else:
            self.log("Merging files")
            dataframe_1: pd.DataFrame = pd.read_parquet(file_1,
                                                        columns=columns_1)
            dataframe_2: pd.DataFrame = pd.read_parquet(file_2,
                                                        columns=columns_2)
            if join_type == 'right':
                suffix_right: str = ''
                suffix_left: str = "_drop"
            else:
                suffix_right: str = '_drop'
                suffix_left: str = ''
            dataframe_1 = dataframe_1.merge(
                dataframe_2, join_type, merge_column, suffixes=(
                    suffix_left, suffix_right))
            del dataframe_2
            dataframe_1 = dataframe_1.drop(
                [c for c in dataframe_1 if '_drop' in c], axis=1)
            if columns_final is not None:
                dataframe_1 = dataframe_1[columns_final]
            dataframe_1 = dataframe_1.drop_duplicates(ignore_index=True)
            data: object = self._get_temp_file(file_name, dataframe_1)
        return data

    # TODO: make this an intuitive way for user to combine data objects without
    # thinking about what state each data object is in
    def join(self,
             result_columns: List[str],
             data2: _Data,
             join_type: str,
             join_columns: List[str],
             group_columns: List[str] = []) -> _Data:
        """Edit this."""
        pass
