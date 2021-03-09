# [reportio](https://github.com/deschman/reportio)

## Short Description
A package containing templates for reporting with Python.

## Long Description
**reportio** is a Python package providing template classes in an effort to
speed up report building for BI Developers. It aims to provide users with an
API for interacting with various data sources and end-user file types as well
as a simple object for quickly building straight-forward reports.

## Examples
### SimpleReport
    from reportio import SimpleReport


    # Initialize report object
    objReport = SimpleReport("Yearly Sales")

    # Add queries to report object
    objReport.addQuery("Category", "SELECT * FROM CATEGORY", 'sqlite')
    objReport.addQuery("Subcategory", "SELECT * FROM SUB_CATEGORY", 'sqlite')
    objReport.addQuery("Segment", "SELECT * FROM SEGMENT", 'sqlite')

    # Process and export
    objReport.run()

### ReportTemplate
    import os
    from typing import Dict, Any

    from reportio import ReportTemplate


    # Create report object at runtime
    class Report(ReportTemplate):
        """Test report."""

        def __init__(self,
                     report_name: str = 'test',
                     log_location: str = os.path.join(
                         os.path.dirname(__file__), 'simple_log.txt'),
                     config_location: str = os.path.join(
                         os.path.dirname(__file__), 'simple_config.txt'),
                     connection_dictionary: Dict[str, object] = {},
                     client: Any = None,
                     optional_function: callable = None) -> None:
            super().__init__(report_name,
                             log_location,
                             config_location,
                             connection_dictionary,
                             client,
                             optional_function)

        # 'run' method must be instantiated
        def run(self):
            """Run test report."""
            self.file = self.get_data('test_data',
                                      "SELECT * FROM CATEGORY",
                                      'sqlite')[0]
            self.export_data(self.file, self.config['REPORT']['export_to'])


    # Script report object to run
    if __name__ == '__main__':
        report = Report()
        try:
            report.run()
        except Exception:
            report.log("See log for debug details", 'CRITICAL')
            report.backup_data()
            report.log("Backup successful")
            input("PRESS ENTER KEY TO QUIT")
            report.log("QUITTING")

### Windows Authentication of SSMS Database
Before utilizing **reportio** the user should first examine the **config**
file. Below *[DB]* the user should add their database details if they plan to
use a saved connection. Additional parameters may be added after the initial
semicolon. For example:

    myservername = DSN=servername;TrustedConnection=yes;

Additionally, be sure to verify the filepaths below the *[REPORT]* header.

Alternatively to using saved connections, the user may create their own
connection at runtime. In this case, the header of your script should import
**reportio** and your preferred connection module, and define your connection.

    import reportio as r
    import pyodbc

    conn = pyodbc.connect(driver='{SQL Server Native Client 11.0}',
                          server='myserver',
                          database='mydatabase',
                          trusted_connection = 'yes')

You can create an instance of *SimpleReport* to get started. You need to
provide a string name for your report. You can add, remove, and rename queries.
When adding queries, you must provide a string name for the query, the SQL
as a string, and the name of your saved connection or your created connection.
When you run the report, the queries will be run with multithreading. To use
single threading pass *False* into the run method.

    rep=r.SimpleReport("test")
    rep.addQuery("testQuery",
                 "SELECT * FROM mydatabase.dbo.mytable",
                 connection=conn)
    rep.addQuery("testQuery2",
                 "SELECT * FROM mydatabase.dbo.myothertable",
                 connection=conn)
    rep.run(False)

## Logging
**reportio** has robust logging functionality. Refer to your config to find the
filepaths for the output and logs. The console will also display this
information, as well as the filepath to your report in an Excel file. If you
run multiple queries in one report, your workbook will have a tab for each
query.

## Limitations
User should have solid grasp Python and object oriented programming.
User should be familiar with available data sources and structures.
User must be prepared to interact with data sources using SQL.
User must deliver reports outside the scope of this module.
