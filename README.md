# [reporting](https://github.com/deschman/reporting)

## Short Description
A package containing templates for reporting with Python.

## Long Description
**reporting** is a Python package providing template classes in an effort to
speed up report building for BI Developers. It aims to provide users with an
API for interacting with various data sources and end-user file types as well
as a simple object for quickly building straight-forward reports.

## Examples

### Windows Authentication of SSMS Database
Before utilizing **reporting** the user should first examine the **config** file.  Below 
*[ODBC]* the user should add their server details.  Additional parameters may be added after 
the initial semicolon  For example:

    myservername = servername;TrustedConnection=yes;

Additionally, be sure to verify the filepaths below the *[REPORT]* header.

The header of your script should import **reporting** and **odbc**, and define your connection.

    import reporting as r
    import pyodbc

    conn = pyodbc.connect(driver='{SQL Server Native Client 11.0}',
                            server='myserver',
                            database='mydatabase',
                            trusted_connection = 'yes')'''
                      
You can create an instance of *SimpleReport* to get started.  You need to provide a string
name for your report.  You can add, remove, and rename queries.  When adding queries, 
you must provide a string name for the query, the SQL query as a string, the name of 
your odbc configuration, and your odbc connection.  When you run the report, the queries
will be run with multithreading.  To use single threading pass *False* into 
the run method.


    rep=r.SimpleReport("test")
    rep.addQuery("testQuery","SELECT * FROM mydatabase.dbo.mytable","myservername",conn)
    rep.addQuery("testQuery2","SELECT * FROM mydatabase.dbo.myothertable","myservername",conn)
    rep.run(False)


**reporting** has robust logging functionality.  Refer to your config to find the filepaths
for the output and logs.  The console will also display this information, as well as the 
filepath to your report in an Excel file.  If you run multiple queries in one report, your
workbook will have a tab for each query. 

## Limitations
User should have solid grasp Python and object oriented programming.
User should be familiar with available data sources and structures.
User must be prepared to interact with data sources using SQL.
User must deliver reports outside the scope of this module.
