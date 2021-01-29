# [reporting](https://github.com/deschman/reporting)

## Short Description
A package containing templates for reporting with Python.

## Long Description
**reporting** is a Python package providing template classes in an effort to
speed up report building for BI Developers. It aims to provide users with an
API for interacting with various data sources and end-user file types as well
as a simple object for quickly building straight-forward reports.

## Examples
# SimpleReport
'''
from reporting import SimpleReport


\# Initialize report object
objReport = SimpleReport("Yearly Sales")

\# Add queries to report object
objReport.addQuery("Category", "SELECT * FROM CATEGORY", 'sqlite')
objReport.addQuery("Subcategory", "SELECT * FROM SUB_CATEGORY", 'sqlite')
objReport.addQuery("Segment", "SELECT * FROM SEGMENT", 'sqlite')

\# Process and export
objReport.run()
'''

# ReportTemplate
'''

'''


## Limitations
User should have solid grasp Python and object oriented programming.
User should be familiar with available data sources and structures.
User must be prepared to interact with data sources using SQL.
User must deliver reports outside the scope of this module.
