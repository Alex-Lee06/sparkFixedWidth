# si_datamonster_sparkApp
Spark Application for ETL POC

This POC is used to pull data from S3 to spark on local machine.  Each object contains
processing for the different sources that they will pull from using a mapping document
provided by the user.

This also handles duplication issues and similar name issues.

# BMI Object Description

BMI object contains methods to process fixed width columns and adding in column names

# Site Impact Object Description

Site impact object contains processing for csv files coming into Site impact

# Import Data Description

Import data contains processing for consumer data.

# Deduplication and Similar names

Deduplication data was created by adding in values that are duplicated and also
changed to have the same name in specific columns then using columns that should
be different to compare them and make the decision to see if they are the same
or not.