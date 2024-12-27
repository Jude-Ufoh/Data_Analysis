# Big_Data_Analysis
## About the Project
The project was one of the assignments at masters level. It involves the combination of clinical trial datasets and with a 
list of pharmaceutical companies in order to bring out some useful insights and answer important questions on the datasets.
## Datasets
### Data 1 : Clinicaltrial_<year>.csv
Clinicaltrial_<year>.csv:
Each row represents an individual clinical trial, identified by an Id, listing the sponsor
(Sponsor), the status of the study at time of the file’s download (Status), the start
and completion dates (Start and Completion respectively), the type of study (Type),
when the trial was first submitted (Submission), and the lists of conditions the trial
concerns (Conditions) and the interventions explored (Interventions). Individual
conditions and interventions are separated by commas. [Source](https://clinicaltrials.gov/data-api/about-api/csv-download)
### Data 2 : Pharma.csv
The file contains a small number of a publicly available list of pharmaceutical
violations. For the purposes of this work, we are interested in the second column,
Parent Company, which contains the name of the pharmaceutical company in
question.
[Source]( https://violationtracker.goodjobsfirst.org/industry/pharmaceuticals)

## Tools
- Databricks Apache Spark (RDD, Dataframe) and Spark SQL
- PowerBI
## Tasks/Problem statement
You are a data analyst / data scientist whose client wishes to gain further insight into
clinical trials. You are tasked with answering these questions, using visualisations where
these would support your conclusions.
You should address the following questions. You should use the solutions for historical
datasets (available on Blackboard) to test your implementation.
1. The number of studies in the dataset. You must ensure that you explicitly check
distinct studies.
2. You should list all the types (as contained in the Type column) of studies in the
dataset along with the frequencies of each type. These should be ordered from
most frequent to least frequent.
3. The top 5 conditions (from Conditions) with their frequencies.
4. Find the 10 most common sponsors that are not pharmaceutical companies, along
with the number of clinical trials they have sponsored. Hint: For a basic
implementation, you can assume that the Parent Company column contains all
possible pharmaceutical companies.
5. Plot number of completed studies each month in a given year – for the submission
dataset, the year is 2021. You need to include your visualization as well as a table
of all the values you have plotted for each month.

## Methodology
1. **Creating a 'year' variable**
 An important requirement of the project was to ensure that the code was reusable such that switching to a different version of the data required only the change of one variable.
Since the datasets for clinicaltrials are arranged according to years, the year was made the variable. The codes were implemented as follows
- **RDD**
  ~~~~
        # creating a year variable and setting  a basefile
        year = "2021"

        basefile = "clinicaltrial_" + year

        dbutils.fs.cp("/FileStore/tables/" + basefile + ".zip", "file:/tmp/")

        dbutils.fs.cp("/FileStore/tables/pharma.zip", "file:/tmp/")
- **Dataframe**
  ~~~~
        import os
        #creating a year variable  and assigning it to base file
        year = "2021"
        basefile= "clinicaltrial_" + year
        os.environ ['basefile'] = basefile
2. **Data Preprocesssing**
   
   Having explored the data,the header rows on both datasets were removed and the datasets were reassigned to a new variable in order to ease combination and manipulation.
   In spark SQL, tables were created in the databricks default database from the already created temporary views for both datasets.
   ~~~~~
        #removing the header row for clinicaltrial data.
        header = clinicaltrial_RDD.first()
        clinicaltrial_RDD_NH =clinicaltrial_RDD.filter(lambda row: row !=header)
        #removing the first row for pharma data
        header2 = pharma_RDD.first()
        pharma_RDD_NH = pharma_RDD.filter(lambda row: row != header2)

   ## PowerBI Visualisation
  From the PowerBI ‘Get data’ tool, the table was imported into Power BI using the connection wizard by selecting databricks in the data source menu and inputing the server hostname and http path copied from the JDBC/ODBC tab under configuration in the compute section of the databricks environment.
  While in the Power BI environment, the table was transformed by changing the Start, Completion and Submission columns to date datatype. 
  
  ![image](https://github.com/user-attachments/assets/96fca2ee-1167-4523-9a3b-cb26a1ff6e58)
  
  After the transformation, the table was loaded into the Power BI interface and an interactive dashboard was created.
  
  ![image](https://github.com/user-attachments/assets/e43e7fda-9ddc-464f-8d64-c13b510a364c)

  View the interactive dashboard [here](https://app.powerbi.com/groups/me/reports/92269f01-0abf-4538-b8e1-7b456f3edf1c/ReportSection?experience=power-bi)

 
  




  

