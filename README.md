# Delta_Lake_Pipeline_GTS

This project create a delta lake pipeline within SCD type II for Yelp dataset. This pipeline joins all sub sources into one unified table and drop the useless column and doing data cleansing.
It answers the following data analysis questions:
a. How many reviews are there for each business?
b. How many businesses take place in each state, In each city? What kind of business do they have the most in each state, in each city ?
c. What time do people usually write reviews?

### split_large_json.py
This file contains a function 'split_json' that split large json file into smaller size, hence allow json to be uploaded to DBFS.

### Delta_Lake_Pipeline.py
This file contains the code for Delta lake pipeline. It reads all json data into a single databricks notebook, merged all splited file back, created a unified dataframe which aid data analysis questions, as well as answers to data analysis questions.

### Delta_Lake_Pipeline.html
This file has the same contents as Delta_Lake_Pipeline.py, while in a html version, which shows more result part of each block from a visual perspective.
