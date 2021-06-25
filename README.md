# Solution to Data Engineering Test

This is my particular attempt to solve to the Data Engineering Test created by Raízen Analytics, which can be found at github.com/raizen-analytics/data-engineering-test.

It consists of a two stages approach. First, the underlying data from the two requested tables on the ANP pivot table (on vendas-combustiveis-m3.xls workbook) is extracted using a VBA Macro whose code can be found at github.com/albertotav/raizen-data-engineering-test/blob/main/raw_data/pivot_data_macro.bas. This method was chosen as its simplicity allow direct extraction of the wanted underlying data from the pivot table without having to convert it to other formats (other than enabling macros), using VBA’s Range.ShowDetail property and saving both resulting sheets as .csv files.

The second stage divides into two possible solutions: 

A) A Jupyter notebook (raizen-analytics_data-engineering-test.ipynb), that read the raw csv files from stage one and exports it as refined data with the challenge required schema set that has been written with the intention of delivering easily replicable results using Google’s Colab plataform and includes, as requested, a verify function that check if the refined data sum matches that of the raw data total sum column. This notebook intends to show how each step was developed and how it works.

B) An Apache Airflow DAG. The pipeline reads the raw data from stage one, set the required schema and save into Parquet format as a single dataframe.
