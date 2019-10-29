#website I referenced in case you want to check it out
#https://www.blendo.co/blog/access-data-google-bigquery-python-r/

#Clear environment
rm(list=ls()) 

#install.packages("bigrquery")

#How to pull stuff down from Big Query to R, seems like there are certain limitations, try out your own queries
library(bigrquery)

project_id <- "peardeck-external-projects"

sql_string <- "SELECT * FROM `peardeck-external-projects.buisness_analytics_in_practice_project.app_events` LIMIT 500000"

query_results <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf)
