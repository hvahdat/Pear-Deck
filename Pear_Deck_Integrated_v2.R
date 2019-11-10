#####################
# SETUP STEPS
#####################

# Clear environment
rm(list=ls()) 

# Capture start time
code_start_time <- Sys.time()

# Install packages (if needed)
#install.packages("zoo")

# Load libraries
library(bigrquery)
library(reshape2)
library(ggplot2)
library(tidyverse)
library(tidyr)
library(scales)
library(zoo)

# Set working directory
setwd("C:\\Users\\katie\\Dropbox\\Grad School - Business Analytics Masters Program\\2019 Fall Classes\\Pear Deck - Analytics Experience\\R Scripts")

# Data pull setup
project_id <- "peardeck-external-projects"

# Set upper limit date to included usage data 
upper_limit_date <- "2019-10-31"

#####################
# SETUP NOTES
#####################

# Website on how to connect to Google BigQuery in R: https://www.blendo.co/blog/access-data-google-bigquery-python-r/
# https://www.rdocumentation.org/packages/bigrquery/versions/1.2.0/topics/bq_query

# https://www.datacamp.com/community/tutorials/long-wide-data-R

# install.packages("bigrquery")
# install.packages("DescTools")


#####################
# DATA PULL: Teachers who participated in a presentation before ever presenting
#####################

# Theory is that they have better retention.

sql_string <- "WITH
Teachers1 AS (
SELECT distinct teacher, min (timestamp) firstTimeTeacher
FROM `peardeck-external-projects.buisness_analytics_in_practice_project.student_responded`
GROUP BY teacher
),

Teachers2 AS (
SELECT distinct externalId
FROM `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized`
WHERE profile_role != 'student'
),

Teachers AS (
SELECT distinct teacher, firstTimeTeacher
FROM Teachers1 JOIN Teachers2 ON teacher = externalId
),

Students AS (
SELECT distinct student, min (timestamp) firstTimeStudent
FROM `peardeck-external-projects.buisness_analytics_in_practice_project.student_responded`
GROUP BY student
),

StudentTeachers AS (
SELECT teacher, firstTimeTeacher, firstTimeStudent
FROM Teachers JOIN Students ON teacher = student
)

SELECT CAST(teacher as STRING) as teacher, firstTimeStudent, firstTimeTeacher
FROM StudentTeachers
WHERE firstTimeStudent < firstTimeTeacher"

# Savae into dataframe
df_ppb4 <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf)

# Add column: 
df_ppb4$participated_b4_used <- as.numeric(1)

# Reduce to only unique columns of interest
df_ppb4 <- df_ppb4[,c(1,4)]

#####################
# DATA PULL: Teachers who presented 5+ presentations in a year
#####################

sql_string <- "SELECT teacher, 
CASE 
WHEN SUM(num_presentations) >= 5 THEN 1
ELSE 0
END as prez_5_plus
FROM
(
  SELECT 
  CAST(sp.externalid AS STRING) AS teacher,
  presentation_id,
  DATE(sp.timestamp) AS Date,
  EXTRACT(HOUR FROM sp.timestamp) AS hour,
  COUNT(DISTINCT sp.presentation_id) as num_presentations,
  COUNT(DISTINCT sr.student) as num_students
  
  FROM 
  
  (SELECT externalid, presentation_id, slide_id, timestamp, DATE(timestamp) as date, EXTRACT(HOUR FROM timestamp) as hour
  FROM `peardeck-external-projects.buisness_analytics_in_practice_project.slide_presented` sp_i RIGHT JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf_i 
  ON sp_i.user_id = uf_i.externalid
  WHERE 
  ((DATE(timestamp) BETWEEN DATE(FirstSeen) AND DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR)) 
  --OR (DATE(timestamp) BETWEEN DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR) AND DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH), INTERVAL 1 YEAR) 
  --OR timestamp IS NULL)
  )
  AND (DATE(FirstSeen) BETWEEN '2018-08-05' AND '2018-08-18') 
  AND profile_role <> 'student'
  ) sp 
  
  LEFT JOIN 
  
  (SELECT externalid, teacher, student, presentation, timestamp, slide, DATE(timestamp) as date, EXTRACT(HOUR FROM timestamp) as hour
  FROM `peardeck-external-projects.buisness_analytics_in_practice_project.student_responses` sr_i RIGHT JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf_i 
  ON sr_i.teacher = uf_i.externalid
  WHERE app not like '%flash%' 
  AND ((DATE(timestamp) BETWEEN DATE(FirstSeen) AND DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR)) 
  --OR (DATE(timestamp) BETWEEN DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR) AND DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH), INTERVAL 1 YEAR) 
  --OR timestamp IS NULL)
  )
  AND (DATE(FirstSeen) BETWEEN '2018-08-05' AND '2018-08-18')
  AND profile_role <> 'student'
  ) sr 
  
  ON sp.externalid = sr.externalid AND sp.slide_id = sr.slide AND sp.presentation_id = sr.presentation AND sp.date = sr.date AND sp.hour = sr.hour
  
  GROUP BY sp.externalid, presentation_id, DATE(sp.timestamp), EXTRACT(HOUR FROM sp.timestamp)
  --Note: count does not exactly equal what's in user facts if user hasn't launched a presentation ever.
) z
  GROUP BY teacher"
  
df_5_plus <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf)


##############################################################################
# Looping starts here
##############################################################################
 
for (k in 1:2) {
  
  if (k == 1) {

    #####################
    # DATA PULL: User Statuses
    #####################
    
    # 10 seconds
    sql_string <- "SELECT CAST(externalid as STRING) as teacher, first_pres_after_trial, last_pres_1st_few_mnths, timestamp, 
    CASE WHEN first_pres_after_trial = timestamp THEN b.account_status END AS account_status_min,
    CASE WHEN last_pres_1st_few_mnths = timestamp THEN b.account_status END AS account_status_max
    
    FROM `peardeck-external-projects.buisness_analytics_in_practice_project.gslides_addon_launch_presentation` b
    RIGHT JOIN
    
    (
    SELECT externalid, MIN(timestamp) as first_pres_after_trial, MAX(timestamp) as last_pres_1st_few_mnths
    FROM `peardeck-external-projects.buisness_analytics_in_practice_project.gslides_addon_launch_presentation` lp_i RIGHT JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf ON lp_i.user_id = uf.externalid
    WHERE ((DATE(timestamp) BETWEEN DATE_ADD(DATE(FirstSeen), INTERVAL 1 MONTH) AND DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH)) OR timestamp IS NULL) 
    AND (DATE(FirstSeen) BETWEEN '2018-08-05' AND '2018-08-18')
    AND profile_role <> 'student'
    GROUP BY externalid
    ) a
    
    ON b.user_id = a.externalid AND (b.timestamp = a.first_pres_after_trial OR b.timestamp = a.last_pres_1st_few_mnths)
    --Note: count does not exactly equal what's in user facts if user hasn't launched a presentation ever."
    
    query_start_time <- Sys.time()
    df_us <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf) #, allowLargeResults = TRUE
    query_end_time <- Sys.time()
    query_runtime <- query_end_time - query_start_time
    print(round(query_runtime,1))
    
    
    #####################
    # DATA PULL: slide_presented & student_responses
    #####################
    
    # NEWEST VERSION
    sql_string <- "SELECT 
    CAST(sp.externalid AS STRING) AS teacher,
    presentation_id,
    DATE(sp.timestamp) AS Date,
    EXTRACT(HOUR FROM sp.timestamp) AS hour,
    COUNT(DISTINCT sp.presentation_id) as num_presentations,
    COUNT(DISTINCT sr.student) as num_students
    
    FROM 
    
    (SELECT externalid, presentation_id, slide_id, timestamp, DATE(timestamp) as date, EXTRACT(HOUR FROM timestamp) as hour
    FROM `peardeck-external-projects.buisness_analytics_in_practice_project.slide_presented` sp_i RIGHT JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf_i 
    ON sp_i.user_id = uf_i.externalid
    WHERE 
    ((DATE(timestamp) BETWEEN DATE(FirstSeen) AND DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH)) 
    --OR (DATE(timestamp) BETWEEN DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR) AND DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH), INTERVAL 1 YEAR) 
    --OR timestamp IS NULL)
    )
    AND (DATE(FirstSeen) BETWEEN '2018-08-05' AND '2018-08-18') 
    AND profile_role <> 'student'
    ) sp 
    
    LEFT JOIN 
    
    (SELECT externalid, teacher, student, presentation, timestamp, slide, DATE(timestamp) as date, EXTRACT(HOUR FROM timestamp) as hour
    FROM `peardeck-external-projects.buisness_analytics_in_practice_project.student_responses` sr_i RIGHT JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf_i 
    ON sr_i.teacher = uf_i.externalid
    WHERE app not like '%flash%' 
    AND ((DATE(timestamp) BETWEEN DATE(FirstSeen) AND DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH)) 
    --OR (DATE(timestamp) BETWEEN DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR) AND DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH), INTERVAL 1 YEAR) 
    --OR timestamp IS NULL)
    )
    AND (DATE(FirstSeen) BETWEEN '2018-08-05' AND '2018-08-18')
    AND profile_role <> 'student'
    ) sr 
    
    ON sp.externalid = sr.externalid AND sp.slide_id = sr.slide AND sp.presentation_id = sr.presentation AND sp.date = sr.date AND sp.hour = sr.hour
    
    GROUP BY sp.externalid, presentation_id, DATE(sp.timestamp), EXTRACT(HOUR FROM sp.timestamp)
    --Note: count does not exactly equal what's in user facts if user hasn't launched a presentation ever.
    "
    
    # Note: structured the query this way because slide_id can be null
    
    # Pull data
    # Approximate run time: 7.3 min
    query_start_time <- Sys.time()
    df_sr <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf) #, allowLargeResults = TRUE
    query_end_time <- Sys.time()
    query_runtime <- query_end_time - query_start_time
    print(round(query_runtime,1))
    
    # Add field of date in YYYY-MM format
    df_sr$Date_year_month <- format(df_sr$Date, format="%Y-%m")
    
    # Remove column "presentation_id" (need it for cases when two presentations are presented within the same hour)
    df_sr <- df_sr[,-which(colnames(df_sr)=="presentation_id")]

  }
  
  if (k == 2) {
    
    #####################
    # DATA PULL: User Statuses NEXT YEAR
    #####################
    
    # 10 seconds
    sql_string <- "SELECT CAST(externalid as STRING) as teacher, first_pres_after_trial, last_pres_1st_few_mnths, timestamp, 
    CASE WHEN first_pres_after_trial = timestamp THEN b.account_status END AS account_status_min,
    CASE WHEN last_pres_1st_few_mnths = timestamp THEN b.account_status END AS account_status_max
    
    FROM `peardeck-external-projects.buisness_analytics_in_practice_project.gslides_addon_launch_presentation` b
    RIGHT JOIN
    
    (
    SELECT externalid, MIN(timestamp) as first_pres_after_trial, MAX(timestamp) as last_pres_1st_few_mnths
    FROM `peardeck-external-projects.buisness_analytics_in_practice_project.gslides_addon_launch_presentation` lp_i RIGHT JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf ON lp_i.user_id = uf.externalid
    WHERE ((DATE(timestamp) BETWEEN DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 1 MONTH), INTERVAL 1 YEAR) AND DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH), INTERVAL 1 YEAR) OR timestamp IS NULL)) 
    AND (DATE(FirstSeen) BETWEEN '2018-08-05' AND '2018-08-18')
    AND profile_role <> 'student'
    GROUP BY externalid
    ) a
    
    ON b.user_id = a.externalid AND (b.timestamp = a.first_pres_after_trial OR b.timestamp = a.last_pres_1st_few_mnths)
    --Note: count does not exactly equal what's in user facts if user hasn't launched a presentation ever."
    
    query_start_time <- Sys.time()
    df_us <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf) #, allowLargeResults = TRUE
    query_end_time <- Sys.time()
    query_runtime <- query_end_time - query_start_time
    print(round(query_runtime,1))
    
    #####################
    # DATA PULL: slide_presented & student_responses NEXT YEAR
    #####################
    
    # NEWEST VERSION
    sql_string <- "SELECT 
    CAST(sp.externalid AS STRING) AS teacher,
    presentation_id,
    DATE(sp.timestamp) AS Date,
    EXTRACT(HOUR FROM sp.timestamp) AS hour,
    COUNT(DISTINCT sp.presentation_id) as num_presentations,
    COUNT(DISTINCT sr.student) as num_students
    
    FROM 
    
    (SELECT externalid, presentation_id, slide_id, timestamp, DATE(timestamp) as date, EXTRACT(HOUR FROM timestamp) as hour
    FROM `peardeck-external-projects.buisness_analytics_in_practice_project.slide_presented` sp_i RIGHT JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf_i 
    ON sp_i.user_id = uf_i.externalid
    WHERE 
    ((DATE(timestamp) BETWEEN DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR) AND DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH), INTERVAL 1 YEAR)) 
    --OR (DATE(timestamp) BETWEEN DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR) AND DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH), INTERVAL 1 YEAR) 
    --OR timestamp IS NULL)
    )
    AND (DATE(FirstSeen) BETWEEN '2018-08-05' AND '2018-08-18') 
    AND profile_role <> 'student'
    ) sp 
    
    LEFT JOIN 
    
    (SELECT externalid, teacher, student, presentation, timestamp, slide, DATE(timestamp) as date, EXTRACT(HOUR FROM timestamp) as hour
    FROM `peardeck-external-projects.buisness_analytics_in_practice_project.student_responses` sr_i RIGHT JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf_i 
    ON sr_i.teacher = uf_i.externalid
    WHERE app not like '%flash%' 
    AND ((DATE(timestamp) BETWEEN DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR) AND DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH), INTERVAL 1 YEAR)) 
    --OR (DATE(timestamp) BETWEEN DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR) AND DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH), INTERVAL 1 YEAR) 
    --OR timestamp IS NULL)
    )
    AND (DATE(FirstSeen) BETWEEN '2018-08-05' AND '2018-08-18')
    AND profile_role <> 'student'
    ) sr 
    
    ON sp.externalid = sr.externalid AND sp.slide_id = sr.slide AND sp.presentation_id = sr.presentation AND sp.date = sr.date AND sp.hour = sr.hour
    
    GROUP BY sp.externalid, presentation_id, DATE(sp.timestamp), EXTRACT(HOUR FROM sp.timestamp)
    --Note: count does not exactly equal what's in user facts if user hasn't launched a presentation ever.
    "
    
    # Pull data
    # Approximate run time: 7.3 min
    query_start_time <- Sys.time()
    df_sr <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf) #, allowLargeResults = TRUE
    query_end_time <- Sys.time()
    query_runtime <- query_end_time - query_start_time
    print(round(query_runtime,1))
    
    # Add field of date in YYYY-MM format
    df_sr$Date_year_month <- format(df_sr$Date, format="%Y-%m")
    
  }

#####################
# DATA PULL: slide_presented
#####################

sql_string <- "SELECT user_id, slide_type, SUM(slide_type_ct) AS slide_type_ct 
FROM (

SELECT CAST(user_id AS STRING) AS user_id, presentation_id, slide_id, slide_type, DATE(timestamp) as Date, EXTRACT(HOUR FROM timestamp) as Hour, COUNT(DISTINCT user_id) AS slide_type_ct 
FROM `peardeck-external-projects.buisness_analytics_in_practice_project.slide_presented` sp JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf ON sp.user_id = uf.externalid
WHERE ((DATE(timestamp) BETWEEN DATE(FirstSeen) AND DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH))
AND (DATE(FirstSeen) BETWEEN '2018-08-05' AND '2018-08-18') AND profile_role <> 'student')
GROUP BY user_id, presentation_id, slide_id, slide_type, DATE(timestamp), EXTRACT(HOUR FROM timestamp)
) 

GROUP BY user_id, slide_type
"

# Pull data
# Run time: 3 min
query_start_time <- Sys.time()
df_sp <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf)
query_end_time <- Sys.time()
query_runtime <- query_end_time - query_start_time
print(round(query_runtime,1))

names(df_sp)[names(df_sp) == "user_id"] <- "teacher"

#####################
# DATA PULL: user_facts_anonymized
#####################

# NEW VERSION
sql_string <- "SELECT CAST(externalid AS STRING) as teacher, firstseen
FROM `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` 
WHERE (DATE(FirstSeen) BETWEEN '2018-08-05' AND '2018-08-18')
AND profile_role <> 'student'"

# sql_string <- "SELECT DISTINCT CAST(externalid AS STRING) AS externalid, profile_role, firstseen, CAST(domain AS STRING) as domain, firstpremiumtime, premiumstatusatsignup, accountType, expirationdate, trial_end, coupon_medium FROM `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.student_responses` ON `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized`.externalid = `peardeck-external-projects.buisness_analytics_in_practice_project.student_responses`.teacher WHERE DATE(firstseen) < '2019-10-01' AND lower(app) not like '%flash%' AND DATE(timestamp) >= '2017-01-01' AND DATE(timestamp) <= '2019-10-31'"

# Pull data
# Run time: 1 min
query_start_time <- Sys.time()
df_uf <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf)
query_end_time <- Sys.time()
query_runtime <- query_end_time - query_start_time
print(round(query_runtime,1))

#####################
# CREATE DFs: df_sp_wide
#####################

# Create summary: total slides per teacher
df_sp <- group_by(df_sp, teacher)
summ <- summarize(df_sp, total_slides = sum(slide_type_ct))
df_sp <- ungroup(df_sp)

# Merge total_slides into df_sp
df_sp <- merge(summ, df_sp, by = "teacher")

# Calculate normalized slide_type
df_sp$slide_type_amt_norm <- df_sp$slide_type_ct/df_sp$total_slides

# Create df_sp_wide
df_sp_wide <- dcast(df_sp, teacher + total_slides ~  slide_type, value.var = "slide_type_amt_norm")

##############################################################################
# Begin integrated script
##############################################################################

#collapse df_us to only have unique teachers (per row), should return 118,466
df_us$timestamp <- NULL

#do this to merge teachers into one row
library(data.table)
df_us <- dcast(setDT(df_us), teacher ~ rowid(teacher), value.var = c("first_pres_after_trial",
                                                                     "last_pres_1st_few_mnths",
                                                                     "account_status_min",
                                                                     "account_status_max"))

#Here we need to merge 2 columns into one, since min and max often fill in either column 1 or 2
df_us$account_status_min_1[is.na(df_us$account_status_min_1)] <- ""
df_us$account_status_min_2[is.na(df_us$account_status_min_2)] <- ""
df_us$account_status_max_1[is.na(df_us$account_status_max_1)] <- ""
df_us$account_status_max_2[is.na(df_us$account_status_max_2)] <- ""

df_us <- unite(df_us, "account_status_min", account_status_min_1:account_status_min_2,sep = "",remove = TRUE)
df_us <- unite(df_us, "account_status_max", account_status_max_1:account_status_max_2, sep = "", remove = TRUE)

df_us <- subset(df_us, select = c("teacher","first_pres_after_trial_1","last_pres_1st_few_mnths_1",
                                  "account_status_min","account_status_max"))

#change back to original column names
names(df_us)[names(df_us) == "first_pres_after_trial_1"] <- "first_pres_after_trial" 
names(df_us)[names(df_us) == "last_pres_1st_few_mnths_1"] <- "last_pres_1st_few_mnths"


dfus_into_dfuf <- merge(x = df_uf, y = df_us, by = "teacher", all.x = TRUE)

entire_dfuf <- merge(x = dfus_into_dfuf, y = df_sr, by = "teacher", all.x = TRUE)

# Create summary: presentations by year/month
entire_dfuf <- group_by(entire_dfuf, teacher, Date_year_month)
summ  <- summarize(entire_dfuf, num_presentations = sum(num_presentations)) 
entire_dfuf <- ungroup(entire_dfuf)

# Create wide df
df_sr_wide <- dcast(summ, teacher ~ Date_year_month, value.var="num_presentations")
# Remove the NA column (happens b/c some have NAs for all dates) 
df_sr_wide <- df_sr_wide[,-which(colnames(df_sr_wide)=="NA")]

# Create new col summing num_students: total_students
entire_dfuf <- group_by(entire_dfuf, teacher)
summ5  <- summarize(entire_dfuf, total_students = sum(num_students)) 
entire_dfuf <- ungroup(entire_dfuf)

# Create new col in df_sr_wide: total_presentations & total_months_used
summ <- group_by(summ, teacher)
summ2 <- summarize(summ, num_presentations = sum(num_presentations))
summ3 <- summarize(summ, total_months_used = sum(!is.na(Date_year_month)) )
summ <- ungroup(summ)

#Now we want all users from df_uf, so first I will merge df_uf & df_sr_wide, with df_uf on left
df_sr_wide <- merge(x= df_uf, y=df_sr_wide, by = "teacher", all.x = TRUE)


#Now we want to add on the columns from dfuf_into_dfsr on our df_sr_wide
#df_sr_wide <- merge(x= df_sr_wide, y = entire_dfuf, by = c("teacher","firstseen"), all.x = TRUE)

### Add to df_sr_wide: # Presentations by user & Total Months by user
df_sr_wide$total_presentations <- summ2$num_presentations
df_sr_wide$total_students <- summ5$total_students
df_sr_wide$total_months_used <- summ3$total_months_used

df_sr_wide$total_presentations[is.na(df_sr_wide$total_presentations)]<- 0
df_sr$num_students[is.na(df_sr$num_students)] <- 0 


### Student Engagement

# Create bucket labels for num_students
df_sr$num_students_label <- df_sr$num_students
df_sr$num_students_label[which(df_sr$num_students <= 1)] <- "0_Testing"
df_sr$num_students_label[which(df_sr$num_students >= 2 & df_sr$num_students <= 5)] <- "1_Low"
df_sr$num_students_label[which(df_sr$num_students >= 6 & df_sr$num_students <= 10)] <- "2_Low-Medium"
df_sr$num_students_label[which(df_sr$num_students >= 11 & df_sr$num_students <= 30)] <- "3_Medium"
df_sr$num_students_label[which(df_sr$num_students >= 31 & df_sr$num_students <= 40)] <- "4_Medium-High"
df_sr$num_students_label[which(df_sr$num_students >= 41)] <- "5_High"

# Rename the levels (note: did it this way to preserve the desired order)
df_sr$num_students_label <- factor(df_sr$num_students_label)
levels(df_sr$num_students_label) <- paste("Num_stu_", substr(levels(df_sr$num_students_label), 3, nchar(levels(df_sr$num_students_label))), sep="")

# Create summary: Count in each student engagement bucket by user
df_sr <- group_by(df_sr, teacher, num_students_label)
summ  <- summarize(df_sr, num_students_label_ct = n()) 
df_sr <- ungroup(df_sr)
# Merge in total_presentations
summ <- merge(summ, df_sr_wide[,c(which(colnames(df_sr_wide)=="teacher"),which(colnames(df_sr_wide)=="total_presentations"))])
# Calculate normalized num_students_label
summ$num_stu_norm <- summ$num_students_label_ct/summ$total_presentations
# Make it wide (merge prep)
summ2 <- dcast(summ, teacher ~ num_students_label, value.var="num_stu_norm")
summ2[is.na(summ2)] <- 0 
# Add to df_sr_wide
df_sr_wide <- merge(df_sr_wide, summ2, all.x = TRUE)

# Add Date_year_month_2 to df_sr
df_sr$Date_year_month_2 <- as.Date(paste(df_sr$Date_year_month, "-01", sep=""))

# Create summary: review first and last use dates
df_sr <- group_by(df_sr, teacher)
summ  <- summarize(df_sr, YM_first_use = min(Date_year_month_2), YM_last_use = max(Date_year_month_2)) 
df_sr <- ungroup(df_sr)
summ$months_bw_1st_last_use <- ((as.yearmon(strptime(summ$YM_last_use, format = "%Y-%m-%d")) - as.yearmon(strptime(summ$YM_first_use, format = "%Y-%m-%d")))*12)+1

# Add to df_sr_wide
df_sr_wide <- merge(df_sr_wide, summ, all.x = TRUE)

# Add months_since_1st_use to df_sr_wide
df_sr_wide$months_since_1st_use <- ((as.yearmon(strptime(upper_limit_date, format = "%Y-%m-%d")) - as.yearmon(strptime(df_sr_wide$YM_first_use, format = "%Y-%m-%d")))*12)+1

# Add normalized time usage to df_sr_wide
df_sr_wide$time_usage_perc <- round(df_sr_wide$total_months_used/df_sr_wide$months_since_1st_use,2)
#df_sr_wide$time_usage_perc2 <- round(df_sr_wide$total_months_used/df_sr_wide$months_bw_1st_last_use,2)

# Add normalized presentation usage to df_sr_wide
df_sr_wide$pres_usage_norm1 <- round(df_sr_wide$total_presentations/df_sr_wide$total_months_used,2)
df_sr_wide$pres_usage_norm2 <- round(df_sr_wide$total_presentations/df_sr_wide$months_bw_1st_last_use,2)
df_sr_wide$pres_usage_norm3 <- round(df_sr_wide$total_presentations/df_sr_wide$months_since_1st_use,2)

# Remove unnecessary dfs
rm(list = c("summ","summ2","summ3"))

# Turn blanks into NAs
entire_dfuf$account_status_min <- na_if(entire_dfuf$account_status_min,"")
entire_dfuf$account_status_max <- na_if(entire_dfuf$account_status_max,"")

# Pull over two columns
df_statuses <- entire_dfuf[ , c(which(colnames(entire_dfuf)=="teacher"), which(colnames(entire_dfuf)=="account_status_min"), which(colnames(entire_dfuf)=="account_status_max"))]

# Remove duplicates
df_statuses <- unique(df_statuses)

# # Exploring account status
# # Examine the unique combinations (delete later)
# df_statuses <- group_by(df_statuses, account_status_min, account_status_max)
# summ_status <- summarize(df_statuses, total = n())
# df_statuses <- ungroup(df_statuses)

# Merge df_statuses into df_sr_wide
df_sr_wide <- merge(x = df_sr_wide, y = df_statuses, by = "teacher", all.x = TRUE)

# entire_dfuf <- group_by(entire_dfuf, teacher)
# summ5 <- summarize(entire_dfuf, )
# entire_dfuf <- ungroup(entire_dfuf)

# Create empty column
df_sr_wide$status_label <- as.character("")

# Never used
df_sr_wide$status_label[which( df_sr_wide$total_presentations == 0)] <- "Never Used"
# Tested Project Only
df_sr_wide$status_label[which( df_sr_wide$total_presentations > 0 & df_sr_wide$total_students <= 1)] <- "Tested Project Only"
# Presented to 2+ went free
df_sr_wide$status_label[which( df_sr_wide$total_presentations > 0 & df_sr_wide$total_students > 1 & df_sr_wide$account_status_max == "free")] <- "Used Product Free"
# Presented to 2+ went premium
df_sr_wide$status_label[which( df_sr_wide$total_presentations > 0 & df_sr_wide$total_students > 1 & df_sr_wide$account_status_max == "premium")] <- "Used Product Premium"
# Presented to 2+ went premiumtrial
df_sr_wide$status_label[which( df_sr_wide$total_presentations > 0 & df_sr_wide$total_students > 1 & df_sr_wide$account_status_max == "premiumTrial")] <- "Used Product PremiumTrial"
# Presented to 2+ but status unknown (i.e. they used the product in the 1st month but not the subsequent months in the time span)
df_sr_wide$status_label[which( df_sr_wide$total_presentations > 0 & df_sr_wide$total_students > 1 & is.na(df_sr_wide$account_status_max) == TRUE)] <- "Used Product Unknown Status"

#####################
# ADD: slide diversity to df_sr_wide
#####################

df_sp_wide$slideDiversity <- as.numeric(0)

for (i in 1:nrow(df_sp_wide)) {
  
  sum = 0
  
  for (j in 1:(ncol(df_sp_wide)-3)) {
    
    if(!is.na(df_sp_wide[i, 2+j])) {
      sum = sum + (df_sp_wide[i, 2+j]*(log(df_sp_wide[i, 2+j], (ncol(df_sp_wide)-3))))
    }
    
  }
  
  sum = sum * -1
  
  df_sp_wide$slideDiversity[i] <- round(sum, 3)
  
}

# Create a subset of just the data to merge into df_sr_wide
df_sp_wide_subset <- df_sp_wide[,c(1,ncol(df_sp_wide))]

# Add slide diversity into df_sp_wide
df_sr_wide <- merge(x = df_sr_wide, y = df_sp_wide_subset, by = "teacher", all.x = TRUE)


#####################
# DATA PULL: add in 2019 account status & account type
#####################

sql_string <- "WITH
Cohort AS (
SELECT externalId user_id, currentlyPremium, currentlyOnTrial, accountType
FROM `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized`
WHERE (DATE(FirstSeen) BETWEEN '2018-08-05' AND '2018-08-18')
AND profile_role <> 'student'
)

SELECT CAST(user_id AS STRING) AS teacher, 
CASE 
WHEN currentlyPremium = false AND currentlyOnTrial = false THEN 'free'
WHEN currentlyPremium = true AND currentlyOnTrial = false THEN 'premium'
WHEN currentlyPremium = true AND currentlyOnTrial = true THEN 'premiumtrial' END AS accountStatus_1yr_later, accountType as accountType_1yr_later
FROM Cohort"


# Pull data
# Run time: 3.6 sec
query_start_time <- Sys.time()
df_as <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf)
query_end_time <- Sys.time()
query_runtime <- query_end_time - query_start_time
print(round(query_runtime,1))

# Change change "individual - synthetic" to "individual"
df_as$accountType_1yr_later[which(df_as$accountType_1yr_later == "individual - synthetic")] <- "individual"

#Merge 2019 account status & account type to df_sr_wide
df_sr_wide <- merge(x = df_sr_wide, y = df_as, by = "teacher", all.x = TRUE)



#Running Chi Square and One way ANOVA tests to test for significant diffences

#Chi square test requires a table as an input, this makes sense b/c
#Chi square compares categorical variable to categorical variable
#One way Anova compares one categorical variable against a continuous variable

##############################################################################
# Integrate initial months with 1 year later
##############################################################################
  
  if (k == 1) {
    df_sr_wide_INITIAL <- df_sr_wide
    df_sr_INITIAL <- df_sr
    df_us_INITIAL <- df_us
  }
  
  if (k == 2) {
    df_sr_wide_INITIAL$status_label_yr_later <- df_sr_wide$status_label
    df_wide <- df_sr_wide_INITIAL[,-c(which(colnames(df_sr_wide_INITIAL)=="accountStatus_1yr_later"), 
                                      which(colnames(df_sr_wide_INITIAL)=="account_status_min"), 
                                      which(colnames(df_sr_wide_INITIAL)=="account_status_max"))]
    names(df_wide)[names(df_wide) == "status_label"] <- "status_label_initial_months"
    
  }


}

# total_students: turn NAs to zeros
df_wide$total_students[which(is.na(df_wide$total_students))] <- 0
#df_wide$slideDiversity[which(is.na(df_wide$slideDiversity))] <- 0 # Might not want to do this -- 0 means low slide diversity, but NA is actually the absence of values. 

# Create new columns: num_prez_audience & num_prez_testing
df_wide$num_prez_audience <- df_wide$total_presentations-(df_wide$total_presentations*df_wide$Num_stu_Testing)
df_wide$num_prez_testing <- df_wide$total_presentations*df_wide$Num_stu_Testing

# Visualize number of presentations for an audience
p <- qplot(df_wide$num_prez_audience, geom="histogram", binwidth = 5) +
  scale_y_continuous(name = "Count", labels = scales::comma) +  # unit_format(unit = "K") +
  scale_x_continuous(name="Number of Presentations Given to an Audience") +
  theme(panel.grid.minor.y = element_blank()) + 
  theme(panel.background = element_blank()) +
  ggtitle("Presentations Given to Audience") + 
  theme(text = element_text(size = 14)) 

ggsave(filename = "Presentations Given to Audience.png", plot = p, width = 15, height = 7, units = "in")

# Visualize number of presentations for an audience (zoomed in)
p <- qplot(df_wide$num_prez_audience, geom="histogram", binwidth = 5) +
  scale_y_continuous(name = "Count", labels = scales::comma) +  # unit_format(unit = "K") +
  scale_x_continuous(name="Number of Presentations Given to an Audience") +
  coord_cartesian(xlim = c(0, 30)) +
  theme(panel.grid.minor.y = element_blank()) + 
  theme(panel.background = element_blank()) +
  ggtitle("Presentations Given to Audience (Zoomed In)") + 
  theme(text = element_text(size = 14)) 

ggsave(filename = "Presentations Given to Audience (Zoomed In).png", plot = p, width = 15, height = 7, units = "in")

# Turn blanks from num_prez_audience & num_prez_testing to zeros
df_wide$num_prez_audience[which(is.na(df_wide$num_prez_audience))] <- 0
df_wide$num_prez_testing[which(is.na(df_wide$num_prez_testing))] <- 0

### Create column: total_prez_label
# Initialize column
df_wide$total_prez_label <- as.character(NA)
# Add labels
df_wide$total_prez_label[which(df_wide$total_presentations <= 4)] <- "1_Light"
df_wide$total_prez_label[which(df_wide$total_presentations >=5 & df_wide$total_presentations <= 9)] <- "2_Medium"
df_wide$total_prez_label[which(df_wide$total_presentations >=10 & df_wide$total_presentations <= 19)] <- "3_Heavy"
df_wide$total_prez_label[which(df_wide$total_presentations >= 20)] <- "4_Superuser"
# Rename the levels (note: did it this way to preserve the desired order)
df_wide$total_prez_label <- factor(df_wide$total_prez_label)
levels(df_wide$total_prez_label) <- substr(levels(df_wide$total_prez_label), 3, nchar(levels(df_wide$total_prez_label)))

# Merge in participated in presentation before presented
df_wide <- merge(x = df_wide, y = df_ppb4, by = "teacher", all.x = TRUE)
# participated_b4_used: turn NAs to zeros
df_wide$participated_b4_used[which(is.na(df_wide$participated_b4_used))] <- 0

# Merge in prez_5_plus
df_wide <- merge(x = df_wide, y = df_5_plus, by = "teacher", all.x = TRUE)
# participated_b4_used: turn NAs to zeros
df_wide$prez_5_plus[which(is.na(df_wide$prez_5_plus))] <- 0
