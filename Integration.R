#Instruction prior to running this
# 1. Run Katie's "SETUP STEPS"
# 2. Run Katie's "DATA PULL: User Statuses"
# 3. Run Katie's "DATA PULL: slide_presented & student_responses"
# 4. Run Katie's "DATA PULL: user_facts_anonymized"







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

library(tidyr)
df_us <- unite(df_us, "account_status_min", account_status_min_1:account_status_min_2,sep = "",remove = TRUE)
df_us <- unite(df_us, "account_status_max", account_status_max_1:account_status_max_2, sep = "", remove = TRUE)

#Remove extra columns since we merge multiple rows into one row we have duplicate data
#df_us$first_pres_after_trial_2 <- NULL
#df_us$last_pres_1st_few_mnths_2 <- NULL
#df_us$account_status_min_2 <- NULL
#df_us$account_status_max_1 <- NULL

df_us <- subset(df_us, select = c("teacher","first_pres_after_trial_1","last_pres_1st_few_mnths_1",
                                  "account_status_min","account_status_max"))

#change back to original column names
names(df_us)[names(df_us) == "first_pres_after_trial_1"] <- "first_pres_after_trial" 
names(df_us)[names(df_us) == "last_pres_1st_few_mnths_1"] <- "last_pres_1st_few_mnths"





dfus_into_dfuf <- merge(x = df_uf, y = df_us, by = "teacher", all.x = TRUE)

entire_dfuf <- merge(x = dfus_into_dfuf, y = df_sr, by = "teacher", all.x = TRUE)

##### ADD SOMETHING TO CHANGE BLANKS BACK TO NAs - KATIE

# Create summary: presentations by year/month


entire_dfuf <- group_by(entire_dfuf, teacher, Date_year_month)
summ  <- summarize(entire_dfuf, num_presentations = sum(num_presentations)) 
entire_dfuf <- ungroup(entire_dfuf)


# Create wide df
#121,938 unique teachers in wide
df_sr_wide <- dcast(summ, teacher ~ Date_year_month, value.var="num_presentations")
# Remove the NA column (happens b/c some have NAs for all dates) ############### KATIE!! change
df_sr_wide <- df_sr_wide[,1:ncol(df_sr_wide)-1]

# Create new col summing num_students: total_students
entire_dfuf <- group_by(entire_dfuf, teacher)
summ5  <- summarize(entire_dfuf, total_students = sum(num_students)) 
entire_dfuf <- ungroup(entire_dfuf)

# Create new col in df_sr_wide: total_presentations & total_months_used
summ <- group_by(summ, teacher)
summ2 <- summarize(summ, num_presentations = sum(num_presentations))
summ3 <- summarize(summ, total_months_used = sum(!is.na(Date_year_month)) ) ####KATIE CHANGED FROM n()
summ <- ungroup(summ)


#Now we want all users from df_uf, so first I will merge df_uf & df_sr_wide, with df_uf on left
df_sr_wide <- merge(x= df_uf, y=df_sr_wide, by = "teacher", all.x = TRUE)

#May not want to do this since it ruins 121938 counts
#Now we want to add on the columns from dfuf_into_dfsr on our df_sr_wide
#df_sr_wide <- merge(x= df_sr_wide, y = entire_dfuf, by = c("teacher","firstseen"), all.x = TRUE)

### Add to df_sr_wide: # Presentations by user & Total Months by user
df_sr_wide$total_presentations <- summ2$num_presentations
df_sr_wide$total_students <- summ5$total_students
df_sr_wide$total_months_used <- summ3$total_months_used

df_sr_wide$total_presentations[is.na(df_sr_wide$total_presentations)]<- 0
#df_sr_wide$num_students[is.na(df_sr_wide$num_students)] <- 0 



####Continue here#######

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
summ$num_stu_norm <- round(summ$num_students_label_ct/summ$total_presentations,2)
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

#So now we have df_sr_wide for you to analyze at the unique row 
#level (by teacher) for the first two weeks of August people


#Also have entire_dfuf, we cannot combine the two, 
#b/c this is NOT unique at the row level

#We cannot combine the two dataframes because one is unique at the row level and the other is not
#Unless if you want a data loss

#unless if you want the code below, which is NOT unique at row level

#entire_dfuf2 <- merge(x = entire_dfuf, y = df_sr_wide, by = c("teacher","firstseen"), all.x = TRUE) #### KATIE COMMENTED OUT


# Turn blanks into NAs
entire_dfuf$account_status_min <- na_if(entire_dfuf$account_status_min,"")
entire_dfuf$account_status_max <- na_if(entire_dfuf$account_status_max,"")

# Pull over two columns
df_statuses <- entire_dfuf[ , c(which(colnames(entire_dfuf)=="teacher"), which(colnames(entire_dfuf)=="account_status_min"), which(colnames(entire_dfuf)=="account_status_max"))]

# Remove duplicates
df_statuses <- unique(df_statuses)

# Exploring account status
# Examine the unique combinations (delete later)
df_statuses <- group_by(df_statuses, account_status_min, account_status_max)
summ_status <- summarize(df_statuses, total = n())
df_statuses <- ungroup(df_statuses)

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
