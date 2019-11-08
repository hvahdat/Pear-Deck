#Instruction prior to running this
# 1. Run Katie's "SETUP STEPS"
# 2. Run Katie's "DATA PULL: User Statuses"
# 3. Run Katie's "DATA PULL: slide_presented & student_responses"
# 4. Run Katie's "DATA PULL: user_facts_anonymized"







#collapse df_us to only have unique teachers (per row), should return 118,466
#df_us$timestamp <- NULL

#do this to merge teachers into one row
library(data.table)
df_us <- dcast(setDT(df_us), teacher ~ rowid(teacher), value.var = c("first_pres_after_trial",
                                                                       "last_pres_1st_few_mnths",
                                                                       "account_status_min",
                                                                       "account_status_max"))
#Remove extra columns since we merge multiple rows into one row we have duplicate data
#df_us$first_pres_after_trial_2 <- NULL
#df_us$last_pres_1st_few_mnths_2 <- NULL
#df_us$account_status_min_2 <- NULL
#df_us$account_status_max_1 <- NULL

df_us <- subset(df_us, select = c("teacher","first_pres_after_trial_1","last_pres_1st_few_mnths_1",
                                  "account_status_min_1","account_status_max_2"))

#change back to original column names
names(df_us)[names(df_us) == "first_pres_after_trial_1"] <- "first_pres_after_trial" 
names(df_us)[names(df_us) == "last_pres_1st_few_mnths_1"] <- "last_pres_1st_few_mnths"
names(df_us)[names(df_us) == "account_status_min_1"] <- "account_status_min"
names(df_us)[names(df_us) == "account_status_max_2"] <- "account_status_max"




dfus_into_dfuf <- merge(x = df_uf, y = df_us, by = "teacher", all.x = TRUE)

entire_dfuf <- merge(x = dfus_into_dfuf, y = df_sr, by = "teacher", all.x = TRUE)



#Only keep columns that will add to katie's jumbo wide data frame
#entire_dfuf <- subset(entire_dfuf, select = c("teacher",
#                                                    "firstseen",
#                                                    "first_pres_after_trial",
#                                                    "last_pres_1st_few_mnths",
#                                                    "account_status_min",
#                                                    "account_status_max"))
#get only unique rows from dfuf_into_dfsr
#entire_dfuf <- unique(entire_dfuf)


### Presentations for year/month

# Create summary: presentations by year/month


entire_dfuf <- group_by(entire_dfuf, teacher, Date_year_month)
summ  <- summarize(entire_dfuf, num_presentations = sum(num_presentations)) 
entire_dfuf <- ungroup(entire_dfuf)



# Create wide df
#121,938 unique teachers in wide
df_sr_wide <- dcast(summ, teacher ~ Date_year_month, value.var="num_presentations")

# Create new col in df_sr_wide: total_presentations & total_months_used
summ <- group_by(summ, teacher)
summ2 <- summarize(summ, num_presentations = sum(num_presentations)) 
summ3 <- summarize(summ, total_months_used = n()) 
summ <- ungroup(summ)









#Now we want all users from df_uf, so first I will merge df_uf & df_sr_wide, with df_uf on left
df_sr_wide <- merge(x= df_uf, y=df_sr_wide, by = "teacher", all.x = TRUE)

#Now we want to add on the columns from dfuf_into_dfsr on our df_sr_wide
df_sr_wide <- merge(x= df_sr_wide, y = dfuf_into_dfsr, by = c("teacher","firstseen"), all.x = TRUE)

### Add to df_sr_wide: # Presentations by user & Total Months by user
df_sr_wide$total_presentations <- summ2$num_presentations
df_sr_wide$total_months_used <- summ3$total_months_used

df_sr_wide$total_presentations[is.na(df_sr_wide$total_presentations)]<- 0
#df_sr_wide$num_students[is.na(df_sr_wide$num_students)] <- 0 



####Continue here#######

### Student Engagement


#Create bucket labels for num_students - hutan
entire_dfuf$num_students_label <- entire_dfuf$num_students
entire_dfuf$num_students_label[which(entire_dfuf$num_students == 1)] <- "0_Testing"
entire_dfuf$num_students_label[which(entire_dfuf$num_students >= 2 & entire_dfuf$num_students <= 5)] <- "1_Low"
entire_dfuf$num_students_label[which(entire_dfuf$num_students >= 6 & entire_dfuf$num_students <= 10)] <- "2_Low-Medium"
entire_dfuf$num_students_label[which(entire_dfuf$num_students >= 11 & entire_dfuf$num_students <= 30)] <- "3_Medium"
entire_dfuf$num_students_label[which(entire_dfuf$num_students >= 31 & entire_dfuf$num_students <= 40)] <- "4_Medium-High"
entire_dfuf$num_students_label[which(entire_dfuf$num_students >= 41)] <- "5_High"

# Rename the levels (note: did it this way to preserve the desired order) - hutan
entire_dfuf$num_students_label <- factor(entire_dfuf$num_students_label)
levels(entire_dfuf$num_students_label) <- paste("Num_stu_", substr(levels(entire_dfuf$num_students_label), 3, nchar(levels(entire_dfuf$num_students_label))), sep="")

# Create summary: Count in each student engagement bucket by user
entire_dfuf <- group_by(entire_dfuf, teacher, num_students_label)
summ  <- summarize(entire_dfuf, num_students_label_ct = n()) 
entire_dfuf <- ungroup(entire_dfuf)

















