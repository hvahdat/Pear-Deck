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
library(data.table)

# Set working directory
setwd("C:\\Users\\katie\\Dropbox\\Grad School - Business Analytics Masters Program\\2019 Fall Classes\\Pear Deck - Analytics Experience\\R Scripts")

# Data pull setup
project_id <- "peardeck-external-projects"

# Set upper limit date to included usage data 
upper_limit_date <- Sys.Date()

# Set FirstSeen date range (to be used in the query pulls below)
firstSeen <- "BETWEEN '2018-08-05' AND '2018-08-18'"

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
SELECT distinct f0_ as teacher, min (timestamp) firstTimeTeacher
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

# Save into dataframe
df_ppb4 <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf)

# Add column: 
df_ppb4$participated_b4_used <- as.numeric(1)

# Reduce to only unique columns of interest
df_ppb4 <- df_ppb4[,c(1,4)]

#####################
# DATA PULL: Teachers who presented 5+ presentations in a year
#####################

# sql_string <- paste("SELECT teacher, 
# CASE 
# WHEN SUM(num_presentations) >= 5 THEN 1
# ELSE 0
# END as prez_5_plus
# FROM
# (
#   SELECT 
#   CAST(sp.externalid AS STRING) AS teacher,
#   presentation_id,
#   DATE(sp.timestamp) AS Date,
#   EXTRACT(HOUR FROM sp.timestamp) AS hour,
#   COUNT(DISTINCT sp.presentation_id) as num_presentations,
#   COUNT(DISTINCT sr.student) as num_students
#   
#   FROM 
#   
#   (SELECT externalid, presentation_id, slide_id, timestamp, DATE(timestamp) as date, EXTRACT(HOUR FROM timestamp) as hour
#   FROM `peardeck-external-projects.buisness_analytics_in_practice_project.slide_presented` sp_i RIGHT JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf_i 
#   ON sp_i.user_id = uf_i.externalid
#   WHERE 
#   ((DATE(timestamp) BETWEEN DATE(FirstSeen) AND DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR)) 
#   --OR (DATE(timestamp) BETWEEN DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR) AND DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH), INTERVAL 1 YEAR) 
#   --OR timestamp IS NULL)
#   )
#   AND (DATE(FirstSeen)", firstSeen, ") 
#   AND profile_role <> 'student'
#   ) sp 
#   
#   LEFT JOIN 
#   
#   (SELECT externalid, teacher, student, presentation, timestamp, slide, DATE(timestamp) as date, EXTRACT(HOUR FROM timestamp) as hour
#   FROM `peardeck-external-projects.buisness_analytics_in_practice_project.student_responses` sr_i RIGHT JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf_i 
#   ON sr_i.teacher = uf_i.externalid
#   WHERE app not like '%flash%' 
#   AND ((DATE(timestamp) BETWEEN DATE(FirstSeen) AND DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR)) 
#   --OR (DATE(timestamp) BETWEEN DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR) AND DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH), INTERVAL 1 YEAR) 
#   --OR timestamp IS NULL)
#   )
#   AND (DATE(FirstSeen) ", firstSeen, ")
#   AND profile_role <> 'student'
#   ) sr 
#   
#   ON sp.externalid = sr.externalid AND sp.slide_id = sr.slide AND sp.presentation_id = sr.presentation AND sp.date = sr.date AND sp.hour = sr.hour
#   
#   GROUP BY sp.externalid, presentation_id, DATE(sp.timestamp), EXTRACT(HOUR FROM sp.timestamp)
#   --Note: count does not exactly equal what's in user facts if user hasn't launched a presentation ever.
# ) z
#   GROUP BY teacher"
# )
# 
# df_5_plus <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf)

#####################
# DATA PULL: app_events
#####################

sql_string = paste("SELECT CAST(user_id as STRING) as teacher,  SUM(CASE WHEN Event IN 
('successfully_invited_10_students',
'gslides_addon_show_how_it_works',
'gslides_addon_library_template_added',
'gslides_addon_toggle_custom_question_block',
'presentation_launcher_join_code_closed_timing',
'file_opener_pd_converter_user_seen',
'vocab_editor_button_clicked',
'finished_upgrade_flow',
'orchard_publisher_copied_deck_snapshot_file',
'successfully_invited_10_students_gslides_addon',
'file_opener_opened_file',
'v2_enabled_classroom_add_on',
'presentation_launcher_all_slides_loaded',
'coupon_used',
'valet_coupon_with_utm',
'orchard_publisher_file_selection_user_seen',
'did_add_easy_pear_gslides_install',
'didnt_add_easy_pear_gslides_install',
'successfully_invited_100_students_gslides_addon',
'opened_app_home',
'opened_app_editor',
'gslides_addon_response_added',
'ppt_addin_shared_presentation_attempt',
'orchard_publisher_error_state',
'successfully_invited_350_students_pd_classic',
'successfully_invited_250_students',
'successfully_invited_150_students_gslides_addon',
'successfully_invited_380_students',
'successfully_invited_450_students',
'successfully_invited_200_students_gslides_addon',
'successfully_invited_290_students_pd_classic',
'successfully_invited_430_students',
'joined_demo_gafe_summit',
'classroom_add_on_enabled',
'classic_editor_new_file_created',
'freeze_session_freeze_thumbnails_process_thumbnail_uploaded_gslide_thumbnail_to_s3',
'published_takeaways',
'gslides_presentation_created',
'gslides_addon_toggle_library_block',
'gslides_addon_show_present_modal',
'gslides_addon_returned_from_chrome_store_with_extension_installed',
'freeze_session_freeze_thumbnails_process_thumbnail_uploaded_s3thumbnail_to_fb',
'successfully_invited_20_students_gslides_addon',
'freeze_session_freeze_thumbnails_process_thumbnail_got_gslide_thumbnail',
'successfully_invited_60_students_pd_classic',
'bia_installer_next_steps_action',
'account_started_trial',
'gslides_addon_caught_error',
'successfully_invited_80_students',
'freeze_session_freeze_annotations_upload_annotations_to_session_done',
'orchard_publisher_preview_owner_opened_edit',
'file_opener_pd_converter_contact_support_requested',
'successfully_invited_210_students_gslides_addon',
'slide_uploaded',
'started_presentation',
'freeze_session_freeze_thumbnails_finished_thumbnail_batch_of_size1',
'successfully_invited_140_students_gslides_addon',
'successfully_invited_180_students_gslides_addon',
'successfully_invited_360_students',
'successfully_invited_280_students_pd_classic',
'successfully_invited_440_students',
'successfully_invited_460_students_pd_classic',
'used_quota_import',
'added_slide',
'gslides_addon_slide_added',
'successfully_invited_20_students',
'presented',
'orchard_publisher_preview_user_seen',
'deck_snapshot_preview',
'used_existing_vocab_list',
'gslides_addon_open_pricing_page',
'freeze_session_freeze_thumbnails_got_gslides_presentation',
'enabled_takeaway_add_on',
'clever_portal_login',
'logged_in_with_microsoft',
'gslides_addon_open_upgrade_modal',
'presentation_launcher_pdf_downloaded',
'gslides_addon_open_install_extension_modal',
'successfully_invited_80_students_pd_classic',
'blocked_student',
'presenter_successfully_presented_to_three_students',
'presentation_launcher_first_slide_loaded',
'gslides_addon_opened_bia_installed_deck',
'microsoft_login_error',
'disabled_classroom_climate_add_on',
'vocab_editor_user_dragged_list_items',
'ms_home_create_presentation_succeeded',
'freeze_session_freeze_annotations_recieved_drive_file_pdf_stream',
'freeze_session_freeze_thumbnails_complete',
'successfully_invited_100_students',
'did_add_easy_pear_gslides_menu',
'clicked_easy_pear_gslides_install',
'successfully_invited_140_students',
'successfully_invited_220_students',
'file_imported',
'opened_app_presenter',
'loaded_sessions_fully',
'successfully_invited_190_students',
'successfully_invited_190_students_gslides_addon',
'successfully_invited_400_students',
'successfully_invited_290_students',
'successfully_invited_370_students_pd_classic',
'simultaneous_use_of_both_the_projector_and_session_dashboard_views',
'session_navigated_to_slide_missing_image',
'successfully_invited_20_students_pd_classic',
'account_extended_trial',
'freeze_session_found_non_gslides_presentation',
'archived_session_list_action',
'successfully_invited_90_students_pd_classic',
'file_opener_user_has_popups_blocked',
'import_slides_failed',
'successfully_invited_60_students',
'opened_gslides_editor_with_powerup',
'freeze_session_freeze_annotations_started',
'freeze_session_freeze_annotations_pdf_download_done',
'disabled_takeaway_add_on',
'freeze_session_got_gslides_presentation',
'successfully_invited_160_students',
'orchard_publisher_preview_user_signed_in',
'file_opener_pd_converter_fatal_error',
'successfully_invited_50_students_gslides_addon',
'bia_installer_installation_error',
'successfully_invited_170_students_gslides_addon',
'orchard_publisher_preview_error',
'successfully_invited_230_students_pd_classic',
'vocab_editor_historical_docid_url_param_used',
'successfully_invited_120_students_gslides_addon',
'opened_projector_view',
'prepare_slides_from_file',
'has_used_new_presenter_2018',
'successfully_invited_270_students',
'ppt_addin_opened_pricing_page_from_promo_footer',
'successfully_invited_220_students_pd_classic',
'successfully_invited_410_students_pd_classic',
'successfully_invited_320_students',
'successfully_invited_310_students_pd_classic',
'successfully_invited_390_students',
'successfully_invited_400_students_pd_classic',
'removed_slide',
'successfully_invited_2_students_pd_classic',
'remove_deck_from_recent_activities',
'account_1_prompt_for_role',
'opened_drive_picker',
'import_pdf',
'session_list_action',
'orchard_publisher_accepted_terms_and_published',
'vocab_editor_pasted_list_received',
'vocab_editor_card_button_clicked',
'successfully_invited_130_students_pd_classic',
'invited_section_via_classroom',
'successfully_invited_50_students',
'freeze_session_freeze_thumbnails_process_thumbnail_complete',
'successfully_invited_40_students_pd_classic',
'ppt_addin_sidebar_opened',
'successfully_invited_110_students',
'freeze_session_freeze_thumbnails_finished_thumbnail_batch',
'freeze_session_freeze_annotations_complete',
'nudge_to_gslides_import_modal',
'successfully_invited_150_students_pd_classic',
'didnt_add_easy_pear_gslides_menu_2',
'updated_student_login_service',
'successfully_invited_90_students_gslides_addon',
'successfully_invited_200_students_pd_classic',
'successfully_invited_180_students',
'successfully_invited_200_students',
'successfully_invited_340_students',
'successfully_invited_160_students_gslides_addon',
'successfully_invited_310_students',
'successfully_invited_330_students_pd_classic',
'successfully_invited_240_students_gslides_addon',
'successfully_invited_440_students_pd_classic',
'copy_deck',
'duplicated_slide',
'successfully_invited_30_students_pd_classic',
'directed_to_install_gslides_addon',
'file_opener_pd_converter_open_converted_file_requested',
'session_list_menu_action',
'valet_page_seen',
'shown_team_drive_is_premium_modal',
'gslides_addon_extension_modal_chrome_store_opened',
'presenter_starred_response',
'file_opener_pd_converter_conversion_completed',
'successfully_invited_2_students_gslides_addon',
'file_opener_convert_pd_file_requested',
'ppt_addin_launch_presentation_requested',
'ppt_addin_opened_add_assessment_dialog',
'ms_home_create_presentation_requested',
'freeze_session_freeze_thumbnails_finished_thumbnail_batch_of_size4',
'orchard_publisher_edit_owner_seen',
'freeze_session_complete',
'peardeck_signed_up',
'successfully_invited_210_students_pd_classic',
'successfully_invited_180_students_pd_classic',
'successfully_invited_410_students',
'successfully_invited_430_students_pd_classic',
'successfully_invited_330_students',
'successfully_invited_300_students',
'successfully_invited_270_students_gslides_addon',
'gslides_addon_open_library',
'interrupter_was_annoying',
'gslides_addon_launch_presentation',
'file_opener_pd_converter_start_presenting_requested_from_landing_page',
'successfully_invited_10_students_pd_classic',
'orchard_publisher_owner_preview_file_requested',
'orchard_publisher_owner_deck_categories_updated',
'opened_featured_news_feed_item',
'vocab_editor_opened_file_by_id',
'user_received_pearror',
'opened_recent_file',
'valet_flow_completed',
'vocab_editor_new_file_created',
'vocab_editor_imported_list_received',
'used_vocab_word_suggestion',
'file_opener_pd_converter_go_to_peardeck_editor_requested',
'successfully_invited_90_students',
'flashcard_factory_ended_session_with_premade_content',
'successfully_invited_130_students',
'gslides_addon_open_extension_modal_from_banner_requested',
'v2_disabled_classroom_add_on',
'freeze_session_started',
'freeze_session_freeze_thumbnails_got_firebase_user_slides',
'launched_non_peardeck_presentation',
'freeze_session_freeze_annotations_pdf_parsed_annotations_done',
'freeze_session_freeze_annotations_loaded_pdf',
'successfully_invited_140_students_pd_classic',
'successfully_invited_120_students',
'orchard_publisher_file_selection_file_chosen',
'successfully_invited_160_students_pd_classic',
'session_created_quick_question_slide_missing_assets',
'successfully_invited_230_students_gslides_addon',
'gslides_presentation_permission_denied',
'freeze_session_freeze_thumbnails_finished_thumbnail_batch_of_size3',
'orchard_publisher_report_file_email_opened',
'successfully_invited_240_students_pd_classic',
'successfully_invited_260_students_pd_classic',
'successfully_invited_240_students',
'successfully_invited_190_students_pd_classic',
'successfully_invited_350_students',
'successfully_invited_360_students_pd_classic',
'successfully_invited_370_students',
'successfully_invited_450_students_pd_classic',
'successfully_invited_470_students',
'successfully_invited_280_students_gslides_addon',
'used_quota_skip_the_editor',
'gslides_addon_installed',
'gslides_addon_switch_to_main',
'session_navigated_to_slide_missing_annotations',
'gslides_addon_open_account_creation_modal',
'orchard_publisher_terms_required_owner_seen',
'successfully_invited_30_students',
'enabled_lock_timer',
'nudge_to_gslides_editor_banner',
'freeze_session_freeze_thumbnails_process_thumbnail_uploaded_gslide_thumbnail_to_fb',
'enabled_classroom_climate_add_on',
'orchard_publisher_terms_accepted',
'bia_installer_started_installation',
'bia_installer_completed_installation',
'teams_user_seen',
'clicked_hubspot_promotion_banner',
'clicked_easy_pear_gslides_menu',
'did_add_easy_pear_gslides_menu_2',
'successfully_invited_70_students_gslides_addon',
'successfully_invited_60_students_gslides_addon',
'microsoft_new_user_log_in',
'student_domain_lock_enforced',
'slide_presented',
'gslides_addon_sidebar_opened',
'gslides_addon_open_add_assessment_modal',
'successfully_invited_110_students_gslides_addon',
'successfully_invited_250_students_pd_classic',
'successfully_invited_230_students',
'successfully_invited_420_students_pd_classic',
'freeze_session_freeze_thumbnails_finished_thumbnail_batch_of_size2',
'successfully_invited_210_students',
'successfully_invited_170_students',
'successfully_invited_260_students_gslides_addon',
'successfully_invited_340_students_pd_classic',
'successfully_invited_380_students_pd_classic',
'successfully_invited_420_students',
'slides_imported',
'interrupter_was_helpful',
'flashcard_factory_opened_presenter',
'valet_prompted_for_school',
'nudge_to_gslides_ppt_modal',
'ppt_addin_added_question',
'vocab_session_completed',
'successfully_invited_50_students_pd_classic',
'successfully_invited_40_students',
'successfully_invited_100_students_pd_classic',
'successfully_invited_30_students_gslides_addon',
'unblocked_student',
'presentation_launcher_microsoft_session_uploaded',
'successfully_invited_110_students_pd_classic',
'teams_presentation_launched',
'freeze_session_found_presentation_link',
'file_opener_pd_converter_try_again_requested',
'presentation_with_enforced_whitelist_created',
'orchard_publisher_preview_opened_resource',
'successfully_invited_170_students_pd_classic',
'freeze_session_enqueued',
'orchard_publisher_social_media_share_clicked',
'microsoft_returning_user_log_in',
'successfully_invited_150_students',
'couldnt_add_easy_pear_gslides_menu_no_addon',
'successfully_invited_260_students',
'successfully_invited_320_students_pd_classic',
'successfully_invited_220_students_gslides_addon',
'successfully_invited_280_students',
'ppt_addin_clicked_upgrade_to_premium',
'successfully_invited_470_students_pd_classic',
'create_presentation',
'opened_app_vocab_editor',
'successfully_invited_2_students',
'presented_with_extension',
'orchard_publisher_owner_closed_ccss_modal',
'flashcard_factory_ended_session',
'gslides_addon_toggle_featured_library_block',
'successfully_invited_40_students_gslides_addon',
'orchard_publisher_owner_copied_preview_link',
'orchard_publisher_preview_copied_preview_link',
'successfully_invited_70_students',
'successfully_invited_70_students_pd_classic',
'freeze_session_freeze_thumbnails_finished_thumbnail_batch_of_size5',
'couldnt_add_easy_pear_gslides_install_insert_failure',
'orchard_publisher_owner_changed_anonymity',
'bia_installer_preview_resource',
'vocab_editor_file_copied_on_load',
'freeze_session_freeze_thumbnails_started',
'flashcard_factory_export_to_quizlet_clicked',
'didnt_add_easy_pear_gslides_menu',
'successfully_invited_120_students_pd_classic',
'orchard_publisher_owner_error',
'teams_projector_link_opened',
'successfully_invited_80_students_gslides_addon',
'gslides_addon_hide_extension_banner_requested',
'logged_in_with_google',
'successfully_invited_130_students_gslides_addon',
'successfully_invited_270_students_pd_classic',
'unsupported_one_drive_file_type_selected',
'successfully_invited_300_students_pd_classic',
'successfully_invited_250_students_gslides_addon',
'successfully_invited_390_students_pd_classic',
'successfully_invited_460_students'
)
THEN 1 ELSE 0 END)   AS teacher_event_count,
SUM(CASE WHEN Event IN 
('opened_app_student',
'joined_presentation',
'student_recorded_feedback',
'microsoft_login_error',
'student_recorded_mood',
'vocab_student_responded',
'student_joined_pp2s_deck',
'presentation_launcher_first_student_joined_timing',
'account_1_prompt_for_role',
'student_joined',
'presenter_asked_quick_question',
'peardeck_signed_up',
'vocab_student_joined',
'microsoft_new_user_log_in',
'teams_presentation_student_joined')
THEN 1 ELSE 0 END)   AS student_event_count,

SUM(CASE WHEN Event IN ('gslides_addon_show_how_it_works',
'gslides_addon_library_template_added',
'presentation_launcher_join_code_closed_timing',
'file_opener_pd_converter_user_seen',
'vocab_editor_button_clicked',
'finished_upgrade_flow',
'orchard_publisher_copied_deck_snapshot_file',
'file_opener_opened_file',
'presentation_launcher_all_slides_loaded',
'coupon_used',
'valet_coupon_with_utm',
'orchard_publisher_file_selection_user_seen',
'did_add_easy_pear_gslides_install',
'didnt_add_easy_pear_gslides_install',
'opened_app_student',
'opened_app_home',
'opened_app_editor',
'gslides_addon_response_added',
'ppt_addin_shared_presentation_attempt',
'orchard_publisher_error_state',
'joined_demo_gafe_summit',
'classic_editor_new_file_created',
'gslides_presentation_created',
'gslides_addon_returned_from_chrome_store_with_extension_installed',
'bia_installer_next_steps_action',
'account_started_trial',
'gslides_addon_caught_error',
'orchard_publisher_preview_owner_opened_edit',
'file_opener_pd_converter_contact_support_requested',
'slide_uploaded',
'started_presentation',
'used_quota_import',
'added_slide',
'gslides_addon_slide_added',
'orchard_publisher_preview_user_seen',
'deck_snapshot_preview',
'used_existing_vocab_list',
'gslides_addon_open_pricing_page',
'clever_portal_login',
'logged_in_with_microsoft',
'gslides_addon_open_upgrade_modal',
'presentation_launcher_pdf_downloaded',
'gslides_addon_open_install_extension_modal',
'gslides_addon_opened_bia_installed_deck',
'microsoft_login_error',
'disabled_classroom_climate_add_on',
'vocab_editor_user_dragged_list_items',
'ms_home_create_presentation_succeeded',
'did_add_easy_pear_gslides_menu',
'clicked_easy_pear_gslides_install',
'file_imported',
'opened_app_presenter',
'loaded_sessions_fully',
'account_extended_trial',
'file_opener_user_has_popups_blocked',
'import_slides_failed',
'opened_gslides_editor_with_powerup',
'orchard_publisher_preview_user_signed_in',
'file_opener_pd_converter_fatal_error',
'bia_installer_installation_error',
'orchard_publisher_preview_error',
'vocab_editor_historical_docid_url_param_used',
'prepare_slides_from_file',
'has_used_new_presenter_2018',
'removed_slide',
'remove_deck_from_recent_activities',
'opened_drive_picker',
'import_pdf',
'session_list_action',
'orchard_publisher_accepted_terms_and_published',
'vocab_editor_pasted_list_received',
'vocab_editor_card_button_clicked',
'invited_section_via_classroom',
'ppt_addin_sidebar_opened',
'nudge_to_gslides_import_modal',
'didnt_add_easy_pear_gslides_menu_2',
'updated_student_login_service',
'copy_deck',
'duplicated_slide',
'directed_to_install_gslides_addon',
'file_opener_pd_converter_open_converted_file_requested',
'valet_page_seen',
'shown_team_drive_is_premium_modal',
'gslides_addon_extension_modal_chrome_store_opened',
'file_opener_pd_converter_conversion_completed',
'file_opener_convert_pd_file_requested',
'ppt_addin_launch_presentation_requested',
'ppt_addin_opened_add_assessment_dialog',
'ms_home_create_presentation_requested',
'orchard_publisher_edit_owner_seen',
'peardeck_signed_up',
'gslides_addon_open_library',
'gslides_addon_launch_presentation',
'file_opener_pd_converter_start_presenting_requested_from_landing_page',
'orchard_publisher_owner_preview_file_requested',
'orchard_publisher_owner_deck_categories_updated',
'opened_featured_news_feed_item',
'vocab_editor_opened_file_by_id',
'user_received_pearror',
'opened_recent_file',
'valet_flow_completed',
'vocab_editor_new_file_created',
'vocab_editor_imported_list_received',
'used_vocab_word_suggestion',
'file_opener_pd_converter_go_to_peardeck_editor_requested',
'gslides_addon_open_extension_modal_from_banner_requested',
'launched_non_peardeck_presentation',
'gslides_presentation_permission_denied',
'orchard_publisher_report_file_email_opened',
'used_quota_skip_the_editor',
'gslides_addon_installed',
'gslides_addon_switch_to_main',
'gslides_addon_open_account_creation_modal',
'orchard_publisher_terms_required_owner_seen',
'nudge_to_gslides_editor_banner',
'enabled_classroom_climate_add_on',
'orchard_publisher_terms_accepted',
'bia_installer_started_installation',
'bia_installer_completed_installation',
'teams_user_seen',
'clicked_hubspot_promotion_banner',
'clicked_easy_pear_gslides_menu',
'did_add_easy_pear_gslides_menu_2',
'microsoft_new_user_log_in',
'gslides_addon_sidebar_opened',
'gslides_addon_open_add_assessment_modal',
'flashcard_factory_opened_presenter',
'valet_prompted_for_school',
'nudge_to_gslides_ppt_modal',
'ppt_addin_added_question',
'vocab_session_completed',
'presentation_launcher_microsoft_session_uploaded',
'teams_presentation_launched',
'file_opener_pd_converter_try_again_requested',
'presentation_with_enforced_whitelist_created',
'orchard_publisher_preview_opened_resource',
'orchard_publisher_social_media_share_clicked',
'microsoft_returning_user_log_in',
'couldnt_add_easy_pear_gslides_menu_no_addon',
'ppt_addin_clicked_upgrade_to_premium',
'create_presentation',
'opened_app_vocab_editor',
'presented_with_extension',
'orchard_publisher_owner_closed_ccss_modal',
'gslides_addon_toggle_featured_library_block',
'orchard_publisher_owner_copied_preview_link',
'orchard_publisher_preview_copied_preview_link',
'couldnt_add_easy_pear_gslides_install_insert_failure',
'orchard_publisher_owner_changed_anonymity',
'bia_installer_preview_resource',
'vocab_editor_file_copied_on_load',
'didnt_add_easy_pear_gslides_menu',
'orchard_publisher_owner_error',
'gslides_addon_hide_extension_banner_requested',
'logged_in_with_google',
'unsupported_one_drive_file_type_selected'
)
THEN 1 ELSE 0 END) AS before_session_count,

SUM(CASE WHEN Event IN 
('successfully_invited_10_students',
'gslides_addon_toggle_custom_question_block',
'successfully_invited_10_students_gslides_addon',
'freeze_session_freeze_thumbnails_process_thumbnail_started',
'v2_enabled_classroom_add_on',
'successfully_invited_100_students_gslides_addon',
'successfully_invited_350_students_pd_classic',
'successfully_invited_250_students',
'successfully_invited_150_students_gslides_addon',
'successfully_invited_380_students',
'successfully_invited_450_students',
'successfully_invited_200_students_gslides_addon',
'successfully_invited_290_students_pd_classic',
'successfully_invited_430_students',
'classroom_add_on_enabled',
'freeze_session_freeze_thumbnails_process_thumbnail_uploaded_gslide_thumbnail_to_s3',
'gslides_addon_toggle_library_block',
'gslides_addon_show_present_modal',
'freeze_session_freeze_thumbnails_process_thumbnail_uploaded_s3thumbnail_to_fb',
'successfully_invited_20_students_gslides_addon',
'freeze_session_freeze_thumbnails_process_thumbnail_got_gslide_thumbnail',
'successfully_invited_60_students_pd_classic',
'successfully_invited_80_students',
'freeze_session_freeze_annotations_upload_annotations_to_session_done',
'successfully_invited_210_students_gslides_addon',
'joined_presentation',
'freeze_session_freeze_thumbnails_finished_thumbnail_batch_of_size1',
'successfully_invited_140_students_gslides_addon',
'successfully_invited_180_students_gslides_addon',
'successfully_invited_360_students',
'successfully_invited_280_students_pd_classic',
'successfully_invited_440_students',
'successfully_invited_460_students_pd_classic',
'successfully_invited_20_students',
'freeze_session_freeze_thumbnails_got_gslides_presentation',
'enabled_takeaway_add_on',
'successfully_invited_80_students_pd_classic',
'blocked_student',
'presenter_successfully_presented_to_three_students',
'presentation_launcher_first_slide_loaded',
'freeze_session_freeze_annotations_recieved_drive_file_pdf_stream',
'freeze_session_freeze_thumbnails_complete',
'successfully_invited_100_students',
'successfully_invited_140_students',
'successfully_invited_220_students',
'student_recorded_mood',
'vocab_student_responded',
'successfully_invited_190_students',
'successfully_invited_190_students_gslides_addon',
'successfully_invited_400_students',
'successfully_invited_290_students',
'successfully_invited_370_students_pd_classic',
'simultaneous_use_of_both_the_projector_and_session_dashboard_views',
'session_navigated_to_slide_missing_image',
'successfully_invited_20_students_pd_classic',
'freeze_session_found_non_gslides_presentation',
'successfully_invited_90_students_pd_classic',
'successfully_invited_60_students',
'student_joined_pp2s_deck',
'freeze_session_freeze_annotations_started',
'freeze_session_freeze_annotations_pdf_download_done',
'disabled_takeaway_add_on',
'freeze_session_got_gslides_presentation',
'successfully_invited_160_students',
'successfully_invited_50_students_gslides_addon',
'successfully_invited_170_students_gslides_addon',
'successfully_invited_230_students_pd_classic',
'successfully_invited_120_students_gslides_addon',
'opened_projector_view',
'successfully_invited_270_students',
'successfully_invited_220_students_pd_classic',
'successfully_invited_410_students_pd_classic',
'successfully_invited_320_students',
'successfully_invited_310_students_pd_classic',
'successfully_invited_390_students',
'successfully_invited_400_students_pd_classic',
'successfully_invited_2_students_pd_classic',
'presentation_launcher_first_student_joined_timing',
'successfully_invited_130_students_pd_classic',
'successfully_invited_50_students',
'freeze_session_freeze_thumbnails_process_thumbnail_complete',
'successfully_invited_40_students_pd_classic',
'successfully_invited_110_students',
'freeze_session_freeze_thumbnails_finished_thumbnail_batch',
'freeze_session_freeze_annotations_complete',
'successfully_invited_150_students_pd_classic',
'successfully_invited_90_students_gslides_addon',
'student_joined',
'successfully_invited_200_students_pd_classic',
'successfully_invited_180_students',
'successfully_invited_200_students',
'successfully_invited_340_students',
'successfully_invited_160_students_gslides_addon',
'successfully_invited_310_students',
'successfully_invited_330_students_pd_classic',
'successfully_invited_240_students_gslides_addon',
'successfully_invited_440_students_pd_classic',
'successfully_invited_30_students_pd_classic',
'session_list_menu_action',
'presenter_asked_quick_question',
'presenter_starred_response',
'successfully_invited_2_students_gslides_addon',
'freeze_session_freeze_thumbnails_finished_thumbnail_batch_of_size4',
'freeze_session_complete',
'successfully_invited_210_students_pd_classic',
'successfully_invited_180_students_pd_classic',
'successfully_invited_410_students',
'successfully_invited_430_students_pd_classic',
'successfully_invited_330_students',
'successfully_invited_300_students',
'successfully_invited_270_students_gslides_addon',
'successfully_invited_10_students_pd_classic',
'successfully_invited_90_students',
'successfully_invited_130_students',
'v2_disabled_classroom_add_on',
'freeze_session_started',
'freeze_session_freeze_thumbnails_got_firebase_user_slides',
'freeze_session_freeze_annotations_pdf_parsed_annotations_done',
'freeze_session_freeze_annotations_loaded_pdf',
'successfully_invited_140_students_pd_classic',
'successfully_invited_120_students',
'successfully_invited_160_students_pd_classic',
'successfully_invited_230_students_gslides_addon',
'vocab_student_joined',
'freeze_session_freeze_thumbnails_finished_thumbnail_batch_of_size3',
'successfully_invited_240_students_pd_classic',
'successfully_invited_260_students_pd_classic',
'successfully_invited_240_students',
'successfully_invited_190_students_pd_classic',
'successfully_invited_350_students',
'successfully_invited_360_students_pd_classic',
'successfully_invited_370_students',
'successfully_invited_450_students_pd_classic',
'successfully_invited_470_students',
'successfully_invited_280_students_gslides_addon',
'session_navigated_to_slide_missing_annotations',
'successfully_invited_30_students',
'enabled_lock_timer',
'freeze_session_freeze_thumbnails_process_thumbnail_uploaded_gslide_thumbnail_to_fb',
'successfully_invited_70_students_gslides_addon',
'successfully_invited_60_students_gslides_addon',
'student_domain_lock_enforced',
'successfully_invited_110_students_gslides_addon',
'successfully_invited_250_students_pd_classic',
'successfully_invited_230_students',
'successfully_invited_420_students_pd_classic',
'freeze_session_freeze_thumbnails_finished_thumbnail_batch_of_size2',
'successfully_invited_210_students',
'successfully_invited_170_students',
'successfully_invited_260_students_gslides_addon',
'successfully_invited_340_students_pd_classic',
'successfully_invited_380_students_pd_classic',
'successfully_invited_420_students',
'successfully_invited_50_students_pd_classic',
'successfully_invited_40_students',
'successfully_invited_100_students_pd_classic',
'successfully_invited_30_students_gslides_addon',
'unblocked_student',
'successfully_invited_110_students_pd_classic',
'freeze_session_found_presentation_link',
'successfully_invited_170_students_pd_classic',
'freeze_session_enqueued',
'successfully_invited_150_students',
'successfully_invited_260_students',
'successfully_invited_320_students_pd_classic',
'successfully_invited_220_students_gslides_addon',
'successfully_invited_280_students',
'successfully_invited_470_students_pd_classic',
'successfully_invited_2_students',
'successfully_invited_40_students_gslides_addon',
'teams_presentation_student_joined',
'successfully_invited_70_students',
'successfully_invited_70_students_pd_classic',
'freeze_session_freeze_thumbnails_finished_thumbnail_batch_of_size5',
'freeze_session_freeze_thumbnails_started',
'flashcard_factory_export_to_quizlet_clicked',
'successfully_invited_120_students_pd_classic',
'teams_projector_link_opened',
'successfully_invited_80_students_gslides_addon',
'successfully_invited_130_students_gslides_addon',
'successfully_invited_270_students_pd_classic',
'successfully_invited_300_students_pd_classic',
'successfully_invited_250_students_gslides_addon',
'successfully_invited_390_students_pd_classic',
'successfully_invited_460_students'
)
THEN 1 ELSE 0 END) AS during_session_count,

SUM(CASE WHEN Event IN ('published_takeaways',
'student_recorded_feedback',
'presented',
'archived_session_list_action',
'interrupter_was_annoying',
'flashcard_factory_ended_session_with_premade_content',
'slide_presented',
'interrupter_was_helpful',
'flashcard_factory_ended_session'
)
THEN 1 ELSE 0 END) AS after_session_count,

SUM(CASE WHEN Event IN ('account_1_prompt_for_role')
THEN 1 ELSE 0 END) AS before_signup_count

FROM `peardeck-external-projects.buisness_analytics_in_practice_project.app_events` ae JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf ON ae.user_id = uf.externalid

WHERE (DATE(timestamp) BETWEEN DATE(FirstSeen) AND DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH))
AND (DATE(FirstSeen) ", firstSeen, ") 
AND profile_role <> 'student'

GROUP BY user_id"
)

df_ae <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf)

##############################################################################
# Looping starts here
##############################################################################
 
for (k in 1:2) {
  
  if (k == 1) {

    #####################
    # DATA PULL: User Statuses
    #####################
    
    # 10 seconds
    sql_string <- paste(
    "SELECT CAST(externalid as STRING) as teacher, first_pres_after_trial, last_pres_1st_few_mnths, timestamp, 
    CASE WHEN first_pres_after_trial = timestamp THEN b.account_status END AS account_status_min,
    CASE WHEN last_pres_1st_few_mnths = timestamp THEN b.account_status END AS account_status_max
    
    FROM `peardeck-external-projects.buisness_analytics_in_practice_project.gslides_addon_launch_presentation` b
    RIGHT JOIN
    
    (
    SELECT externalid, MIN(timestamp) as first_pres_after_trial, MAX(timestamp) as last_pres_1st_few_mnths
    FROM `peardeck-external-projects.buisness_analytics_in_practice_project.gslides_addon_launch_presentation` lp_i RIGHT JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf ON lp_i.user_id = uf.externalid
    WHERE ((DATE(timestamp) BETWEEN DATE_ADD(DATE(FirstSeen), INTERVAL 1 MONTH) AND DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH)) OR timestamp IS NULL) 
    AND (DATE(FirstSeen) ", firstSeen, ")
    AND profile_role <> 'student'
    GROUP BY externalid
    ) a
    
    ON b.user_id = a.externalid AND (b.timestamp = a.first_pres_after_trial OR b.timestamp = a.last_pres_1st_few_mnths)
    --Note: count does not exactly equal what's in user facts if user hasn't launched a presentation ever."
    )
    
    query_start_time <- Sys.time()
    df_us <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf) #, allowLargeResults = TRUE
    query_end_time <- Sys.time()
    query_runtime <- query_end_time - query_start_time
    print(round(query_runtime,1))
    
    
    #####################
    # DATA PULL: slide_presented & student_responses
    #####################
    
    # NEWEST VERSION
    sql_string <- paste(
    "SELECT 
    CAST(sp.externalid AS STRING) AS teacher,
    presentation_id,
    DATE(sp.timestamp) AS Date,
    EXTRACT(HOUR FROM sp.timestamp) AS hour,
    COUNT(DISTINCT sp.presentation_id) as num_presentations,
    COUNT(DISTINCT sr.student) as num_students, 
    COUNT(sr.student) as num_students_dup
    
    FROM 
    
    (SELECT externalid, presentation_id, slide_id, timestamp, DATE(timestamp) as date, EXTRACT(HOUR FROM timestamp) as hour
    FROM `peardeck-external-projects.buisness_analytics_in_practice_project.slide_presented` sp_i RIGHT JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf_i 
    ON sp_i.user_id = uf_i.externalid
    WHERE 
    ((DATE(timestamp) BETWEEN DATE(FirstSeen) AND DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH)) 
    --OR (DATE(timestamp) BETWEEN DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR) AND DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH), INTERVAL 1 YEAR) 
    --OR timestamp IS NULL)
    )
    AND (DATE(FirstSeen) ", firstSeen, ") 
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
    AND (DATE(FirstSeen) ", firstSeen, ")
    AND profile_role <> 'student'
    ) sr 
    
    ON sp.externalid = sr.externalid AND sp.slide_id = sr.slide AND sp.presentation_id = sr.presentation AND sp.date = sr.date AND sp.hour = sr.hour
    
    GROUP BY sp.externalid, presentation_id, DATE(sp.timestamp), EXTRACT(HOUR FROM sp.timestamp)
    --Note: count does not exactly equal what's in user facts if user hasn't launched a presentation ever.
    "
    )
    
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
    sql_string <- paste(
    "SELECT CAST(externalid as STRING) as teacher, first_pres_after_trial, last_pres_1st_few_mnths, timestamp, 
    CASE WHEN first_pres_after_trial = timestamp THEN b.account_status END AS account_status_min,
    CASE WHEN last_pres_1st_few_mnths = timestamp THEN b.account_status END AS account_status_max
    
    FROM `peardeck-external-projects.buisness_analytics_in_practice_project.gslides_addon_launch_presentation` b
    RIGHT JOIN
    
    (
    SELECT externalid, MIN(timestamp) as first_pres_after_trial, MAX(timestamp) as last_pres_1st_few_mnths
    FROM `peardeck-external-projects.buisness_analytics_in_practice_project.gslides_addon_launch_presentation` lp_i RIGHT JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf ON lp_i.user_id = uf.externalid
    WHERE ((DATE(timestamp) BETWEEN DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 1 MONTH), INTERVAL 1 YEAR) AND DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH), INTERVAL 1 YEAR) OR timestamp IS NULL)) 
    AND (DATE(FirstSeen) ", firstSeen, ")
    AND profile_role <> 'student'
    GROUP BY externalid
    ) a
    
    ON b.user_id = a.externalid AND (b.timestamp = a.first_pres_after_trial OR b.timestamp = a.last_pres_1st_few_mnths)
    --Note: count does not exactly equal what's in user facts if user hasn't launched a presentation ever."
    )
    
    query_start_time <- Sys.time()
    df_us <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf) #, allowLargeResults = TRUE
    query_end_time <- Sys.time()
    query_runtime <- query_end_time - query_start_time
    print(round(query_runtime,1))
    
    #####################
    # DATA PULL: slide_presented & student_responses NEXT YEAR
    #####################
    
    # NEWEST VERSION
    sql_string <- paste(
    "SELECT 
    CAST(sp.externalid AS STRING) AS teacher,
    presentation_id,
    DATE(sp.timestamp) AS Date,
    EXTRACT(HOUR FROM sp.timestamp) AS hour,
    COUNT(DISTINCT sp.presentation_id) as num_presentations,
    COUNT(DISTINCT sr.student) as num_students,
    COUNT(sr.student) as num_students_dup
    
    FROM 
    
    (SELECT externalid, presentation_id, slide_id, timestamp, DATE(timestamp) as date, EXTRACT(HOUR FROM timestamp) as hour
    FROM `peardeck-external-projects.buisness_analytics_in_practice_project.slide_presented` sp_i RIGHT JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf_i 
    ON sp_i.user_id = uf_i.externalid
    WHERE 
    ((DATE(timestamp) BETWEEN DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR) AND DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH), INTERVAL 1 YEAR)) 
    --OR (DATE(timestamp) BETWEEN DATE_ADD(DATE(FirstSeen), INTERVAL 1 YEAR) AND DATE_ADD(DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH), INTERVAL 1 YEAR) 
    --OR timestamp IS NULL)
    )
    AND (DATE(FirstSeen) ", firstSeen, ") 
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
    AND (DATE(FirstSeen) ", firstSeen, ")
    AND profile_role <> 'student'
    ) sr 
    
    ON sp.externalid = sr.externalid AND sp.slide_id = sr.slide AND sp.presentation_id = sr.presentation AND sp.date = sr.date AND sp.hour = sr.hour
    
    GROUP BY sp.externalid, presentation_id, DATE(sp.timestamp), EXTRACT(HOUR FROM sp.timestamp)
    --Note: count does not exactly equal what's in user facts if user hasn't launched a presentation ever.
    "
    )
    
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

sql_string <- paste(
"SELECT user_id, slide_type, SUM(slide_type_ct) AS slide_type_ct 
FROM (

SELECT CAST(user_id AS STRING) AS user_id, presentation_id, slide_id, slide_type, DATE(timestamp) as Date, EXTRACT(HOUR FROM timestamp) as Hour, COUNT(DISTINCT user_id) AS slide_type_ct 
FROM `peardeck-external-projects.buisness_analytics_in_practice_project.slide_presented` sp JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf ON sp.user_id = uf.externalid
WHERE ((DATE(timestamp) BETWEEN DATE(FirstSeen) AND DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH))
AND (DATE(FirstSeen) ", firstSeen, ") AND profile_role <> 'student')
GROUP BY user_id, presentation_id, slide_id, slide_type, DATE(timestamp), EXTRACT(HOUR FROM timestamp)
) 

GROUP BY user_id, slide_type
"
)

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

sql_string <- paste(
"SELECT CAST(externalid AS STRING) as teacher, firstseen
FROM `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` 
WHERE (DATE(FirstSeen) ", firstSeen, ")
AND profile_role <> 'student'"
)

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
summ <- summarise(df_sp, total_slides = sum(slide_type_ct))
df_sp <- ungroup(df_sp)

# Merge total_slides into df_sp
df_sp <- merge(summ, df_sp, by = "teacher")

# Calculate normalized slide_type
df_sp$slide_type_amt_norm <- df_sp$slide_type_ct/df_sp$total_slides

# Create df_sp_wide
df_sp_wide <- dcast(df_sp, teacher + total_slides ~  slide_type, value.var = "slide_type_amt_norm")


#####################
# CREATE: subscription buckets
#####################

### Generated "subscription status" field cleanup

#collapse df_us to only have unique teachers (per row)
df_us$timestamp <- NULL

#do this to merge teachers into one row
df_us <- dcast(setDT(df_us), teacher ~ rowid(teacher), value.var = c("first_pres_after_trial",
                                                                     "last_pres_1st_few_mnths",
                                                                     "account_status_min",
                                                                     "account_status_max"))

# Merge 2 columns into one, since min and max often fill in either column 1 or 2
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

# Turn blanks into NAs
df_us$account_status_min <- na_if(df_us$account_status_min,"")
df_us$account_status_max <- na_if(df_us$account_status_max,"")

# Pull over relevant columns only
#df_statuses <- df_us[ , c(which(colnames(df_us)=="teacher"), which(colnames(df_us)=="account_status_max"))]
df_statuses <- df_us[ , c(1,5)]

# Remove duplicates
df_statuses <- unique(df_statuses)

# Rename column
colnames(df_statuses)[2] = "subscription"

#####################
# CREATE DFs: df_sr_wide
#####################

# Create summary: presentations by year/month
df_sr <- group_by(df_sr, teacher, Date_year_month)
summ  <- summarise(df_sr, num_presentations = sum(num_presentations)) 
df_sr <- ungroup(df_sr)

# Create wide df
df_sr_wide <- dcast(summ, teacher ~ Date_year_month, value.var="num_presentations")
# # Remove NA column (happens b/c some have NAs for all dates)
# df_sr_wide <- df_sr_wide[,-which(colnames(df_sr_wide)=="NA")]

# Create new col summing num_students: total_students
df_sr <- group_by(df_sr, teacher)
summ5  <- summarise(df_sr, total_students = sum(num_students), total_students_dup = sum(num_students_dup)) 
df_sr <- ungroup(df_sr)

# Create new col in df_sr_wide: total_presentations & total_months_used
summ <- group_by(summ, teacher)
summ2 <- summarise(summ, num_presentations = sum(num_presentations))
summ3 <- summarise(summ, total_months_used = sum(!is.na(Date_year_month)) )
summ <- ungroup(summ)

### Add to df_sr_wide: # Presentations by user & Total Months by user
df_sr_wide$total_presentations <- summ2$num_presentations
df_sr_wide$total_students <- summ5$total_students
df_sr_wide$total_students_dup <- summ5$total_students_dup
df_sr_wide$total_students_dup_prop <- df_sr_wide$total_students_dup/df_sr_wide$total_presentations
df_sr_wide$total_engagements_per_student <- df_sr_wide$total_students_dup/df_sr_wide$total_students

df_sr_wide$total_engagements_per_student[which(is.nan(df_sr_wide$total_engagements_per_student))] <- NA

df_sr_wide$total_months_used <- summ3$total_months_used

df_sr$num_students[is.na(df_sr$num_students)] <- 0 

### Student Engagement

# Create bucket labels for num_students
df_sr$num_students_label <- df_sr$num_students
df_sr$num_students_label[which(df_sr$num_students == 0)] <- "0_Testing"
df_sr$num_students_label[which(df_sr$num_students == 1)] <- "1_Low"
df_sr$num_students_label[which(df_sr$num_students >= 2 & df_sr$num_students <= 10)] <- "2_Low-Medium"
df_sr$num_students_label[which(df_sr$num_students >= 11 & df_sr$num_students <= 19)] <- "3_Medium"
df_sr$num_students_label[which(df_sr$num_students >= 20 & df_sr$num_students <= 30)] <- "4_Medium-High"
df_sr$num_students_label[which(df_sr$num_students >= 31)] <- "5_High"

# Rename the levels (note: did it this way to preserve the desired order)
df_sr$num_students_label <- factor(df_sr$num_students_label)
levels(df_sr$num_students_label) <- paste("Num_stu_", substr(levels(df_sr$num_students_label), 3, nchar(levels(df_sr$num_students_label))), sep="")

# Create summary: Count in each student engagement bucket by user
df_sr <- group_by(df_sr, teacher, num_students_label)
summ  <- summarise(df_sr, num_students_label_ct = n()) 
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
summ  <- summarise(df_sr, YM_first_use = min(Date_year_month_2), YM_last_use = max(Date_year_month_2)) 
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

#####################
# MERGE: Pull in df_uf (captures people with no usage) & df_statuses
#####################

# Merge df_uf & df_sr_wide, with df_uf on left (to get new users from df_uf)
df_sr_wide <- merge(x= df_uf, y=df_sr_wide, by = "teacher", all.x = TRUE)

# Merge df_statuses into df_sr_wide
df_sr_wide <- merge(x = df_sr_wide, y = df_statuses, by = "teacher", all.x = TRUE)

# Add columns from dfuf_into_dfsr on df_sr_wide
#df_sr_wide <- merge(x= df_sr_wide, y = entire_dfuf, by = c("teacher","firstseen"), all.x = TRUE)

df_sr_wide$total_presentations[is.na(df_sr_wide$total_presentations)]<- 0

#####################
# CREATE: usage buckets
#####################

# Create empty column
df_sr_wide$usage_label <- as.character("")

# Never used
df_sr_wide$usage_label[which( df_sr_wide$total_presentations == 0)] <- "Never Used"
# Tested Product Only
df_sr_wide$usage_label[which( df_sr_wide$total_presentations > 0 & df_sr_wide$total_students <= 1)] <- "Tested Product Only"
# Presented to 2+ went free
df_sr_wide$usage_label[which( df_sr_wide$total_presentations > 0 & df_sr_wide$total_students > 1)] <- "Used Product"

#### CODE WAS USED WHEN SUBSCRIPTION STATUS & USAGE STATUS WERE COMBINED.
# # Create empty column
# df_sr_wide$status_label <- as.character("")
# 
# # Never used
# df_sr_wide$status_label[which( df_sr_wide$total_presentations == 0)] <- "Never Used"
# # Tested Product Only
# df_sr_wide$status_label[which( df_sr_wide$total_presentations > 0 & df_sr_wide$total_students <= 1)] <- "Tested Product Only"
# # Presented to 2+ went free
# df_sr_wide$status_label[which( df_sr_wide$total_presentations > 0 & df_sr_wide$total_students > 1 & df_sr_wide$account_status_max == "free")] <- "Used Product Free"
# # Presented to 2+ went premium
# df_sr_wide$status_label[which( df_sr_wide$total_presentations > 0 & df_sr_wide$total_students > 1 & df_sr_wide$account_status_max == "premium")] <- "Used Product Premium"
# # Presented to 2+ went premiumtrial
# df_sr_wide$status_label[which( df_sr_wide$total_presentations > 0 & df_sr_wide$total_students > 1 & df_sr_wide$account_status_max == "premiumTrial")] <- "Used Product PremiumTrial"
# # Presented to 2+ but status unknown (i.e. they used the product in the 1st month but not the subsequent months in the time span)
# df_sr_wide$status_label[which( df_sr_wide$total_presentations > 0 & df_sr_wide$total_students > 1 & is.na(df_sr_wide$account_status_max) == TRUE)] <- "Used Product Unknown Status"

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
# DATA PULL: Teachers who have used flashcard factory
#####################

sql_string <- paste("
SELECT CAST(externalid as STRING) as teacher, COUNT(externalid) as num_flash_use
FROM `peardeck-external-projects.buisness_analytics_in_practice_project.student_responses` sr_i RIGHT JOIN `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized` uf_i 
ON sr_i.teacher = uf_i.externalid
WHERE app like '%flash%' AND ((DATE(timestamp) BETWEEN DATE(FirstSeen) AND DATE_ADD(DATE(FirstSeen), INTERVAL 3 MONTH))) 
AND (DATE(FirstSeen) ", firstSeen, ") 
AND profile_role <> 'student'
GROUP BY CAST(externalid as STRING)
"
)

# Save into dataframe
df_ff <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf)

# Add binary column: ff_use
df_ff$ff_use <- as.numeric(1)

# Reduce to only unique columns of interest
df_ff <- df_ff[,-2]

# Merge into df_sr_wide
df_sr_wide <- merge(x = df_sr_wide, y = df_ff, by = "teacher", all.x = TRUE)

# Turn NAs into zeros
df_sr_wide$ff_use[which(is.na(df_sr_wide$ff_use))] <- 0

#####################
# DATA PULL: add in year later account status & account type
#####################

sql_string <- paste(
"WITH
Cohort AS (
SELECT externalId user_id, PrimaryAccountDetails.currentlyPremium, PrimaryAccountDetails.currentlyOnTrial, PrimaryAccountDetails.accountType
FROM `peardeck-external-projects.buisness_analytics_in_practice_project.user_facts_anonymized`
WHERE (DATE(FirstSeen) ", firstSeen, ")
AND profile_role <> 'student'
)

SELECT CAST(user_id AS STRING) AS teacher, 
CASE 
WHEN currentlyPremium = false AND currentlyOnTrial = false THEN 'free'
WHEN currentlyPremium = true AND currentlyOnTrial = false THEN 'premium'
WHEN currentlyPremium = true AND currentlyOnTrial = true THEN 'premiumtrial' END AS accountStatus_1yr_later, accountType as accountType_1yr_later
FROM Cohort"
)

# Pull data
# Run time: 3.6 sec
query_start_time <- Sys.time()
df_as <- query_exec(sql_string, project = project_id, use_legacy_sql = FALSE, max_pages = Inf)
query_end_time <- Sys.time()
query_runtime <- query_end_time - query_start_time
print(round(query_runtime,1))

# Change change "individual - synthetic" to "individual"
df_as$accountType_1yr_later[which(df_as$accountType_1yr_later == "individual - synthetic")] <- "individual"

# Merge year-later account status & account type to df_sr_wide
df_sr_wide <- merge(x = df_sr_wide, y = df_as, by = "teacher", all.x = TRUE)

#####################
# CREATE: num_prez_audience & num_prez_testing
#####################

# Create new columns: num_prez_audience & num_prez_testing
df_sr_wide$num_prez_audience <- df_sr_wide$total_presentations-(df_sr_wide$total_presentations*df_sr_wide$Num_stu_Testing)
df_sr_wide$num_prez_testing <- dtotal_months_usedf_sr_wide$total_presentations*df_sr_wide$Num_stu_Testing

# Turn blanks from num_prez_audience & num_prez_testing to zeros
df_sr_wide$num_prez_audience[which(is.na(df_sr_wide$num_prez_audience))] <- 0
df_sr_wide$num_prez_testing[which(is.na(df_sr_wide$num_prez_testing))] <- 0

#####################
# CREATE: total_prez_label
#####################

### Create column: total_prez_label
# Initialize column
df_sr_wide$total_prez_label <- as.character(NA)
# Add labels
df_sr_wide$total_prez_label[which(df_sr_wide$total_presentations == 0)] <- "0_None"
df_sr_wide$total_prez_label[which(df_sr_wide$total_presentations > 0 & df_sr_wide$total_presentation <= 9)] <- "1_Light"
df_sr_wide$total_prez_label[which(df_sr_wide$total_presentations >=10 & df_sr_wide$total_presentations <= 19)] <- "2_Medium"
df_sr_wide$total_prez_label[which(df_sr_wide$total_presentations >=20 & df_sr_wide$total_presentations <= 34)] <- "3_Heavy"
df_sr_wide$total_prez_label[which(df_sr_wide$total_presentations >= 35)] <- "4_Superuser"
# Rename the levels (note: did it this way to preserve the desired order)
df_sr_wide$total_prez_label <- factor(df_sr_wide$total_prez_label)
levels(df_sr_wide$total_prez_label) <- substr(levels(df_sr_wide$total_prez_label), 3, nchar(levels(df_sr_wide$total_prez_label)))


##############################################################################
# Integrate initial months with 1 year later
##############################################################################
  
  if (k == 1) {
    df_sr_wide_INITIAL <- df_sr_wide
    df_sr_INITIAL <- df_sr
    df_us_INITIAL <- df_us
  }
  
  if (k == 2) {
    
    # When status unknown for year later, use user facts status
    df_sr_wide$subscription[which(is.na(df_sr_wide$subscription) == TRUE)] <- df_sr_wide$accountStatus_1yr_later[which(is.na(df_sr_wide$subscription) == TRUE)]
    
    # Create binary variable from Year Later data
    df_sr_wide$dep_prez_usage_yr_later <- ifelse((df_sr_wide$total_presentations > 0 & df_sr_wide$usage_label != "Tested Product Only"), 1, 0)
    
    # Pull year later fields into Initial DF
    df_sr_wide_INITIAL$usage_label_yr_later <- df_sr_wide$usage_label
    df_sr_wide_INITIAL$subscription_yr_later <- df_sr_wide$subscription
    df_sr_wide_INITIAL$dep_total_presentations_yr_later <- df_sr_wide$total_presentations
    df_sr_wide_INITIAL$dep_prez_usage_yr_later <- df_sr_wide$dep_prez_usage_yr_later
    
    # Create binary variable on INITIAL set
    df_sr_wide_INITIAL$prez_usage_initial <- ifelse((df_sr_wide_INITIAL$total_presentations > 0 & df_sr_wide_INITIAL$usage_label != "Tested Product Only"), 1, 0)
    
    # Create df_wide (removing irrelevant columns) from Initial DF
    df_wide <- df_sr_wide_INITIAL[,-c(which(colnames(df_sr_wide_INITIAL)=="accountStatus_1yr_later"), 
                                      which(colnames(df_sr_wide_INITIAL)=="account_status_min"))]
    # Rename columns for clarity
    names(df_wide)[names(df_wide) == "usage_label"] <- "usage_label_initial_months"
    names(df_wide)[names(df_wide) == "subscription"] <- "subscription_initial_months"
    
    # Create binary dependent variables: Subscription Status
    df_wide$dep_premium <- ifelse(df_wide$subscription_yr_later == "premium", 1, 0)
    df_wide$dep_premiumtrial <- ifelse(df_wide$subscription_yr_later == "premiumTrial", 1, 0)
    df_wide$dep_free <- ifelse(df_wide$subscription_yr_later == "free", 1, 0)
    
  }


}

# total_students: turn NAs to zeros
df_wide$total_students[which(is.na(df_wide$total_students))] <- 0
#df_wide$slideDiversity[which(is.na(df_wide$slideDiversity))] <- 0 # Might not want to do this -- 0 means low slide diversity, but NA is actually the absence of values. 



#####################
# MERGE: add in participated_b4_used & app_events data (from initial months)
#####################

# Merge in participated in presentation before presented
df_wide <- merge(x = df_wide, y = df_ppb4, by = "teacher", all.x = TRUE)
# participated_b4_used: turn NAs to zeros
df_wide$participated_b4_used[which(is.na(df_wide$participated_b4_used))] <- 0

# # Merge in prez_5_plus
# df_wide <- merge(x = df_wide, y = df_5_plus, by = "teacher", all.x = TRUE)
# # participated_b4_used: turn NAs to zeros
# df_wide$prez_5_plus[which(is.na(df_wide$prez_5_plus))] <- 0

# Merge in AE data
df_wide <- merge(x = df_wide, y = df_ae, by = "teacher", all.x = TRUE)



##############################################################################
# Explore Distributions
##############################################################################

#######################################
# Dependent Variables
#######################################

# Create bar/histogram plotting function
plot_bar <- function(dfx, xaxistitle, title, w = 15, h = 7, bw=0, xcart = 0) 
  {
    p <- qplot(dfx, geom=geomtype, stat="count") + 
    scale_y_continuous(name = "Count", labels = scales::comma) +  # unit_format(unit = "K") +
    scale_x_discrete(name=xaxistitle) +
    coord_cartesian(xlim = c(0, xcart)) +
    theme(panel.grid.minor.y = element_blank()) + 
    theme(panel.background = element_blank()) +
    ggtitle(title) +
    theme(text = element_text(size = 14)) 
    
    ggsave(filename = paste(title, ".png", sep=""), plot = p, width = w, height = h, units = "in")
  }


# Bar plot: usage_label_initial_months
plot_bar(dfx=df_wide$usage_label_initial_months, xaxistitle="Statuses", title="Total Users by Statuses (Initial Months)")

# Bar Plot: usage_label_yr_later
plot_bar(dfx=df_wide$usage_label_yr_later, xaxistitle="Statuses", title="Total Users by Statuses (Year Later)")

# Histogram plot: # of presentations for an audience
p <- qplot(df_wide2$num_prez_audience, geom="histogram", binwidth = 5) +
  scale_y_continuous(name = "Count", labels = scales::comma) +  # unit_format(unit = "K") +
  scale_x_continuous(name="Number of Presentations Given to an Audience") +
  theme(panel.grid.minor.y = element_blank()) + 
  theme(panel.background = element_blank()) +
  ggtitle("Presentations Given to Audience") + 
  theme(text = element_text(size = 14)) 

ggsave(filename = "Presentations Given to Audience.png", plot = p, width = 15, height = 7, units = "in")

# Histogram plot: # of presentations for an audience (zoomed in)
p <- qplot(df_wide2$num_prez_audience, geom="histogram", binwidth = 1) +
  scale_y_continuous(name = "Count", labels = scales::comma) +  # unit_format(unit = "K") +
  scale_x_continuous(name="Number of Presentations Given to an Audience") +
  coord_cartesian(xlim = c(0, 60)) +
  theme(panel.grid.minor.y = element_blank()) + 
  theme(panel.background = element_blank()) +
  ggtitle("Presentations Given to Audience (Zoomed In)") + 
  theme(text = element_text(size = 14)) 

ggsave(filename = "Presentations Given to Audience (Zoomed In).png", plot = p, width = 15, height = 7, units = "in")


#######################################
# Independent Variables viewed proportionally against Dependent Variable
#######################################

# Create proportional, facetted plotting function
prop_plot <- function(df = df_wide2, x, facet = df_wide2$usage_label_yr_later, xaxistitle, title, legendtitle = "Status", w = 15, h = 7) #, zoomY=0
{
  #if(zoomY != 0) title = paste(title,"(zoomed in)")
  
  # Load libraries
  library(tidyverse)
  library(scales) 
  
  # Generate counts for every number of x for every facet.
  df <- group_by(df, !! facet, !! x)
  df_prop1 <- summarise(df, n = n())
  df <- ungroup(df)
  
  # Sum the counts and report by facet label
  df_prop1 <- group_by(df_prop1, `<chr>`)
  df_prop2 <- summarise(df_prop1, total =sum(n))
  df_prop1 <- ungroup(df_prop1)
  
  # Add the status label totals together with the counts
  df_prop <<- merge(x=df_prop1, y=df_prop2, by = "<chr>", all.x = TRUE)
  
  # Create proportion 
  df_prop$prop <<- df_prop$n/df_prop$total
  
  # Visualize x
  p <- ggplot(aes(x=`<dbl>`,y=prop, fill = `<chr>`), data = df_prop) + #, binwidth = 5
    geom_bar(stat = 'identity') +
    facet_grid(. ~ `<chr>`) +
    scale_y_continuous(name = "Percentage", labels = percent) + 
    scale_x_continuous(name=xaxistitle) +
    theme(panel.grid.minor.y = element_blank()) + 
    theme(panel.background = element_blank()) +
    ggtitle(title) + 
    theme(text = element_text(size = 14)) + 
    guides(fill=guide_legend(title=legendtitle))
  
  #if(zoomY != 0) p + coord_cartesian(ylim = c(0, zoomY))
  
  ggsave(filename = paste(title, ".png", sep=""), plot = p, width = w, height = h, units = "in")
  print(p)
  
}

prop_plot(x = df_wide2$total_presentations, xaxistitle = "Total Presentations", title = "Total Presentations by User")
prop_plot(x = df_wide2$total_students, xaxistitle = "Total Students", title = "Total Students Presented to by User")
prop_plot(x = df_wide2$total_months_used, xaxistitle = "Number of Months Used", title = "Total Months Used by User")
prop_plot(x = df_wide2$num_prez_audience, xaxistitle = "Number of Presentations to an Audience", title = "Total Presentations to an Audience")
prop_plot(x = df_wide2$num_prez_testing, xaxistitle = "Presentation Tests", title = "Total Presentations launched Testing Product")
prop_plot(x = df_wide2$num_prez_audience, xaxistitle = "Number of Presentations Given to an Audience", title = "Presentations Given to Audience")

########### plot testing !!!!!!!!!!!!!!

ggplot(data=df_wide2, aes(x=total_presentations_yr_later, y=slideDiversity, colour=usage_label_yr_later)) + #, group=supp, colour=supp
  #geom_line() +
  geom_point()

ggplot(data=df_wide2, aes(x=total_presentations_yr_later, y=slideDiversity)) + geom_point()

##############################################################################
# Analysis
##############################################################################

#Running Chi Square and One way ANOVA tests to test for significant diffences

#Chi square test requires a table as an input, this makes sense b/c
#Chi square compares categorical variable to categorical variable
#One way Anova compares one categorical variable against a continuous variable
