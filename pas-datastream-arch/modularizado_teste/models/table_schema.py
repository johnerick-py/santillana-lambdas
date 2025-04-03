"""
Este módulo define os esquemas das tabelas utilizadas no Redshift.
"""

# Colunas da tabela gold.dim_lrs_result_scorm
LRS_RESULT_SCORM = [
    "dt_load",                   #0
    "id_system",                 #1
    "lrs_id",                    #2
    "client_id",                 #3
    "statement_id",              #4
    "correct_responses_pattern", #5
    "response",                  #6
    "multiple_recording",        #7
    "image"                      #8
]

# Colunas da tabela gold.dim_school_class
DIM_SCHOOL_CLASS = [
"dt_load",                   #0
"school_class_id",           #1
"school_info_id",            #2
"base_group_id",             #3
"order_number",              #4
"school_level_id",           #5
"score_metric",              #6
"library",                   #7
"forum",                     #8
"grade_id",                  #9
"school_class_name"          #10
]

DIM_SCHOOL_GRADE_GROUP = [
"dt_load",                   #0
"school_level_id",           #1
"session_id",                #2
"school_id",                 #3
"school_level_session_id",   #4
"grade_id",                  #5
"group_id",                  #6
"group_description",         #7
"description",               #8
"school_grade_group_id",     #9
"grade_code",                #10
"group_code",                #11
"level_code",                #12
"grade_description"          #13
]

DIM_SCHOOL_LEVEL_SESSION = [
"dt_load",                   #0
"school_level_session_id",   #1
"school_level_session_code", #2
"session_id",                #3
"school_id",                 #4
"school_level_id",           #5
"company_id",                #6
"active_status",             #7
"demo_status",               #8
"image",                     #9
"payment_allowed"            #10
]

DIM_SESSION = [
"dt_load",                   #0
"session_id",                #1
"company_id",                #2
"session_code",              #3
"session_description",       #4
"session_paying_status",     #5
"session_active"             #6
]

DIM_SECTION_SUBJECT = [
"dt_load",                   #0
"school_class_id",           #1
"subject_grade_id",          #2
"subject_class_id",          #3
"school_id",                 #4
"subject_id",                #5
"school_code",               #6
"subject_class_name",        #7
"score_metric",              #8
"section_subject_weight",    #9
"order_number"               #10
]

DIM_SCHOOL = [
"dt_load",                   #0
"school_id",                 #1
"country_id",                #2
"school_id_system",          #3
"name",                      #4
"corporate_id",              #5
"corporate_name",            #6
"active_status",             #7
"demo_status"                #8
]

DIM_SCHOOL_ADDRESS = [
"dt_load",                   #0
"full_address",              #1
"school_address_id",         #2
"building_site_number",      #3
"neighborhood",              #4
"city",                      #5
"postal_code",               #6
"address_type"               #7
]

DIM_SCHOOL_PHONE = [
"school_phone_id",           #0
"phone_number",              #1
"phone_type"                 #2
]

DIM_SCHOOL_EMAIL = [
"school_email_id",           #0
"email"                      #1
]

DIM_USER = [
"dt_load",                   #0
"user_id",                   #1
"country_id",                #2
"language_code",             #3
"full_name",                 #4
"first_name",                #5
"second_name",               #6
"last_name",                 #7
"birth_date",                #8
"gender",                    #9
"fiscal_id",                 #10
"official_id",               #11
"uno_user_name",             #12
"active_status"              #13
]

DIM_USER_PHONE = [
"dt_load",                   #0
"user_phone_id",             #1
"phone_number",              #2
"phone_type"                 #3
]

DIM_USER_EMAIL = [
"dt_load",                   #0
"user_email_id",             #1
"email"                      #2
]

DIM_USER_ADDRESS = [
"dt_load",                   #0
"user_address_id",           #1
"address_type",              #2
"full_address",              #3
"city",                      #4
"neighborhood",              #5
"postal_code",               #6
"building_site_number",      #7
"country_id"                 #8
]

DIM_USER_GROUP = [
"dt_load",                   #0
"group_id",                  #1
"grade_id",                  #2
"school_id",                 #3
"school_level_id",           #4
"school_stage_season_id",    #5
"school_level_session_id",   #6
"session_id",                #7
"person_id",                 #8
"role_id",                   #9
"person_rol_id",             #10
"active_status"              #11
]

DIM_USER_ROLE = [
"dt_load",                   #0
"user_role_id",              #1
"student_id",                #2
"user_login",                #3
"role_id",                   #4
"status_role",               #5
"school_level_id",           #6
"school_id",                 #7
"company_id",                #8
"school_level_sessions_id",  #9
"grade_id",                  #10
"group_id",                  #11
"school_grade_group_id",     #12
"status_user_login",         #13
"person_id"                  #14
]

DIM_CLASS_PARTICIPANT = [
"dt_load",                   #0
"user_id",                   #1
"role_id",                   #2
"section_id",                #3
"person_role_id",            #4
"active_status",             #5
"dt_year"                    #6
]