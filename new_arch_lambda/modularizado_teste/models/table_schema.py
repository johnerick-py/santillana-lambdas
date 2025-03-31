"""
Este módulo define os esquemas das tabelas utilizadas no Redshift.
"""

# Colunas da tabela gold.dim_lrs_object_http
LRS_OBJECT_HTTP_COLUMNS = [
    "dt_load",                   #0
    "id_system",                 #1
    "lrs_id",                    #2
    "client_id",                 #3
    "statement_id",              #4
    "object_id",                 #5
    "action_type_id",            #6
    "resource_id",               #7
    "page_id",                   #8
    "unit_id",                   #9
    "school_class_id",           #10
    "student_id",                #11
    "school_id",                 #12
    "role_id",                   #13
    "subject_grade_id",          #14
    "section_subject_id",        #15
    "grade_id",                  #16
    "source_id",                 #17
    "type_evaluation",           #18
    "planning_id",               #19
    "moment_code",               #20
    "unit_code",                 #21
    "group_id",                  #22
    "resource_ref_id",           #23
    "activity_id",               #24
    "learning_unit_id",          #25
    "learning_objective_id",     #26
    "page_ref",                  #27
    "type_intelligence",         #28
    "level_code",                #29
    "learning_level_id",         #30
    "adventure_code",            #31
    "activity_type",             #32
    "act_type",                  #33
    "learning_objective_id_api", #34
    "achievement_code"           #35
]

# Colunas da tabela gold.dim_lrs_result
LRS_RESULTS_COLUMNS = [
    "dt_load",          #0
    "id_system",        #1
    "lrs_id",           #2
    "client_id",        #3
    "statement_id",     #4
    "success_status",   #5
    "response",         #6
    "duration",         #7
    "score_scaled",     #8
    "score_raw",        #9
    "score_min",        #10
    "score_max",        #11
    "completion",       #12
    "interaction_type", #13
    "extensions"        #14
]

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

# Colunas da tabela gold.dim_verb
DIM_VERB = [
    "dt_load",       #0
    "id_system",     #1
    "lrs_id",        #2
    "client_id",     #3
    "statement_id",  #4
    "verb_id",       #5
    "display_de_de", #6
    "display_en_us", #7
    "display_fr_fr", #8
    "display_es_es", #9
    "display_und",   #10
    "display_es"     #11
]

# Colunas da tabela gold.dim_statement
DIM_STATEMENT = [
    "dt_load",            #0
    "id_system",          #1
    "lrs_id",             #2
    "client_id",          #3
    "statement_id",       #4
    "date_insert",        #5
    "active_status",      #6
    "verb_id",            #7
    "activity_id",        #8
    "related_agents",     #9
    "related_activities", #10
    "date_stored",        #11
    "voided_status",      #12
    "dt_time"             #13
]