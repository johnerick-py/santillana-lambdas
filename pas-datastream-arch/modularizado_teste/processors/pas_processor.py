"""
Módulo que contém os processadores específicos para cada tabela definida em table_schema.py.
"""
import logging
from utils.redshift_utils import build_processed_record
from models.table_schema import (
    DIM_SCHOOL_CLASS,
    DIM_SCHOOL_GRADE_GROUP,
    DIM_SCHOOL_LEVEL_SESSION,
    DIM_SESSION,
    DIM_SECTION_SUBJECT,
    DIM_SCHOOL,
    DIM_SCHOOL_ADDRESS,
    DIM_SCHOOL_PHONE,
    DIM_SCHOOL_EMAIL,
    DIM_USER,
    DIM_USER_PHONE,
    DIM_USER_EMAIL,
    DIM_USER_ADDRESS,
    DIM_USER_GROUP,
    DIM_USER_ROLE,
    DIM_CLASS_PARTICIPANT
)

logger = logging.getLogger()

def process_school_class_event(payload):
    """
    Processa dados para a tabela gold.dim_school_class.
    """
    processed = {}

    processed['gold.dim_school_class'] = build_processed_record(
        [
            (DIM_SCHOOL_CLASS[1], payload.get("data", {}).get("attributes", {}).get("refId", None)),
            (DIM_SCHOOL_CLASS[2], payload.get("data", {}).get("attributes", {}).get("unoSchool", {}).get("refId", None)),
            (DIM_SCHOOL_CLASS[3], payload.get("data", {}).get("base_group_id", None)), # nao tem
            (DIM_SCHOOL_CLASS[4], payload.get("data", {}).get("attributes", {}).get("order", None)),
            (DIM_SCHOOL_CLASS[5], payload.get("data", {}).get("attributes", {}).get("schoolLevel", {}).get("refId", None)),
            (DIM_SCHOOL_CLASS[6], payload.get("data", {}).get("score_metric", None)), # nao tem
            (DIM_SCHOOL_CLASS[7], payload.get("data", {}).get("library", None)), # nao tem
            (DIM_SCHOOL_CLASS[8], payload.get("data", {}).get("forum", None)), # nao tem
            (DIM_SCHOOL_CLASS[9], payload.get("data", {}).get("grade_id", None)), # nao tem
            (DIM_SCHOOL_CLASS[10], payload.get("data", {}).get("attributes", {}).get("name", None)),
            (DIM_SCHOOL_CLASS[11], payload.get("data", {}).get("attributes", {}).get("schoolGradeGroup", {}).get("refId", None)),
            (DIM_SCHOOL_CLASS[12], payload.get("data", {}).get("attributes", {}).get("session", {}).get("refId", None)),

            

        ],
        DIM_SCHOOL_CLASS
    )

    return processed

def process_school_grade_group_event(payload):
    """
    Processa dados para a tabela gold.dim_school_grade_group.
    """
    processed = {}

    processed['gold.dim_school_grade_group'] = build_processed_record(
        [
            (DIM_SCHOOL_GRADE_GROUP[1], payload.get("data", {}).get("attributes", {}).get("schoolLevelSession", {}).get("schoolLevel", {}).get("refId", None)), #school_level_id
            (DIM_SCHOOL_GRADE_GROUP[2], payload.get("data", {}).get("attributes", {}).get("schoolLevelSession", {}).get("session", {}).get("refId", None)), #session_id
            (DIM_SCHOOL_GRADE_GROUP[3], payload.get("data", {}).get("attributes", {}).get("schoolLevelSession", {}).get("unoSchool", {}).get("refId", None)), #school_id
            (DIM_SCHOOL_GRADE_GROUP[4], payload.get("data", {}).get("attributes", {}).get("schoolLevelSession", {}).get("refId", None)), # school_level_session_id
            (DIM_SCHOOL_GRADE_GROUP[5], payload.get("data", {}).get("attributes", {}).get("grade", {}).get("refId", None)), #grade_id
            (DIM_SCHOOL_GRADE_GROUP[6], payload.get("data", {}).get("attributes", {}).get("group", {}).get("refId", None)), # group_id
            (DIM_SCHOOL_GRADE_GROUP[7], payload.get("data", {}).get("attributes", {}).get("group", {}).get("groupDescription", None)), #group_description
            (DIM_SCHOOL_GRADE_GROUP[8], payload.get("data", {}).get("attributes", {}).get("alias", None)), #description
            (DIM_SCHOOL_GRADE_GROUP[9], payload.get("data", {}).get("attributes", {}).get("refId", None)), #grade_group_id
            (DIM_SCHOOL_GRADE_GROUP[10], payload.get("data", {}).get("grade_code", None)), #nao achei
            (DIM_SCHOOL_GRADE_GROUP[11], payload.get("data", {}).get("group_code", None)), #nao achei
            (DIM_SCHOOL_GRADE_GROUP[12], payload.get("data", {}).get("level_code", None)), #nao achei
            (DIM_SCHOOL_GRADE_GROUP[13], payload.get("data", {}).get("grade_description", None)) #nao achei
        ],
        DIM_SCHOOL_GRADE_GROUP
    )

    return processed

def process_school_level_session_event(payload):
    """
    Processa dados para a tabela gold.dim_school_level_session.
    """
    processed = {}

    processed['gold.dim_school_level_session'] = build_processed_record(
        [
            (DIM_SCHOOL_LEVEL_SESSION[1], payload.get("data", {}).get("attributes", {}).get("refId", None)),#school_level_session_id
            (DIM_SCHOOL_LEVEL_SESSION[2], payload.get("data", {}).get("school_level_session_code", None)),#school_level_session_code nao achei
            (DIM_SCHOOL_LEVEL_SESSION[3], payload.get("data", {}).get("attributes", {}).get("session", {}).get("refId", None)), #session_id
            (DIM_SCHOOL_LEVEL_SESSION[4], payload.get("data", {}).get("attributes", {}).get("unoSchool", {}).get("refId", None)), #school_id 
            (DIM_SCHOOL_LEVEL_SESSION[5], payload.get("data", {}).get("attributes", {}).get("schoolLevel", {}).get("refId", None)),# school_level_id
            (DIM_SCHOOL_LEVEL_SESSION[6], payload.get("data", {}).get("attributes", {}).get("empresa", {}).get("refId", None)), #company_id
            (DIM_SCHOOL_LEVEL_SESSION[7], payload.get("data", {}).get("attributes", {}).get("active", None)), #active_status
            (DIM_SCHOOL_LEVEL_SESSION[8], payload.get("data", {}).get("demo_status", None)), #demo_status nao achei
            (DIM_SCHOOL_LEVEL_SESSION[9], payload.get("data", {}).get("attributes", {}).get("image", None)), #image
            (DIM_SCHOOL_LEVEL_SESSION[10], payload.get("data", {}).get("attributes", {}).get("paymentAllowed", None)) #payment_allowed
        ],
        DIM_SCHOOL_LEVEL_SESSION
    )

    return processed

def process_session_event(payload):
    """
    Processa dados para a tabela gold.dim_session.
    """
    processed = {}

    processed['gold.dim_session'] = build_processed_record(
        [
            (DIM_SESSION[1], payload.get("data", {}).get("attributes", {}).get("refId", None)), #session_id
            (DIM_SESSION[2], payload.get("data", {}).get("attributes", {}).get("empresa", {}).get("refId", None)), #company_id
            (DIM_SESSION[3], payload.get("data", {}).get("attributes", {}).get("sessionCode", None)), #session_code
            (DIM_SESSION[4], payload.get("data", {}).get("attributes", {}).get("sessionDescription", None)), #session_description
            (DIM_SESSION[5], payload.get("data", {}).get("attributes", {}).get("sessionPaying", None)), #session_paying_status
            (DIM_SESSION[6], payload.get("data", {}).get("attributes", {}).get("sessionActive", None)) #session_active
        ],
        DIM_SESSION
    )

    return processed

def process_subject_school_class_event(payload):
    """
    Processa dados para a tabela gold.dim_section_subject.
    """
    processed = {}

    processed['gold.dim_section_subject'] = build_processed_record(
        [
            (DIM_SECTION_SUBJECT[1], payload.get("data", {}).get("attributes", {}).get("schoolClass", {}).get("school_class_id", None)), #school_class_id
            (DIM_SECTION_SUBJECT[2], payload.get("data", {}).get("attributes", {}).get("subjectSchoolClass", {}).get("subjectGradeSchoolClassRefId", None)), #subject_grade_id
            (DIM_SECTION_SUBJECT[3], payload.get("data", {}).get("attributes", {}).get("subjectSchoolClass", {}).get("subjectSchoolClassRefId", None)), #subject_class_id
            (DIM_SECTION_SUBJECT[4], payload.get("data", {}).get("attributes", {}).get("unoSchool", {}).get("refId", None)), #school_id
            (DIM_SECTION_SUBJECT[5], payload.get("data", {}).get("attributes", {}).get("subjectSchoolClass", {}).get("subjectRefId", None)), #subject_id
            (DIM_SECTION_SUBJECT[6], payload.get("school_code", None)), #school_code nao achei
            (DIM_SECTION_SUBJECT[7], payload.get("data", {}).get("attributes", {}).get("subjectSchoolClass", {}).get("subjectSchoolClassName", None)), #subject_class_name
            (DIM_SECTION_SUBJECT[8], payload.get("score_metric", None)), #score_metric nao achei
            (DIM_SECTION_SUBJECT[9], payload.get("section_subject_weight", None)), #section_subject_weight nao achei
            (DIM_SECTION_SUBJECT[10], payload.get("data", {}).get("attributes", {}).get("subjectSchoolClass", {}).get("order", None)), #order_number
            (DIM_SECTION_SUBJECT[11], payload.get("data", {}).get("attributes", {}).get("schoolLevel", {}).get("refId", None)), #school_level_id
            (DIM_SECTION_SUBJECT[12], payload.get("data", {}).get("attributes", {}).get("schoolStageSeason", {}).get("refId", None)), #school_stage_season
            (DIM_SECTION_SUBJECT[13], payload.get("data", {}).get("attributes", {}).get("schoolLevelSession", {}).get("refId", None)), #school_level_session_id
            (DIM_SECTION_SUBJECT[14], payload.get("data", {}).get("attributes", {}).get("session", {}).get("refId", None))#session_id
        ],
        DIM_SECTION_SUBJECT
    )

    return processed

def process_uno_school_event(payload):
    """
    Processa dados para a tabela gold.dim_school.
    """
    processed = {}
    
    school_id = payload.get("data", {}).get("attributes", {}).get("school_id", None)

    processed['gold.dim_school'] = build_processed_record(
        [
            (DIM_SCHOOL[1], school_id), #school_id
            (DIM_SCHOOL[2], payload.get("data", {}).get("attributes", {}).get("country", {}).get("refId", None)), #country_id
            (DIM_SCHOOL[3], payload.get("data", {}).get("attributes", {}).get("schoolIdSystem", None)), #school_id_system
            (DIM_SCHOOL[4], payload.get("data", {}).get("attributes", {}).get("name", None)), #name
            (DIM_SCHOOL[5], payload.get("data", {}).get("attributes", {}).get("corporate_id", None)), #corporate_id nao achei
            (DIM_SCHOOL[6], payload.get("data", {}).get("attributes", {}).get("corporate_name", None)), #corporate_name nao achei
            (DIM_SCHOOL[7], payload.get("data", {}).get("attributes", {}).get("active", None)), #active_status
            (DIM_SCHOOL[8], payload.get("data", {}).get("attributes", {}).get("demo", None)) #demo_status
        ],
        DIM_SCHOOL
    )
    
    # processed['gold.dim_school_address'] = build_processed_record(
    #     [
    #         (DIM_SCHOOL_ADDRESS[0], payload.get("dt_load", None)),
    #         (DIM_SCHOOL_ADDRESS[1], payload.get("full_address", None)),
    #         (DIM_SCHOOL_ADDRESS[2], payload.get("school_address_id", None)),
    #         (DIM_SCHOOL_ADDRESS[3], payload.get("building_site_number", None)),
    #         (DIM_SCHOOL_ADDRESS[4], payload.get("neighborhood", None)),
    #         (DIM_SCHOOL_ADDRESS[5], payload.get("city", None)),
    #         (DIM_SCHOOL_ADDRESS[6], payload.get("postal_code", None)),
    #         (DIM_SCHOOL_ADDRESS[7], payload.get("address_type", None))
    #     ],
    #     DIM_SCHOOL_ADDRESS
    # )
    
    # Substituir o código atual pela abordagem com loop
    processed['gold.dim_school_address'] = []  # Inicializa como uma lista vazia
    
    # Verifica se addressList existe e contém itens
    if "addressList" in payload.get("data", {}).get("attributes", {}) and payload.get("data", {}).get("attributes", {}).get("addressList"):
        # Loop através de cada endereço na addressList
        for idx, address_item in enumerate(payload.get("data", {}).get("attributes", {}).get("addressList", [])):
            # Constrói um registro processado para cada endereço na lista
            full_address = address_item.get("city", None) + address_item.get("neighborhood", None) + address_item.get("street", {}).get("line1", None) + address_item.get("postalCode")
            
            address_record = build_processed_record(
            [
                (DIM_SCHOOL_ADDRESS[1], full_address),
                (DIM_SCHOOL_ADDRESS[2], school_id),
                (DIM_SCHOOL_ADDRESS[3], address_item.get("buildingSiteNumber", None)),
                (DIM_SCHOOL_ADDRESS[4], address_item.get("neighborhood", None)),
                (DIM_SCHOOL_ADDRESS[5], address_item.get("city", None)),
                (DIM_SCHOOL_ADDRESS[6], address_item.get("postalCode", None)),
                (DIM_SCHOOL_ADDRESS[7], address_item.get("addressType", None))
            ],
            DIM_SCHOOL_ADDRESS
            )
            # Adiciona o endereço processado à lista de endereços
            processed['gold.dim_school_address'].append(address_record)
    else:
        # Se não houver addressList, cria um registro vazio ou com valores padrão
        address_record = build_processed_record(
        [
            (DIM_SCHOOL_ADDRESS[1], None),
            (DIM_SCHOOL_ADDRESS[2], school_id),
            (DIM_SCHOOL_ADDRESS[3], None),
            (DIM_SCHOOL_ADDRESS[4], None),
            (DIM_SCHOOL_ADDRESS[5], None),
            (DIM_SCHOOL_ADDRESS[6], None),
            (DIM_SCHOOL_ADDRESS[7], None)
        ],
        DIM_SCHOOL_ADDRESS
        )
        processed['gold.dim_school_address'].append(address_record)
    
    
    # processed['gold.dim_school_phone'] = build_processed_record(
    #     [
    #         (DIM_SCHOOL_PHONE[0], payload.get("school_phone_id", None)),
    #         (DIM_SCHOOL_PHONE[1], payload.get("phone_number", None)),
    #         (DIM_SCHOOL_PHONE[2], payload.get("phone_type", None))
    #     ],
    #     DIM_SCHOOL_PHONE
    # )
    
    processed['gold.dim_school_phone'] = []  # Inicializa como uma lista vazia

    # Verifica se phoneNumberList existe e contém itens
    if "phoneNumberList" in payload.get("data", {}).get("attributes", {}) and payload.get("data", {}).get("attributes", {}).get("phoneNumberList"):
        # Loop através de cada número de telefone na phoneNumberList
        for phone_item in payload.get("data", {}).get("attributes", {}).get("phoneNumberList", []):
            # Extrai os dados do telefone
            phone_data = phone_item.get("phoneNumber", {})
            
            # Constrói um registro processado para cada telefone na lista
            phone_record = build_processed_record(
            [
                (DIM_SCHOOL_PHONE[0], school_id),
                (DIM_SCHOOL_PHONE[1], phone_data.get("number", None)),
                (DIM_SCHOOL_PHONE[2], phone_data.get("phoneNumberType", None))
            ],
            DIM_SCHOOL_PHONE
            )
            # Adiciona o telefone processado à lista
            processed['gold.dim_school_phone'].append(phone_record)
    else:
        # Se não houver phoneNumberList, cria um registro vazio ou com valores padrão
        phone_record = build_processed_record(
        [
            (DIM_SCHOOL_PHONE[0], school_id),
            (DIM_SCHOOL_PHONE[1], None),
            (DIM_SCHOOL_PHONE[2], None)
        ],
        DIM_SCHOOL_PHONE
        )
        processed['gold.dim_school_phone'].append(phone_record)
    
    # Armazena a referência para DIM_SCHOOL_PHONE
    
    # processed['gold.dim_school_email'] = build_processed_record(
    #     [
    #         (DIM_SCHOOL_EMAIL[0], payload.get("school_email_id", None)),
    #         (DIM_SCHOOL_EMAIL[1], payload.get("email", None))
    #     ],
    #     DIM_SCHOOL_EMAIL
    # )
    processed['gold.dim_school_email'] = []  # Inicializa como uma lista vazia

    # Verifica se schoolEmailList existe e contém itens
    if "schoolEmailList" in payload.get("data", {}).get("attributes", {}) and payload.get("data", {}).get("attributes", {}).get("schoolEmailList"):
        # Loop através de cada email na schoolEmailList
        for email_item in payload.get("data", {}).get("attributes", {}).get("schoolEmailList", []):
            # Extrai os dados do email
            email_data = email_item.get("schoolEmail", {})
            
            # Constrói um registro processado para cada email na lista
            email_record = build_processed_record(
            [
                (DIM_SCHOOL_EMAIL[0], school_id),
                (DIM_SCHOOL_EMAIL[1], email_data.get("email", None))
            ],
            DIM_SCHOOL_EMAIL
            )
            # Adiciona o email processado à lista
            processed['gold.dim_school_email'].append(email_record)
    else:
        # Se não houver schoolEmailList, cria um registro vazio ou com valores padrão
        email_record = build_processed_record(
        [
            (DIM_SCHOOL_EMAIL[0], school_id),
            (DIM_SCHOOL_EMAIL[1], None)
        ],
        DIM_SCHOOL_EMAIL
        )
        processed['gold.dim_school_email'].append(email_record)
    
    # Armazena a referência para DIM_SCHOOL_EMAIL

    return processed

def process_user_event(payload):
    """
    Processa dados para a tabela gold.dim_user.
    """
    processed = {}
    name_obj = payload.get("data", {}).get("attributes", {}).get("name", {})
    full_name = name_obj.get("firstName", "") + " " + name_obj.get("middleName", "") + " " + name_obj.get("lastName", "")
    user_id = payload.get("data", {}).get("attributes", {}).get("refId", None)

    processed['gold.dim_user'] = build_processed_record(
        [
            (DIM_USER[1], payload.get("data", {}).get("attributes", {}).get("refId", None)), #user_id
            (DIM_USER[2], payload.get("data", {}).get("attributes", {}).get("country", {}).get("refId", None)),#country_id
            (DIM_USER[3], payload.get("data", {}).get("attributes", {}).get("language", {}).get("refId", None)), #language_code
            (DIM_USER[4], full_name), #full_name
            (DIM_USER[5], name_obj.get("firstName", "")), #first_name
            (DIM_USER[6], name_obj.get("middleName", "")), #second_name
            (DIM_USER[7], name_obj.get("lastName", "")), #last_name
            (DIM_USER[8], payload.get("data", {}).get("attributes", {}).get("birthDate", None)), #birth_date
            (DIM_USER[9], payload.get("data", {}).get("attributes", {}).get("sex", None)), #gender
            (DIM_USER[10], payload.get("data", {}).get("attributes", {}).get("fiscal_id", None)), #fiscal_id nao achei
            (DIM_USER[11], payload.get("data", {}).get("attributes", {}).get("oficialId", None)), #official_id
            (DIM_USER[12], payload.get("data", {}).get("attributes", {}).get("uno_user_name", None)), #uno_user_name nao achei
            (DIM_USER[13], payload.get("data", {}).get("attributes", {}).get("active_status", None)) #active_status nao achei
        ],
        DIM_USER
    )
    
    # processed['gold.dim_user_phone'] = build_processed_record(
    #     [
    #         (DIM_USER_PHONE[0], payload.get("dt_load", None)),
    #         (DIM_USER_PHONE[1], payload.get("user_phone_id", None)),
    #         (DIM_USER_PHONE[2], payload.get("phone_number", None)),
    #         (DIM_USER_PHONE[3], payload.get("phone_type", None))
    #     ],
    #     DIM_USER_PHONE
    # )
    
    processed['gold.dim_user_phone'] = []  # Inicializa como uma lista vazia

    # Verifica se phoneNumberList existe e contém itens
    user_phone_list = payload.get("data", {}).get("attributes", {}).get("phoneNumberList", [])
    if user_phone_list:
        # Loop através de cada número de telefone na phoneNumberList
        for phone_item in user_phone_list:
            # Extrai os dados do telefone
            phone_data = phone_item.get("phoneNumber", {})
            
            # Constrói um registro processado para cada telefone na lista
            phone_record = build_processed_record(
            [
                (DIM_USER_PHONE[1], user_id),
                (DIM_USER_PHONE[2], phone_data.get("number", None)),
                (DIM_USER_PHONE[3], phone_data.get("phoneNumberType", None))
            ],
            DIM_USER_PHONE
            )
            # Adiciona o telefone processado à lista
            processed['gold.dim_user_phone'].append(phone_record)
    else:
        # Se não houver phoneNumberList, cria um registro vazio ou com valores padrão
        phone_record = build_processed_record(
        [
            (DIM_USER_PHONE[1], user_id),
            (DIM_USER_PHONE[2], None),
            (DIM_USER_PHONE[3], None)
        ],
        DIM_USER_PHONE
        )
        processed['gold.dim_user_phone'].append(phone_record)
    
    # processed['gold.dim_user_email'] = build_processed_record(
    #     [
    #         (DIM_USER_EMAIL[0], payload.get("dt_load", None)),
    #         (DIM_USER_EMAIL[1], payload.get("user_email_id", None)),
    #         (DIM_USER_EMAIL[2], payload.get("email", None))
    #     ],
    #     DIM_USER_EMAIL
    # )
    
    user_email_list = payload.get("data", {}).get("attributes", {}).get("userEmailList", [])
    if user_email_list:
        # Loop através de cada email na lista
        for email_item in user_email_list:
            # Extrai os dados do email
            email_data = email_item.get("userEmail", {})
            
            # Constrói um registro processado para cada email na lista
            email_record = build_processed_record(
            [
                (DIM_USER_EMAIL[1], user_id),
                (DIM_USER_EMAIL[2], email_data.get("email", None))
            ],
            DIM_USER_EMAIL
            )
            # Adiciona o email processado à lista
            processed['gold.dim_user_email'].append(email_record)
    else:
        # Se não houver a lista de emails, cria um registro vazio ou com valores padrão
        email_record = build_processed_record(
        [
            (DIM_USER_EMAIL[1], user_id),
            (DIM_USER_EMAIL[2], None)
        ],
        DIM_USER_EMAIL
        )
        processed['gold.dim_user_email'].append(email_record)
    
    # processed['gold.dim_user_address'] = build_processed_record(
    #     [
    #         (DIM_USER_ADDRESS[0], payload.get("dt_load", None)),
    #         (DIM_USER_ADDRESS[1], payload.get("user_address_id", None)),
    #         (DIM_USER_ADDRESS[2], payload.get("address_type", None)),
    #         (DIM_USER_ADDRESS[3], payload.get("full_address", None)),
    #         (DIM_USER_ADDRESS[4], payload.get("city", None)),
    #         (DIM_USER_ADDRESS[5], payload.get("neighborhood", None)),
    #         (DIM_USER_ADDRESS[6], payload.get("postal_code", None)),
    #         (DIM_USER_ADDRESS[7], payload.get("building_site_number", None)),
    #         (DIM_USER_ADDRESS[8], payload.get("country_id", None))
    #     ],
    #     DIM_USER_ADDRESS
    # )
    
    user_address_list = payload.get("data", {}).get("attributes", {}).get("addressList", [])
    country_id = payload.get("data", {}).get("attributes", {}).get("country", {}).get("refId", None)
    
    if user_address_list:
        # Loop através de cada endereço na addressList
        for address_item in user_address_list:
            # Extrai os dados do endereço
            address_data = address_item.get("address", {})
            
            full_address = address_data.get("city", None) + " " + address_data.get("neighborhood", None) + " " + address_data.get("street", {}).get("line1", None) 
            
            # Constrói um registro processado para cada endereço na lista
            address_record = build_processed_record(
            [
                (DIM_USER_ADDRESS[1], user_id),
                (DIM_USER_ADDRESS[2], address_data.get("addressType", None))
                (DIM_USER_ADDRESS[3], full_address),
                (DIM_USER_ADDRESS[4], address_data.get("city", None)),
                (DIM_USER_ADDRESS[5], address_data.get("neighborhood", None)),
                (DIM_USER_ADDRESS[6], address_data.get("postalCode", None)),
                (DIM_USER_ADDRESS[7], address_data.get("buildingSiteNumber", None)),
                (DIM_USER_ADDRESS[8], country_id)
            ],
            DIM_USER_ADDRESS
            )
            # Adiciona o endereço processado à lista
            processed['gold.dim_user_address'].append(address_record)
    else:
        # Se não houver addressList, cria um registro vazio ou com valores padrão
        address_record = build_processed_record(
        [
            (DIM_USER_ADDRESS[1], user_id),
            (DIM_USER_ADDRESS[2], None)
            (DIM_USER_ADDRESS[3], None),
            (DIM_USER_ADDRESS[4], None),
            (DIM_USER_ADDRESS[5], None),
            (DIM_USER_ADDRESS[6], None),
            (DIM_USER_ADDRESS[7], None),
            (DIM_USER_ADDRESS[8], country_id)
        ],
        DIM_USER_ADDRESS
        )
        processed['gold.dim_user_address'].append(address_record)
    
    return processed

def process_user_group_event(payload):
    """
    Processa dados para a tabela gold.dim_user_group.
    """
    processed = {}

    processed['gold.dim_user_group'] = build_processed_record(
        [
            (DIM_USER_GROUP[1], payload.get("data", {}).get("attributes", {}).get("group", {}).get("refId", None)),#group_id
            (DIM_USER_GROUP[2], payload.get("data", {}).get("attributes", {}).get("grade", {}).get("refId", None)),#grade_id
            (DIM_USER_GROUP[3], payload.get("data", {}).get("attributes", {}).get("unoSchool", {}).get("refId", None)),#school_id
            (DIM_USER_GROUP[4], payload.get("data", {}).get("attributes", {}).get("schoolLevel", {}).get("refId", None)),#school_level_id
            (DIM_USER_GROUP[5], payload.get("data", {}).get("attributes", {}).get("schoolStageSeason", {}).get("refId", None)),#school_stage_season_id
            (DIM_USER_GROUP[6], payload.get("data", {}).get("attributes", {}).get("schoolLevelSession", {}).get("refId", None)),#school_level_session_id
            (DIM_USER_GROUP[7], payload.get("data", {}).get("attributes", {}).get("session", {}).get("refId", None)),#session_id
            (DIM_USER_GROUP[8], payload.get("data", {}).get("attributes", {}).get("userList", {}).get("personRefId", None)),#person_id
            (DIM_USER_GROUP[9], payload.get("data", {}).get("attributes", {}).get("userList", {}).get("roleRefId", None)),#role_id
            (DIM_USER_GROUP[10], payload.get("data", {}).get("attributes", {}).get("userList", {}).get("personRolRefId", None)),#person_rol_id
            (DIM_USER_GROUP[11], payload.get("data", {}).get("attributes", {}).get("userList", {}).get("active", None))#active_status
        ],
        DIM_USER_GROUP
    )

    return processed

def process_user_role_event(payload):
    """
    Processa dados para a tabela gold.dim_user_role.
    """
    processed = {}

    processed['gold.dim_user_role'] = build_processed_record(
        [
            (DIM_USER_ROLE[1], payload.get("data", {}).get("attributes", {}).get("refId", None)),#user_role_id
            (DIM_USER_ROLE[2], payload.get("data", {}).get("attributes", {}).get("unoStudent", {}).get("refId", None)),#student_id
            (DIM_USER_ROLE[3], payload.get("data", {}).get("attributes", {}).get("user_login", None)),#user_login nao achei
            (DIM_USER_ROLE[4], payload.get("data", {}).get("attributes", {}).get("role", {}).get("refId", None)),#role_id
            (DIM_USER_ROLE[5], payload.get("data", {}).get("attributes", {}).get("active", None)),#status_role
            (DIM_USER_ROLE[6], payload.get("data", {}).get("attributes", {}).get("schoolLevelSession", {}).get("schoolLevel", {}).get("refId", None)),#school_level_id
            (DIM_USER_ROLE[7], payload.get("data", {}).get("attributes", {}).get("schoolLevelSession", {}).get("refId", None)),#school_id
            (DIM_USER_ROLE[8], payload.get("data", {}).get("attributes", {}).get("unoStudent", {}).get("schoolGradeGroup", {}).get("unoSchool", {}).get("refId", None)),#company_id 
            (DIM_USER_ROLE[9], payload.get("data", {}).get("attributes", {}).get("schoolLevelSession", {}).get("session", {}).get("refId", None)),#school_level_sessions_id
            (DIM_USER_ROLE[10], payload.get("data", {}).get("attributes", {}).get("unoStudent", {}).get("schoolGradeGroup", {}).get("grade", {}).get("refId", None)),#grade_id
            (DIM_USER_ROLE[11], payload.get("data", {}).get("attributes", {}).get("unoStudent", {}).get("schoolGradeGroup", {}).get("group", {}).get("refId", None)),#group_id
            (DIM_USER_ROLE[12], payload.get("data", {}).get("attributes", {}).get("unoStudent", {}).get("schoolGradeGroup", {}).get("refId", None)),#school_grade_group_id
            (DIM_USER_ROLE[13], payload.get("data", {}).get("attributes", {}).get("status_user_login", None)),#status_user_login nao achei
            (DIM_USER_ROLE[14], payload.get("data", {}).get("attributes", {}).get("unoStudent", {}).get("person", {}).get("refId", None))#person_id
        ],
        DIM_USER_ROLE
    )

    return processed

def process_user_school_class_event(payload):
    """
    Processa dados para a tabela gold.dim_class_participant.
    """
    processed = {}

    # processed['gold.dim_class_participant'] = build_processed_record(
    #     [
    #         (DIM_CLASS_PARTICIPANT[1], payload.get("user_id", None)),
    #         (DIM_CLASS_PARTICIPANT[2], payload.get("role_id", None)),
    #         (DIM_CLASS_PARTICIPANT[3], payload.get("section_id", None)),
    #         (DIM_CLASS_PARTICIPANT[4], payload.get("person_role_id", None)),
    #         (DIM_CLASS_PARTICIPANT[5], payload.get("active_status", None)),
    #         (DIM_CLASS_PARTICIPANT[6], payload.get("dt_year", None))
    #     ],
    #     DIM_CLASS_PARTICIPANT
    # )
    
    processed['gold.dim_class_participant'] = []
    
    # Verifica se userList existe e contém itens
    user_list = payload.get("data", {}).get("attributes", {}).get("userList", [])
    
    if user_list:
        # Loop através de cada usuário na userList
        for user_item in user_list:
            # Constrói um registro processado para cada usuário na lista
            participant_record = build_processed_record(
            [
                (DIM_CLASS_PARTICIPANT[1], user_item.get("personRefId", None)),  # user_id
                (DIM_CLASS_PARTICIPANT[2], user_item.get("roleRefId", None)),  # role_id
                (DIM_CLASS_PARTICIPANT[3], payload.get("data", {}).get("attributes", {}).get("schoolClass", {}).get("refId", None)),  # section_id
                (DIM_CLASS_PARTICIPANT[4], user_item.get("personRolRefId", None)),  # person_role_id
                (DIM_CLASS_PARTICIPANT[5], user_item.get("active", None)),  # active_status
                (DIM_CLASS_PARTICIPANT[6], payload.get("dt_year", None)),  # dt_year nao achei
                (DIM_CLASS_PARTICIPANT[6], payload.get("data", {}).get("attributes", {}).get("unoSchool", {}).get("refId", None)),#school_id
                (DIM_CLASS_PARTICIPANT[6], payload.get("data", {}).get("attributes", {}).get("schoolLevel", {}).get("refId", None)),#school_level_id
                (DIM_CLASS_PARTICIPANT[6], payload.get("data", {}).get("attributes", {}).get("schoolStageSeason", {}).get("refId", None)),#school_stage_season_id
                (DIM_CLASS_PARTICIPANT[6], payload.get("data", {}).get("attributes", {}).get("session", {}).get("refId", None)),#session_id
            ],
            DIM_CLASS_PARTICIPANT
            )
            # Adiciona o participante processado à lista
            processed['gold.dim_class_participant'].append(participant_record)
    else:
        # Se não houver userList, cria um registro vazio ou com valores padrão
        participant_record = build_processed_record(
        [
            (DIM_CLASS_PARTICIPANT[1], None),  # user_id
            (DIM_CLASS_PARTICIPANT[2], None),  # role_id
            (DIM_CLASS_PARTICIPANT[3], payload.get("data", {}).get("attributes", {}).get("schoolClass", {}).get("refId", None)),  # section_id
            (DIM_CLASS_PARTICIPANT[4], None),  # person_role_id
            (DIM_CLASS_PARTICIPANT[5], None),  # active_status
            (DIM_CLASS_PARTICIPANT[6], payload.get("dt_year", None)),  # dt_year nao achei
            (DIM_CLASS_PARTICIPANT[6], payload.get("data", {}).get("attributes", {}).get("unoSchool", {}).get("refId", None)),#school_id
            (DIM_CLASS_PARTICIPANT[6], payload.get("data", {}).get("attributes", {}).get("schoolLevel", {}).get("refId", None)),#school_level_id
            (DIM_CLASS_PARTICIPANT[6], payload.get("data", {}).get("attributes", {}).get("schoolStageSeason", {}).get("refId", None)),#school_stage_season_id
            (DIM_CLASS_PARTICIPANT[6], payload.get("data", {}).get("attributes", {}).get("session", {}).get("refId", None)),#session_id
        ],
        DIM_CLASS_PARTICIPANT
        )
        processed['gold.dim_class_participant'].append(participant_record)
    
    # Armazena a referência para DIM_CLASS_PARTICIPANT

    return processed