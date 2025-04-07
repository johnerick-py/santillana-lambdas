"""
Módulo que contém os processadores específicos para cada tabela definida em table_schema.py.
"""
import time
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
    Processa dados para as tabelas relacionadas a escolas de forma otimizada.
    """
    start_time = time.time()
    logger.info("Iniciando processamento de UNOSCHOOL event")
    
    processed = {}
    
    try:
        # Extrai dados básicos da escola primeiro
        school_id = payload.get("data", {}).get("attributes", {}).get("refId", None)
        
        logger.info(f"Processando escola: {school_id}")
        
        # Processa a tabela principal da escola
        school_record = build_processed_record(
            [
                (DIM_SCHOOL[1], school_id),
                (DIM_SCHOOL[2], payload.get("data", {}).get("attributes", {}).get("country", {}).get("refId", None)),
                (DIM_SCHOOL[3], payload.get("data", {}).get("attributes", {}).get("schoolIdSystem", None)),
                (DIM_SCHOOL[4], payload.get("data", {}).get("attributes", {}).get("name", None)),
                (DIM_SCHOOL[5], payload.get("data", {}).get("attributes", {}).get("corporate_id", None)),
                (DIM_SCHOOL[6], payload.get("data", {}).get("attributes", {}).get("corporate_name", None)),
                (DIM_SCHOOL[7], payload.get("data", {}).get("attributes", {}).get("active", None)),
                (DIM_SCHOOL[8], payload.get("data", {}).get("attributes", {}).get("demo", None))
            ],
            DIM_SCHOOL
        )
        processed['gold.dim_school'] = school_record
        logger.info(f"Tabela principal de escola processada em {time.time() - start_time:.2f}s")
        
        # Inicializa as listas
        processed['gold.dim_school_address'] = []
        processed['gold.dim_school_phone'] = []
        processed['gold.dim_school_email'] = []
        
        # Processa endereços com limite
        address_start = time.time()
        address_list = payload.get("data", {}).get("attributes", {}).get("addressList", [])
        
        if address_list and len(address_list) > 0:
            # Processa apenas o primeiro endereço
            address_item = address_list[0]
            address_data = address_item.get("address", {})
            
            # Simplifica a construção do endereço completo
            street = address_data.get("street", {}).get("line1", "")
            city = address_data.get("city", "")
            neighborhood = address_data.get("neighborhood", "")
            postal_code = address_data.get("postalCode", "")
            full_address = f"{street}, {neighborhood}, {city}, {postal_code}".strip(", ")
            
            address_record = build_processed_record(
                [
                    (DIM_SCHOOL_ADDRESS[1], full_address),
                    (DIM_SCHOOL_ADDRESS[2], school_id),
                    (DIM_SCHOOL_ADDRESS[3], address_data.get("buildingSiteNumber", None)),
                    (DIM_SCHOOL_ADDRESS[4], neighborhood),
                    (DIM_SCHOOL_ADDRESS[5], city),
                    (DIM_SCHOOL_ADDRESS[6], postal_code),
                    (DIM_SCHOOL_ADDRESS[7], address_data.get("addressType", None))
                ],
                DIM_SCHOOL_ADDRESS
            )
            processed['gold.dim_school_address'].append(address_record)
        else:
            # Registro vazio
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
        
        logger.info(f"Endereços de escola processados em {time.time() - address_start:.2f}s")
        
        # Processa telefones com limite
        phone_start = time.time()
        phone_list = payload.get("data", {}).get("attributes", {}).get("phoneNumberList", [])
        
        if phone_list and len(phone_list) > 0:
            # Processa apenas o primeiro telefone
            phone_item = phone_list[0]
            phone_data = phone_item.get("phoneNumber", {})
            
            phone_record = build_processed_record(
                [
                    (DIM_SCHOOL_PHONE[0], school_id),
                    (DIM_SCHOOL_PHONE[1], phone_data.get("number", None)),
                    (DIM_SCHOOL_PHONE[2], phone_data.get("phoneNumberType", None))
                ],
                DIM_SCHOOL_PHONE
            )
            processed['gold.dim_school_phone'].append(phone_record)
        else:
            # Registro vazio
            phone_record = build_processed_record(
                [
                    (DIM_SCHOOL_PHONE[0], school_id),
                    (DIM_SCHOOL_PHONE[1], None),
                    (DIM_SCHOOL_PHONE[2], None)
                ],
                DIM_SCHOOL_PHONE
            )
            processed['gold.dim_school_phone'].append(phone_record)
        
        logger.info(f"Telefones de escola processados em {time.time() - phone_start:.2f}s")
        
        # Processa emails com limite
        email_start = time.time()
        email_list = payload.get("data", {}).get("attributes", {}).get("schoolEmailList", [])
        
        if email_list and len(email_list) > 0:
            # Processa apenas o primeiro email
            email_item = email_list[0]
            email_data = email_item.get("schoolEmail", {})
            
            email_record = build_processed_record(
                [
                    (DIM_SCHOOL_EMAIL[0], school_id),
                    (DIM_SCHOOL_EMAIL[1], email_data.get("email", None))
                ],
                DIM_SCHOOL_EMAIL
            )
            processed['gold.dim_school_email'].append(email_record)
        else:
            # Registro vazio
            email_record = build_processed_record(
                [
                    (DIM_SCHOOL_EMAIL[0], school_id),
                    (DIM_SCHOOL_EMAIL[1], None)
                ],
                DIM_SCHOOL_EMAIL
            )
            processed['gold.dim_school_email'].append(email_record)
        
        logger.info(f"Emails de escola processados em {time.time() - email_start:.2f}s")
        logger.info(f"Processamento total de UNOSCHOOL concluído em {time.time() - start_time:.2f}s")
        
        return processed
        
    except Exception as e:
        logger.error(f"Erro durante o processamento de UNOSCHOOL: {e}", exc_info=True)
        # Em caso de erro, retorna estrutura mínima
        return {
            'gold.dim_school': build_processed_record(
                [(DIM_SCHOOL[1], school_id)],
                DIM_SCHOOL
            ),
            'gold.dim_school_address': [],
            'gold.dim_school_phone': [],
            'gold.dim_school_email': []
        }


# def process_uno_school_event(payload):
#     """
#     Processa dados para a tabela gold.dim_school.
#     """
#     processed = {}
    
#     school_id = payload.get("data", {}).get("attributes", {}).get("school_id", None)

#     processed['gold.dim_school'] = build_processed_record(
#         [
#             (DIM_SCHOOL[1], school_id), #school_id
#             (DIM_SCHOOL[2], payload.get("data", {}).get("attributes", {}).get("country", {}).get("refId", None)), #country_id
#             (DIM_SCHOOL[3], payload.get("data", {}).get("attributes", {}).get("schoolIdSystem", None)), #school_id_system
#             (DIM_SCHOOL[4], payload.get("data", {}).get("attributes", {}).get("name", None)), #name
#             (DIM_SCHOOL[5], payload.get("data", {}).get("attributes", {}).get("corporate_id", None)), #corporate_id nao achei
#             (DIM_SCHOOL[6], payload.get("data", {}).get("attributes", {}).get("corporate_name", None)), #corporate_name nao achei
#             (DIM_SCHOOL[7], payload.get("data", {}).get("attributes", {}).get("active", None)), #active_status
#             (DIM_SCHOOL[8], payload.get("data", {}).get("attributes", {}).get("demo", None)) #demo_status
#         ],
#         DIM_SCHOOL
#     )
    
#     # processed['gold.dim_school_address'] = build_processed_record(
#     #     [
#     #         (DIM_SCHOOL_ADDRESS[0], payload.get("dt_load", None)),
#     #         (DIM_SCHOOL_ADDRESS[1], payload.get("full_address", None)),
#     #         (DIM_SCHOOL_ADDRESS[2], payload.get("school_address_id", None)),
#     #         (DIM_SCHOOL_ADDRESS[3], payload.get("building_site_number", None)),
#     #         (DIM_SCHOOL_ADDRESS[4], payload.get("neighborhood", None)),
#     #         (DIM_SCHOOL_ADDRESS[5], payload.get("city", None)),
#     #         (DIM_SCHOOL_ADDRESS[6], payload.get("postal_code", None)),
#     #         (DIM_SCHOOL_ADDRESS[7], payload.get("address_type", None))
#     #     ],
#     #     DIM_SCHOOL_ADDRESS
#     # )
    
#     # Substituir o código atual pela abordagem com loop
#     processed['gold.dim_school_address'] = []  # Inicializa como uma lista vazia

#     school_address_list = payload.get("data", {}).get("attributes", {}).get("addressList", [])

#     if school_address_list:
#         for address_item in school_address_list:
#             full_address = address_item.get("city", "") + " " +  address_item.get("neighborhood", "") + " " + address_item.get("street", {}).get("line1", "") + " " + address_item.get("postalCode", "")
            
#             address_record = build_processed_record(
#             [
#                 (DIM_SCHOOL_ADDRESS[1], full_address),
#                 (DIM_SCHOOL_ADDRESS[2], school_id),
#                 (DIM_SCHOOL_ADDRESS[3], address_item.get("buildingSiteNumber", None)),
#                 (DIM_SCHOOL_ADDRESS[4], address_item.get("neighborhood", None)),
#                 (DIM_SCHOOL_ADDRESS[5], address_item.get("city", None)),
#                 (DIM_SCHOOL_ADDRESS[6], address_item.get("postalCode", None)),
#                 (DIM_SCHOOL_ADDRESS[7], address_item.get("addressType", None))
#             ],
#             DIM_SCHOOL_ADDRESS
#             )
#             # Adiciona o endereço processado à lista de endereços
#             processed['gold.dim_school_address'].append(address_record)
#     else:
#         # Se não houver addressList, cria um registro vazio ou com valores padrão
#         address_record = build_processed_record(
#         [
#             (DIM_SCHOOL_ADDRESS[1], None),
#             (DIM_SCHOOL_ADDRESS[2], school_id),
#             (DIM_SCHOOL_ADDRESS[3], None),
#             (DIM_SCHOOL_ADDRESS[4], None),
#             (DIM_SCHOOL_ADDRESS[5], None),
#             (DIM_SCHOOL_ADDRESS[6], None),
#             (DIM_SCHOOL_ADDRESS[7], None)
#         ],
#         DIM_SCHOOL_ADDRESS
#         )
#         processed['gold.dim_school_address'].append(address_record)
    
    
#     # processed['gold.dim_school_phone'] = build_processed_record(
#     #     [
#     #         (DIM_SCHOOL_PHONE[0], payload.get("school_phone_id", None)),
#     #         (DIM_SCHOOL_PHONE[1], payload.get("phone_number", None)),
#     #         (DIM_SCHOOL_PHONE[2], payload.get("phone_type", None))
#     #     ],
#     #     DIM_SCHOOL_PHONE
#     # )

    
#     processed['gold.dim_school_phone'] = []  # Inicializa como uma lista vazia

#     school_phone_list = payload.get("data", {}).get("attributes", {}).get("phoneNumberList", [])

#     if school_phone_list:
#         for phone_item in school_phone_list:
#             # Extrai os dados do telefone
#             phone_data = phone_item.get("phoneNumber", {})
            
#             # Constrói um registro processado para cada telefone na lista
#             phone_record = build_processed_record(
#             [
#                 (DIM_SCHOOL_PHONE[0], school_id),
#                 (DIM_SCHOOL_PHONE[1], phone_data.get("number", None)),
#                 (DIM_SCHOOL_PHONE[2], phone_data.get("phoneNumberType", None))
#             ],
#             DIM_SCHOOL_PHONE
#             )
#             # Adiciona o telefone processado à lista
#             processed['gold.dim_school_phone'].append(phone_record)
#     else:
#         # Se não houver phoneNumberList, cria um registro vazio ou com valores padrão
#         phone_record = build_processed_record(
#         [
#             (DIM_SCHOOL_PHONE[0], school_id),
#             (DIM_SCHOOL_PHONE[1], None),
#             (DIM_SCHOOL_PHONE[2], None)
#         ],
#         DIM_SCHOOL_PHONE
#         )
#         processed['gold.dim_school_phone'].append(phone_record)
    
#     # Armazena a referência para DIM_SCHOOL_PHONE
    
#     # processed['gold.dim_school_email'] = build_processed_record(
#     #     [
#     #         (DIM_SCHOOL_EMAIL[0], payload.get("school_email_id", None)),
#     #         (DIM_SCHOOL_EMAIL[1], payload.get("email", None))
#     #     ],
#     #     DIM_SCHOOL_EMAIL
#     # )
#     processed['gold.dim_school_email'] = []  # Inicializa como uma lista vazia


#     school_email_list = payload.get("data", {}).get("attributes", {}).get("schoolEmailList", [])

#     if school_email_list:
#         for email_item in school_email_list:
#             # Extrai os dados do email
#             email_data = email_item.get("schoolEmail", {})
            
#             # Constrói um registro processado para cada email na lista
#             email_record = build_processed_record(
#             [
#                 (DIM_SCHOOL_EMAIL[0], school_id),
#                 (DIM_SCHOOL_EMAIL[1], email_data.get("email", None))
#             ],
#             DIM_SCHOOL_EMAIL
#             )
#             # Adiciona o email processado à lista
#             processed['gold.dim_school_email'].append(email_record)
#     else:
#         # Se não houver schoolEmailList, cria um registro vazio ou com valores padrão
#         email_record = build_processed_record(
#         [
#             (DIM_SCHOOL_EMAIL[0], school_id),
#             (DIM_SCHOOL_EMAIL[1], None)
#         ],
#         DIM_SCHOOL_EMAIL
#         )
#         processed['gold.dim_school_email'].append(email_record)
    
#     # Armazena a referência para DIM_SCHOOL_EMAIL

#     return processed

def process_user_event(payload):
    """
    Processa dados para a tabela gold.dim_user de forma otimizada.
    """
    start_time = time.time()
    logger.info("Iniciando processamento de USER event")
    
    processed = {}
    
    try:
        # Extrai dados básicos do usuário primeiro - operação rápida
        user_id = payload.get("data", {}).get("attributes", {}).get("refId", None)
        country_id = payload.get("data", {}).get("attributes", {}).get("country", {}).get("refId", None)
        name_obj = payload.get("data", {}).get("attributes", {}).get("name", {})
        
        # Constrói o nome completo de forma eficiente
        first_name = name_obj.get("firstName", "")
        middle_name = name_obj.get("middleName", "")
        last_name = name_obj.get("lastName", "")
        full_name = " ".join(filter(None, [first_name, middle_name, last_name]))
        
        logger.info(f"Processando usuário: {user_id} - {full_name}")
        
        # Processa a tabela principal do usuário
        user_record = build_processed_record(
            [
                (DIM_USER[1], user_id),
                (DIM_USER[2], country_id),
                (DIM_USER[3], payload.get("data", {}).get("attributes", {}).get("language", {}).get("refId", None)),
                (DIM_USER[4], full_name),
                (DIM_USER[5], first_name),
                (DIM_USER[6], middle_name),
                (DIM_USER[7], last_name),
                (DIM_USER[8], payload.get("data", {}).get("attributes", {}).get("birthDate", None)),
                (DIM_USER[9], payload.get("data", {}).get("attributes", {}).get("sex", None)),
                (DIM_USER[10], payload.get("data", {}).get("attributes", {}).get("fiscal_id", None)),
                (DIM_USER[11], payload.get("data", {}).get("attributes", {}).get("oficialId", None)),
                (DIM_USER[12], payload.get("data", {}).get("attributes", {}).get("uno_user_name", None)),
                (DIM_USER[13], payload.get("data", {}).get("attributes", {}).get("active_status", None))
            ],
            DIM_USER
        )
        processed['gold.dim_user'] = user_record
        logger.info(f"Tabela principal processada em {time.time() - start_time:.2f}s")
        
        # Inicializa as listas
        processed['gold.dim_user_phone'] = []
        processed['gold.dim_user_email'] = []
        processed['gold.dim_user_address'] = []
        
        # Processa telefones com limite
        phone_start = time.time()
        phone_list = payload.get("data", {}).get("attributes", {}).get("phoneNumberList", [])
        # Limita a 1 telefone para evitar processamento excessivo
        if phone_list and len(phone_list) > 0:
            # Processa apenas o primeiro telefone
            phone_item = phone_list[0]
            phone_data = phone_item.get("phoneNumber", {})
            
            phone_record = build_processed_record(
                [
                    (DIM_USER_PHONE[1], user_id),
                    (DIM_USER_PHONE[2], phone_data.get("number", None)),
                    (DIM_USER_PHONE[3], phone_data.get("phoneNumberType", None))
                ],
                DIM_USER_PHONE
            )
            processed['gold.dim_user_phone'].append(phone_record)
        else:
            # Registro vazio se não houver telefones
            phone_record = build_processed_record(
                [
                    (DIM_USER_PHONE[1], user_id),
                    (DIM_USER_PHONE[2], None),
                    (DIM_USER_PHONE[3], None)
                ],
                DIM_USER_PHONE
            )
            processed['gold.dim_user_phone'].append(phone_record)
        
        logger.info(f"Telefones processados em {time.time() - phone_start:.2f}s")
        
        # Processa emails com limite
        email_start = time.time()
        # Verifica primeiro personEmailList e depois userEmailList
        email_list = (
            payload.get("data", {}).get("attributes", {}).get("personEmailList", []) or 
            payload.get("data", {}).get("attributes", {}).get("userEmailList", [])
        )
        
        if email_list and len(email_list) > 0:
            # Processa apenas o primeiro email
            email_item = email_list[0]
            # Verifica se é personEmail ou userEmail
            email_data = email_item.get("personEmail", email_item.get("userEmail", {}))
            
            email_record = build_processed_record(
                [
                    (DIM_USER_EMAIL[1], user_id),
                    (DIM_USER_EMAIL[2], email_data.get("email", None))
                ],
                DIM_USER_EMAIL
            )
            processed['gold.dim_user_email'].append(email_record)
        else:
            # Registro vazio se não houver emails
            email_record = build_processed_record(
                [
                    (DIM_USER_EMAIL[1], user_id),
                    (DIM_USER_EMAIL[2], None)
                ],
                DIM_USER_EMAIL
            )
            processed['gold.dim_user_email'].append(email_record)
        
        logger.info(f"Emails processados em {time.time() - email_start:.2f}s")
        
        # Processa endereços com limite
        address_start = time.time()
        address_list = payload.get("data", {}).get("attributes", {}).get("addressList", [])
        
        if address_list and len(address_list) > 0:
            # Processa apenas o primeiro endereço
            address_item = address_list[0]
            address_data = address_item.get("address", {})
            
            # Simplifica a construção do endereço completo
            street = address_data.get("street", {}).get("line1", "")
            city = address_data.get("city", "")
            neighborhood = address_data.get("neighborhood", "")
            full_address = f"{street}, {neighborhood}, {city}".strip(", ")
            
            address_record = build_processed_record(
                [
                    (DIM_USER_ADDRESS[1], user_id),
                    (DIM_USER_ADDRESS[2], address_data.get("addressType", None)),
                    (DIM_USER_ADDRESS[3], full_address),
                    (DIM_USER_ADDRESS[4], city),
                    (DIM_USER_ADDRESS[5], neighborhood),
                    (DIM_USER_ADDRESS[6], address_data.get("postalCode", None)),
                    (DIM_USER_ADDRESS[7], address_data.get("buildingSiteNumber", None)),
                    (DIM_USER_ADDRESS[8], country_id)
                ],
                DIM_USER_ADDRESS
            )
            processed['gold.dim_user_address'].append(address_record)
        else:
            # Registro vazio se não houver endereços
            address_record = build_processed_record(
                [
                    (DIM_USER_ADDRESS[1], user_id),
                    (DIM_USER_ADDRESS[2], None),
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
        
        logger.info(f"Endereços processados em {time.time() - address_start:.2f}s")
        logger.info(f"Processamento total de USER concluído em {time.time() - start_time:.2f}s")
        
        return processed
        
    except Exception as e:
        logger.error(f"Erro durante o processamento de USER: {e}", exc_info=True)
        # Em caso de erro, retorna estrutura mínima para evitar falhas em cascata
        return {
            'gold.dim_user': build_processed_record(
                [(DIM_USER[1], payload.get("data", {}).get("attributes", {}).get("refId", None))],
                DIM_USER
            ),
            'gold.dim_user_phone': [],
            'gold.dim_user_email': [],
            'gold.dim_user_address': []
        }

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