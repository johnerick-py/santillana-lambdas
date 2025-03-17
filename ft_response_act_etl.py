import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, lpad, concat, when, max, coalesce, expr
import boto3
import psycopg2
import datetime
import time
import uuid
import os

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("Starting ETL job for student response activity data")

# Redshift connection parameters
REDSHIFT_CONN = {
    'dbname': 'datalake',
    'user': 'admin',
    'password': '61A7tQeXAnXkGx6p',
    'host': 'pas-prod-redshift-workgroup.888577054267.us-east-1.redshift-serverless.amazonaws.com',
    'port': '5439'
}

# S3 temp directory and IAM role for Redshift COPY
S3_TEMP_DIR = 's3://pas-prod-silver/temp/'
S3_TEMP_PREFIX = "student_response_etl/"
REDSHIFT_IAM_ROLE = "arn:aws:iam::888577054267:role/pas-prod-redshift-role"
S3_TEMP_FULL_PATH = f"{S3_TEMP_DIR.rstrip('/')}/{S3_TEMP_PREFIX}"

s3_client = boto3.client('s3')

def get_redshift_connection():
    """Establish a connection to Redshift using psycopg2"""
    try:
        conn = psycopg2.connect(**REDSHIFT_CONN)
        print(f"[{datetime.datetime.now()}] Successfully connected to Redshift")
        return conn
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Error connecting to Redshift: {str(e)}")
        raise

def execute_and_log_sql(conn, sql, description):
    """Execute SQL and log results"""
    try:
        cursor = conn.cursor()
        print(f"[{datetime.datetime.now()}] Executing: {description}")
        print(f"SQL: {sql}")
        cursor.execute(sql)
        conn.commit()
        print(f"[{datetime.datetime.now()}] Successfully executed: {description}")
        cursor.close()
    except Exception as e:
        conn.rollback()
        print(f"[{datetime.datetime.now()}] Error executing {description}: {str(e)}")
        raise

def read_source_tables_from_redshift():
    """Read source tables from Redshift using Spark JDBC connection"""
    jdbc_url = f"jdbc:postgresql://{REDSHIFT_CONN['host']}:{REDSHIFT_CONN['port']}/{REDSHIFT_CONN['dbname']}"
    
    print(f"[{datetime.datetime.now()}] Reading source tables from Redshift")
    
    common_jdbc_options = {
        "url": jdbc_url,
        "user": REDSHIFT_CONN['user'],
        "password": REDSHIFT_CONN['password'],
        "driver": "org.postgresql.Driver"
    }
    
    # Read source tables using Spark JDBC
    verb_answered = spark.read.format("jdbc") \
        .options(**common_jdbc_options, dbtable="(SELECT DISTINCT lrs_id, client_id, statement_id FROM gold.dim_lrs_verb WHERE verb_id = 'http://adlnet.gov/expapi/verbs/answered') AS verb_answered") \
        .load()
    
    dim_lrs_object_http = spark.read.format("jdbc") \
        .options(**common_jdbc_options, dbtable="gold.dim_lrs_object_http") \
        .load()
    
    dim_lrs_result = spark.read.format("jdbc") \
        .options(**common_jdbc_options, dbtable="gold.dim_lrs_result") \
        .load()
    
    dim_lrs_result_scorm = spark.read.format("jdbc") \
        .options(**common_jdbc_options, dbtable="gold.dim_lrs_result_scorm") \
        .load()
    
    dim_activity = spark.read.format("jdbc") \
        .options(**common_jdbc_options, dbtable="gold.dim_activity") \
        .load()
    
    dim_resource = spark.read.format("jdbc") \
        .options(**common_jdbc_options, dbtable="gold.dim_resource") \
        .load()
    
    dim_page = spark.read.format("jdbc") \
        .options(**common_jdbc_options, dbtable="gold.dim_page") \
        .load()
    
    dim_school = spark.read.format("jdbc") \
        .options(**common_jdbc_options, dbtable="gold.dim_school") \
        .load()
    
    dim_school_level_session = spark.read.format("jdbc") \
        .options(**common_jdbc_options, dbtable="gold.dim_school_level_session") \
        .load()
    
    dim_lrs_statement = spark.read.format("jdbc") \
        .options(**common_jdbc_options, dbtable="gold.dim_lrs_statement") \
        .load()
    
    return {
        "verb_answered": verb_answered,
        "dim_lrs_object_http": dim_lrs_object_http,
        "dim_lrs_result": dim_lrs_result,
        "dim_lrs_result_scorm": dim_lrs_result_scorm,
        "dim_activity": dim_activity,
        "dim_resource": dim_resource,
        "dim_page": dim_page,
        "dim_school": dim_school,
        "dim_school_level_session": dim_school_level_session,
        "dim_lrs_statement": dim_lrs_statement
    }

def truncate_target_table(conn):
    """Truncate the target table before loading new data"""
    try:
        cursor = conn.cursor()
        print(f"[{datetime.datetime.now()}] Truncating target table gold.ft_student_response_activity_table")
        cursor.execute("TRUNCATE TABLE gold.ft_student_response_activity_table;")
        conn.commit()
        cursor.close()
        print(f"[{datetime.datetime.now()}] Target table truncated successfully")
    except Exception as e:
        conn.rollback()
        print(f"[{datetime.datetime.now()}] Error truncating target table: {str(e)}")
        raise

def save_dataframe_to_s3(df, prefix):
    """Save DataFrame to S3 in CSV format"""
    temp_path = f"{S3_TEMP_FULL_PATH}{prefix}_{uuid.uuid4().hex}"
    
    print(f"[{datetime.datetime.now()}] Saving DataFrame to S3: {temp_path}")
    
    # Write DataFrame to S3 as CSV
    df.write \
        .format("csv") \
        .option("header", "false") \
        .option("quote", "\"") \
        .option("escape", "\\") \
        .mode("overwrite") \
        .save(temp_path)
    
    return temp_path

def copy_to_redshift(conn, s3_path, table_name):
    """Run COPY command to load data from S3 to Redshift"""
    try:
        cursor = conn.cursor()
        
        print(f"[{datetime.datetime.now()}] Loading data from {s3_path} to {table_name}")
        
        copy_command = f"""
        COPY {table_name} (
            lrs_id, client_id, statement_id, object_id, session_id,
            session_active, student_code, student_id, school_class_code, school_class_id,
            school_code, school_id, subject_grade_id, page_id, activity_id,
            role_id, section_subject_id, learning_objective_id, learning_level_id, correct_responses_pattern,
            score_raw, success_status, text_response, response, learning_unit_id,
            attempts, activity_type, evaluable, evaluated, dt_time
        )
        FROM '{s3_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        FORMAT AS CSV
        EMPTYASNULL
        BLANKSASNULL
        ACCEPTINVCHARS
        MAXERROR 100;
        """
        
        cursor.execute(copy_command)
        conn.commit()
        
        # Get number of rows loaded
        cursor.execute("SELECT pg_last_copy_count();")
        count = cursor.fetchone()[0]
        
        cursor.close()
        print(f"[{datetime.datetime.now()}] Successfully loaded {count} rows to {table_name}")
        
        return count
    except Exception as e:
        conn.rollback()
        print(f"[{datetime.datetime.now()}] Error executing COPY command: {str(e)}")
        raise

def clean_up_s3_temp(s3_path):
    """Delete temporary files from S3"""
    try:
        print(f"[{datetime.datetime.now()}] Cleaning up temporary files from {s3_path}")
        s3 = boto3.resource('s3')
        
        # Extract bucket and prefix from s3_path
        path_parts = s3_path.replace('s3://', '').split('/', 1)
        bucket = path_parts[0]
        prefix = path_parts[1] if len(path_parts) > 1 else ''
        
        # Delete all objects with the specified prefix
        bucket_obj = s3.Bucket(bucket)
        bucket_obj.objects.filter(Prefix=prefix).delete()
        
        print(f"[{datetime.datetime.now()}] Successfully cleaned up temporary files")
    except Exception as e:
        print(f"[{datetime.datetime.now()}] Error cleaning up temporary files: {str(e)}")

def main():
    try:
        start_time = time.time()
        
        # Establish Redshift connection
        conn = get_redshift_connection()
        
        # Debug: Check if target table exists and has the correct structure
        execute_and_log_sql(conn, 
            "SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'gold' AND tablename = 'ft_student_response_activity_table';", 
            "Check if target table exists")
        
        # Read source tables
        source_tables = read_source_tables_from_redshift()
        
        # Debug: Print schemas of key tables to help diagnose issues
        print(f"[{datetime.datetime.now()}] Schema of dim_lrs_result_scorm:")
        source_tables["dim_lrs_result_scorm"].printSchema()
        
        print(f"[{datetime.datetime.now()}] Processing data transformation - step 1: Create filtered_verbs")
        # Filtered verbs - matches the CTE in SQL
        filtered_verbs = source_tables["verb_answered"]
        filtered_verbs.cache()
        print(f"[{datetime.datetime.now()}] filtered_verbs count: {filtered_verbs.count()}")
        
        print(f"[{datetime.datetime.now()}] Processing data transformation - step 2: Create active_schools")
        # Active schools - matches the CTE in SQL
        active_schools = source_tables["dim_school"].alias("ds").join(
            source_tables["dim_school_level_session"].alias("s").filter(col("active_status") == True),
            col("ds.school_id") == col("s.school_id"),
            "inner"
        ).groupBy("school_id_system") \
            .agg(
                max("session_id").alias("session_id"),
                lit(True).alias("session_active")
            )
        active_schools.cache()
        print(f"[{datetime.datetime.now()}] active_schools count: {active_schools.count()}")
        
        print(f"[{datetime.datetime.now()}] Processing data transformation - step 3: Create base_data")
        # Base data - matches the CTE in SQL
        base_data = source_tables["dim_lrs_object_http"].alias("http").join(
            filtered_verbs.alias("v"),
            (col("http.lrs_id") == col("v.lrs_id")) & 
            (col("http.client_id") == col("v.client_id")) & 
            (col("http.statement_id") == col("v.statement_id")),
            "inner"
        ).select(
            "http.lrs_id", "http.client_id", "http.statement_id", "http.object_id", 
            "http.student_id", "http.school_class_id", "http.school_id", 
            "http.subject_grade_id", "http.page_id", "http.activity_id", 
            "http.role_id", "http.section_subject_id", "http.learning_objective_id", 
            "http.learning_level_id", "http.learning_unit_id"
        ).distinct()
        base_data.cache()
        print(f"[{datetime.datetime.now()}] base_data count: {base_data.count()}")
        
        # Extract join keys for filtering other tables
        base_data_keys = base_data.select("lrs_id", "client_id", "statement_id").distinct()
        base_data_keys.cache()
        
        print(f"[{datetime.datetime.now()}] Processing data transformation - step 4: Create lrs_results")
        # LRS results - matches the CTE in SQL
        lrs_results = source_tables["dim_lrs_result"].join(
            base_data_keys,
            ["lrs_id", "client_id", "statement_id"],
            "inner"
        ).select(
            "lrs_id", "client_id", "statement_id",
            coalesce(col("success_status"), lit(False)).alias("success_status"),
            when(col("score_raw") == "None", None).otherwise(col("score_raw").cast("float")).alias("score_raw"),
            "interaction_type", "extensions", "response"
        )
        lrs_results.cache()
        print(f"[{datetime.datetime.now()}] lrs_results count: {lrs_results.count()}")
        
        print(f"[{datetime.datetime.now()}] Processing data transformation - step 5: Create lrs_scorm_results")
        # SCORM results - matches the CTE in SQL with 'image' column
        lrs_scorm_results = source_tables["dim_lrs_result_scorm"].join(
            base_data_keys,
            ["lrs_id", "client_id", "statement_id"],
            "inner"
        ).select(
            "lrs_id", "client_id", "statement_id",
            "correct_responses_pattern", "response", "image"
        )
        lrs_scorm_results.cache()
        print(f"[{datetime.datetime.now()}] lrs_scorm_results count: {lrs_scorm_results.count()}")
        
        print(f"[{datetime.datetime.now()}] Processing data transformation - step 6: Create activities")
        # Get learning_unit_ids from base_data
        learning_unit_ids = base_data.filter(col("learning_unit_id").isNotNull()) \
            .select("learning_unit_id").distinct()
        
        # Activities - matches the CTE in SQL
        activities = source_tables["dim_page"].alias("pge").join(
            learning_unit_ids,
            "learning_unit_id",
            "inner"
        ).join(
            source_tables["dim_resource"].alias("res"),
            col("pge.page_id") == col("res.page_id"),
            "inner"
        ).join(
            source_tables["dim_activity"].alias("act"),
            col("res.resource_id") == col("act.resource_id"),
            "inner"
        ).groupBy("learning_unit_id") \
            .agg(
                max("activity_type").alias("activity_type"),
                max("activity_attempts").alias("activity_attempts")
            )
        activities.cache()
        print(f"[{datetime.datetime.now()}] activities count: {activities.count()}")
        
        print(f"[{datetime.datetime.now()}] Processing data transformation - step 7: Create statements")
        # Statements - matches the CTE in SQL
        statements = source_tables["dim_lrs_statement"].join(
            base_data_keys,
            ["lrs_id", "client_id", "statement_id"],
            "inner"
        ).select(
            "lrs_id", "client_id", "statement_id", "dt_time"
        )
        statements.cache()
        print(f"[{datetime.datetime.now()}] statements count: {statements.count()}")
        
        print(f"[{datetime.datetime.now()}] Processing data transformation - step 8: Performing final joins")
        # Final join to create the result - matches the main SELECT in SQL
        # Using LEFT JOINS as in the SQL query
        final_df = base_data.alias("b").join(
                lrs_results.alias("r"),
                (col("b.lrs_id") == col("r.lrs_id")) & 
                (col("b.client_id") == col("r.client_id")) & 
                (col("b.statement_id") == col("r.statement_id")),
                "left"
            ).join(
                lrs_scorm_results.alias("s"),
                (col("b.lrs_id") == col("s.lrs_id")) & 
                (col("b.client_id") == col("s.client_id")) & 
                (col("b.statement_id") == col("s.statement_id")),
                "left"
            ).join(
                activities.alias("a"),
                col("b.learning_unit_id") == col("a.learning_unit_id"),
                "left"
            ).join(
                active_schools.alias("sc"),
                col("b.school_id") == col("sc.school_id_system"),
                "left"
            ).join(
                statements.alias("st"),
                (col("b.lrs_id") == col("st.lrs_id")) & 
                (col("b.client_id") == col("st.client_id")) & 
                (col("b.statement_id") == col("st.statement_id")),
                "left"
            )
        
        print(f"[{datetime.datetime.now()}] Processing data transformation - step 9: Creating final dataset")
        # Create the final result with the right columns
        student_response_activity = final_df.select(
            col("b.lrs_id"),
            col("b.client_id"),
            col("b.statement_id"),
            col("b.object_id"),
            col("sc.session_id"),
            coalesce(col("sc.session_active"), lit(False)).alias("session_active"),
            col("b.student_id").alias("student_code"),
            concat(lit('00000000-0000-1000-0000-'), lpad(col("b.student_id").cast("string"), 12, '0')).alias("student_id"),
            col("b.school_class_id").alias("school_class_code"),
            concat(lit('00000000-0000-1000-0000-'), lpad(col("b.school_class_id").cast("string"), 12, '0')).alias("school_class_id"),
            col("b.school_id").alias("school_code"),
            concat(lit('00000000-0000-1000-0000-'), lpad(col("b.school_id").cast("string"), 12, '0')).alias("school_id"),
            col("b.subject_grade_id"),
            col("b.page_id"),
            col("b.activity_id"),
            col("b.role_id"),
            col("b.section_subject_id"),
            when(col("b.learning_objective_id") == -1, None).otherwise(col("b.learning_objective_id")).alias("learning_objective_id"),
            when(col("b.learning_level_id") == -1, None).otherwise(col("b.learning_level_id")).alias("learning_level_id"),
            col("s.correct_responses_pattern"),
            col("r.score_raw"),
            coalesce(col("r.success_status"), lit(False)).alias("success_status"),
            # Updated CASE expressions to match the SQL query
            when(col("r.extensions") == "http://scorm&46;com/extensions/usa-data", col("s.response"))
              .otherwise(col("s.image")).alias("text_response"),
            when(col("r.interaction_type") == "other", lit("image"))
              .otherwise(col("r.response")).alias("response"),
            col("b.learning_unit_id"),
            coalesce(col("a.activity_attempts"), lit(0)).cast("integer").alias("attempts"),
            coalesce(col("a.activity_type"), lit("")).alias("activity_type"),
            lit(0).cast("integer").alias("evaluable"),
            lit(False).cast("boolean").alias("evaluated"),
            col("st.dt_time")
        ).distinct()
        
        # Count rows to be inserted
        sample_count = student_response_activity.limit(10).count()
        print(f"[{datetime.datetime.now()}] Sample count (up to 10 rows): {sample_count}")
        
        if sample_count > 0:
            print(f"[{datetime.datetime.now()}] Sample data preview:")
            student_response_activity.limit(2).show(truncate=False)
        
        row_count = student_response_activity.count()
        print(f"[{datetime.datetime.now()}] Total number of rows to be inserted: {row_count}")
        
        if row_count == 0:
            print(f"[{datetime.datetime.now()}] WARNING: No data to insert! Checking individual table counts...")
            # Debug counts of key tables to identify issues
            print(f"base_data count: {base_data.count()}")
            print(f"lrs_results count: {lrs_results.count()}")
            print(f"lrs_scorm_results count: {lrs_scorm_results.count()}")
            print(f"activities count: {activities.count()}")
            print(f"active_schools count: {active_schools.count()}")
            print(f"statements count: {statements.count()}")
            
            # Check for data after each join to identify the join that's eliminating all rows
            print(f"Checking join results...")
            join1 = base_data.join(lrs_results, ["lrs_id", "client_id", "statement_id"], "left")
            print(f"After join with lrs_results: {join1.count()}")
            
            join2 = join1.join(lrs_scorm_results, ["lrs_id", "client_id", "statement_id"], "left")
            print(f"After join with lrs_scorm_results: {join2.count()}")
            
            # Continue with other joins to identify the issue
        else:
            # Truncate target table
            truncate_target_table(conn)
            
            # Save to S3 temporarily and load to Redshift
            temp_s3_path = save_dataframe_to_s3(student_response_activity, "student_response")
            
            # Perform the COPY operation
            loaded_count = copy_to_redshift(conn, temp_s3_path, "gold.ft_student_response_activity_table")
            
            # Clean up temporary files
            clean_up_s3_temp(temp_s3_path)
        
        # Calculate execution statistics
        end_time = time.time()
        execution_time = end_time - start_time
        
        print(f"[{datetime.datetime.now()}] ETL job completed successfully")
        print(f"[{datetime.datetime.now()}] Total execution time: {execution_time:.2f} seconds")
        print(f"[{datetime.datetime.now()}] Rows processed: {row_count}")
        if 'loaded_count' in locals():
            print(f"[{datetime.datetime.now()}] Rows loaded: {loaded_count}")
        
    except Exception as e:
        print(f"[{datetime.datetime.now()}] ETL job failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print(f"[{datetime.datetime.now()}] Redshift connection closed")

# Execute the main function
main()

job.commit()