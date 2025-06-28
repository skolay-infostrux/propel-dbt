{% macro sp_insurance_dimension_data_population() %}

{% set prepare_database = get_prepare_database() %}
{% set sql %}

CREATE OR REPLACE PROCEDURE {{ prepare_database }}.DWH.SP_INSURANCE_DIMENSION_DATA_POPULATION()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE 

    LN_JOB_ID                  INT;
	LN_BATCH_ID                INT;
	LN_ROW_COUNT               SMALLINT;
	LC_JOB_NAME                VARCHAR(200)   DEFAULT ''SP_INSURANCE_DIMENSION_DATA_POPULATION'';
	LC_BATCH_TYPE              VARCHAR(100)   DEFAULT ''DELTA'';
	LC_BATCH_LABEL             VARCHAR(500)   DEFAULT ''Populate Insurance Dim tables DIM_INSURANCE_PROTECTION_PLAN & DIM_INSURANCE_PROTECTION_PLAN_STATUS'';
	LC_CHECK_POINT_TYPE        VARCHAR(50)    DEFAULT ''TIMESTAMP'';
	LD_CHECK_POINT_START       DATETIME;
	LD_CHECK_POINT_END         DATETIME;
	LN_IS_SUCCESS              SMALLINT;
	LD_START_DATETIME          DATETIME;
	LD_END_DATETIME            DATETIME;
	LC_ERROR_MESSAGE           VARCHAR(500);
	LC_TABLE_NAME              VARCHAR(100)   DEFAULT ''DIM_INSURANCE_PROTECTION_PLAN'';
	LC_LOG_TYPE                VARCHAR(50);
	LC_LOG_LABEL               VARCHAR(200);
	LC_LOG_MESSAGE             VARCHAR(500);


BEGIN
    
   -- -------------------------------------------------------------------------------------------------------------------------------------------------------
   -- step 1 - Log start
   -- -------------------------------------------------------------------------------------------------------------------------------------------------------

    LN_BATCH_ID          := (SELECT OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_ID.NEXTVAL FROM DUAL);

    LN_JOB_ID            := (SELECT JOB_ID FROM OP_ADMIN.OPERATIONS.ETL_JOBS WHERE JOB_NAME = :LC_JOB_NAME);

    LD_CHECK_POINT_START := (SELECT MAX(CAST(T.CHECK_POINT_END AS DATETIME))
                              FROM OP_ADMIN.OPERATIONS.ETL_JOB_BATCHES T
                             WHERE T.JOB_ID = :LN_JOB_ID
                               AND T.IS_SUCCESS = 1);

    LD_CHECK_POINT_START := NVL(:LD_CHECK_POINT_START, CAST(''2000-01-01 12:00:00'' AS DATETIME));

    LD_CHECK_POINT_END   := CURRENT_TIMESTAMP();
    LC_TABLE_NAME        := ''DIM_INSURANCE_PROTECTION_PLAN'';
    LN_IS_SUCCESS        := NULL;
    LD_START_DATETIME    := CURRENT_TIMESTAMP();
    LD_END_DATETIME      := NULL;



    CALL OP_ADMIN.OPERATIONS.SP_ETL_JOB_BATCH_POPULATE (:LN_BATCH_ID,
                                                       :LN_JOB_ID,
                                                       :LC_BATCH_TYPE,
                                                       :LC_BATCH_LABEL,
                                                       :LC_CHECK_POINT_TYPE,
                                                       :LD_CHECK_POINT_START,
                                                       :LD_CHECK_POINT_END,
                                                       :LN_IS_SUCCESS,
                                                       :LD_START_DATETIME,
                                                       :LD_END_DATETIME);
													   
   -- -------------------------------------------------------------------------------------------------------------------------------------------------------
   -- step 2 - Merge data from source tables into Dim tables
   -- -------------------------------------------------------------------------------------------------------------------------------------------------------
	
	
   

	MERGE INTO DWH_DEV.DWH.DIM_INSURANCE_PROTECTION_PLAN target USING (	
	WITH CTE AS (
		 SELECT pp."id" AS protection_plan_id,
				pps."base_loan_id" AS base_loan_id,
				pp."protection_plan_application_id" AS protection_plan_application_id,  
				pp."current_loan_status" AS current_loan_status,
				pp."loan_status_updated_date" AS loan_status_updated_date,
				pp."pay_frequency" AS pay_frequency,
				pp."external_reference" AS external_reference,
				pp."end_of_coverage_datetime" AS end_of_coverage_datetime,
				pp."created_span_id" AS created_span_id,
				pp."updated_span_id" AS updated_span_id,
				pp."version" AS version,
				pp."policy_id" AS policy_id,
				ip."policy_number" AS policy_number,
				ip."product_code" AS product_code,
				ip."policy_document_url" AS policy_document_url,  
				ip."insurance_provider_id" AS insurance_provider_id,  
				ipr."name" AS  insurance_provider_name,
				ipr."code" AS insurance_provider_code,
				ipr."enquiry_email" AS enquiry_email,
				ipr."enquiry_phone" AS enquiry_phone,
				ipr."customer_dashboard_url" AS customer_dashboard_url,
				pp."created_datetime" AS created_datetime,
				pp."updated_datetime" AS updated_datetime, 
				CURRENT_TIMESTAMP() AS process_timestamp 
		  FROM DEV_ENTERPRISE_LANDING."insurance"."protection_plan" pp
			INNER JOIN DEV_ENTERPRISE_LANDING."insurance"."insurance_policy" ip on pp."policy_id" = ip."id"
			INNER JOIN DEV_ENTERPRISE_LANDING."insurance"."insurance_provider" ipr on ip."insurance_provider_id" = ipr."id"
			LEFT JOIN DEV_ENTERPRISE_LANDING."insurance"."protection_plan_status_log" ppsl on pp."protection_plan_application_id" = ppsl."protection_plan_application_id"
			INNER JOIN DEV_ENTERPRISE_LANDING."insurance"."protection_plan_status" pps on ppsl."id" = pps."protection_plan_status_log_id"
			-- WHERE pp.UPDATED_DATETIME >= (CURRENT_DATE()- :INTERVALDAYS -- Old incremental logic
			--WHERE pp.UPDATED_DATETIME > :LD_CHECK_POINT_START AND pp.UPDATED_DATETIME <= :LD_CHECK_POINT_END
            WHERE (
                      (pp._SNOWFLAKE_INSERTED_AT > :LD_CHECK_POINT_START and pp._SNOWFLAKE_INSERTED_AT <= :LD_CHECK_POINT_END) OR
                      (pp._SNOWFLAKE_UPDATED_AT > :LD_CHECK_POINT_START and pp._SNOWFLAKE_UPDATED_AT <= :LD_CHECK_POINT_END)
                   )
		)
	   SELECT * FROM CTE
	) source
		ON target.PROTECTION_PLAN_ID = source.PROTECTION_PLAN_ID
		WHEN MATCHED THEN 
			UPDATE SET 
				TARGET.BASE_LOAN_ID = SOURCE.BASE_LOAN_ID,
				TARGET.PROTECTION_PLAN_APPLICATION_ID = SOURCE.PROTECTION_PLAN_APPLICATION_ID,  
				TARGET.CURRENT_LOAN_STATUS = SOURCE.CURRENT_LOAN_STATUS,
				TARGET.LOAN_STATUS_UPDATED_DATE = SOURCE.LOAN_STATUS_UPDATED_DATE,
				TARGET.PAY_FREQUENCY = SOURCE.PAY_FREQUENCY,
				TARGET.EXTERNAL_REFERENCE = SOURCE.EXTERNAL_REFERENCE,
				TARGET.END_OF_COVERAGE_DATETIME = SOURCE.END_OF_COVERAGE_DATETIME,
				TARGET.CREATED_SPAN_ID = SOURCE.CREATED_SPAN_ID,
				TARGET.UPDATED_SPAN_ID = SOURCE.UPDATED_SPAN_ID,
				TARGET.VERSION = SOURCE.VERSION,
				TARGET.POLICY_ID = SOURCE.POLICY_ID,
				TARGET.POLICY_NUMBER = SOURCE.POLICY_NUMBER,
				TARGET.PRODUCT_CODE = SOURCE.PRODUCT_CODE,
				TARGET.POLICY_DOCUMENT_URL = SOURCE.POLICY_DOCUMENT_URL,  
				TARGET.INSURANCE_PROVIDER_ID = SOURCE.INSURANCE_PROVIDER_ID,  
				TARGET.INSURANCE_PROVIDER_NAME = SOURCE.INSURANCE_PROVIDER_NAME,
				TARGET.INSURANCE_PROVIDER_CODE = SOURCE.INSURANCE_PROVIDER_CODE,
				TARGET.ENQUIRY_EMAIL = SOURCE.ENQUIRY_EMAIL,
				TARGET.ENQUIRY_PHONE = SOURCE.ENQUIRY_PHONE,
				TARGET.CUSTOMER_DASHBOARD_URL = SOURCE.CUSTOMER_DASHBOARD_URL,
				TARGET.CREATED_DATETIME = SOURCE.CREATED_DATETIME,
				TARGET.UPDATED_DATETIME = SOURCE.UPDATED_DATETIME,  
				TARGET.PROCESS_TIMESTAMP = SOURCE.PROCESS_TIMESTAMP
		WHEN NOT MATCHED THEN 
			INSERT (
					PROTECTION_PLAN_ID,
					BASE_LOAN_ID,
					PROTECTION_PLAN_APPLICATION_ID,  
					CURRENT_LOAN_STATUS,
					LOAN_STATUS_UPDATED_DATE,
					PAY_FREQUENCY,
					EXTERNAL_REFERENCE,
					END_OF_COVERAGE_DATETIME,
					CREATED_SPAN_ID,
					UPDATED_SPAN_ID,
					VERSION,
					POLICY_ID,
					POLICY_NUMBER,
					PRODUCT_CODE,
					POLICY_DOCUMENT_URL,  
					INSURANCE_PROVIDER_ID,  
					INSURANCE_PROVIDER_NAME,
					INSURANCE_PROVIDER_CODE,
					ENQUIRY_EMAIL,
					ENQUIRY_PHONE,
					CUSTOMER_DASHBOARD_URL,
					CREATED_DATETIME,
					UPDATED_DATETIME, 
					PROCESS_TIMESTAMP)  
			 VALUES( SOURCE.PROTECTION_PLAN_ID,
					SOURCE.BASE_LOAN_ID,
					SOURCE.PROTECTION_PLAN_APPLICATION_ID,  
					SOURCE.CURRENT_LOAN_STATUS,
					SOURCE.LOAN_STATUS_UPDATED_DATE,
					SOURCE.PAY_FREQUENCY,
					SOURCE.EXTERNAL_REFERENCE,
					SOURCE.END_OF_COVERAGE_DATETIME,
					SOURCE.CREATED_SPAN_ID,
					SOURCE.UPDATED_SPAN_ID,
					SOURCE.VERSION,
					SOURCE.POLICY_ID,
					SOURCE.POLICY_NUMBER,
					SOURCE.PRODUCT_CODE,
					SOURCE.POLICY_DOCUMENT_URL,  
					SOURCE.INSURANCE_PROVIDER_ID,  
					SOURCE.INSURANCE_PROVIDER_NAME,
					SOURCE.INSURANCE_PROVIDER_CODE,
					SOURCE.ENQUIRY_EMAIL,
					SOURCE.ENQUIRY_PHONE,
					SOURCE.CUSTOMER_DASHBOARD_URL,
					SOURCE.CREATED_DATETIME,
					SOURCE.UPDATED_DATETIME, 
					SOURCE.PROCESS_TIMESTAMP);


	LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Merge'';
    LC_LOG_LABEL      := CONCAT(''Merge data into DIM_INSURANCE_PROTECTION_PLAN table'');
    LC_LOG_MESSAGE    := CONCAT(''Merge succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
    LC_ERROR_MESSAGE  := NULL;
    LD_END_DATETIME   := CURRENT_TIMESTAMP();
    LN_IS_SUCCESS     := 1;
   
    CALL OP_ADMIN.OPERATIONS.SP_ETL_BATCH_EXECUTION_LOG_POPULATE(:LN_BATCH_ID,
                                                                :LC_LOG_TYPE,
                                                                :LC_LOG_LABEL,
                                                                :LC_LOG_MESSAGE,
                                                                :LC_ERROR_MESSAGE,
                                                                :LD_START_DATETIME,
                                                                :LD_END_DATETIME);



   
    LC_TABLE_NAME        := ''DIM_INSURANCE_PROTECTION_PLAN_STATUS'';
    LN_IS_SUCCESS        := NULL;
    LD_START_DATETIME    := CURRENT_TIMESTAMP();
    LD_END_DATETIME      := NULL;
   
   
	MERGE INTO DWH_DEV.DWH.DIM_INSURANCE_PROTECTION_PLAN_STATUS target USING (
    WITH CTE AS (
        SELECT 	pps."base_loan_id" AS base_loan_id,
				pps."protection_plan_status_log_id" AS protection_plan_status_log_id,
				ppsl."attempted_status" AS attempted_status,
				ppsl."status" AS status,
				ppsl."span_id" AS span_id,
				ppsl."created_datetime" AS created_datetime,
                CURRENT_TIMESTAMP() AS process_timestamp  
      FROM DEV_ENTERPRISE_LANDING."insurance"."protection_plan_status" pps
      INNER JOIN DEV_ENTERPRISE_LANDING."insurance"."protection_plan_status_log" ppsl ON pps."protection_plan_status_log_id" = ppsl."id"
	)
    SELECT * FROM CTE
) source
	ON TARGET.BASE_LOAN_ID = SOURCE.BASE_LOAN_ID
	WHEN MATCHED THEN 
			UPDATE SET   
			TARGET.PROTECTION_PLAN_STATUS_LOG_ID = SOURCE.PROTECTION_PLAN_STATUS_LOG_ID,
			TARGET.ATTEMPTED_STATUS = SOURCE.ATTEMPTED_STATUS,
			TARGET.STATUS = SOURCE.STATUS,
			TARGET.SPAN_ID = SOURCE.SPAN_ID,
			TARGET.CREATED_DATETIME = SOURCE.CREATED_DATETIME, 
			TARGET.PROCESS_TIMESTAMP = SOURCE.PROCESS_TIMESTAMP
	WHEN NOT MATCHED THEN 
			INSERT ( 
				BASE_LOAN_ID,
				PROTECTION_PLAN_STATUS_LOG_ID,
				ATTEMPTED_STATUS,
				STATUS,
				SPAN_ID,
				CREATED_DATETIME, 
				PROCESS_TIMESTAMP)  
		 VALUES( SOURCE.BASE_LOAN_ID,
				SOURCE.PROTECTION_PLAN_STATUS_LOG_ID,
				SOURCE.ATTEMPTED_STATUS,
				SOURCE.STATUS,
				SOURCE.SPAN_ID,
				SOURCE.CREATED_DATETIME, 
				SOURCE.PROCESS_TIMESTAMP);


	   lN_ROW_COUNT      := SQLROWCOUNT;
	   lC_LOG_TYPE       := ''Merge'';
	   lC_LOG_LABEL      := CONCAT(''Merge data into DIM_INSURANCE_PROTECTION_PLAN_STATUS table'');
	   lC_LOG_MESSAGE    := CONCAT(''Merge succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
	   lC_ERROR_MESSAGE  := NULL;
	   lD_END_DATETIME   := CURRENT_TIMESTAMP();
	   lN_IS_SUCCESS     := 1;

	   CALL OP_ADMIN.OPERATIONS.SP_ETL_BATCH_EXECUTION_LOG_POPULATE(:LN_BATCH_ID,
																	:LC_LOG_TYPE,
																	:LC_LOG_LABEL,
																	:LC_LOG_MESSAGE,
																	:LC_ERROR_MESSAGE,
																	:LD_START_DATETIME,
																	:LD_END_DATETIME);
																	
   -- -------------------------------------------------------------------------------------------------------------------------------------------------------
   -- step 3 - Log end - update batch status and end_datetime
   -- -------------------------------------------------------------------------------------------------------------------------------------------------------

   UPDATE OP_ADMIN.OPERATIONS.ETL_JOB_BATCHES T
      SET T.END_DATETIME = :LD_END_DATETIME,
          T.IS_SUCCESS = :LN_IS_SUCCESS
    WHERE T.BATCH_ID = :LN_BATCH_ID;


   -- -------------------------------------------------------------------------------------------------------------------------------------------------------
   -- step 4 - Log end - update batch status in ETL_JOBS table
   -- -------------------------------------------------------------------------------------------------------------------------------------------------------
   UPDATE OP_ADMIN.OPERATIONS.ETL_JOBS T
      SET T.IS_SUCCESS = :LN_IS_SUCCESS,
          T.CHECK_POINT_START = :LD_CHECK_POINT_START,
          T.CHECK_POINT_END = :LD_CHECK_POINT_END
    WHERE T.JOB_ID = :LN_JOB_ID;

   COMMIT;
   RETURN ''SP_INSURANCE_DIMENSION_DATA_POPULATION : Job is done'';

  
  EXCEPTION

   WHEN OTHER THEN

      LN_ROW_COUNT      := NULL;
      LC_LOG_TYPE       := ''Merge'';
      LC_LOG_LABEL      := CONCAT(''Merge data into '', :LC_TABLE_NAME, '' table'');
      LC_LOG_MESSAGE    := ''Merge failed'';
      LC_ERROR_MESSAGE  := SUBSTR(CONCAT(SQLCODE, '' - '', SQLERRM), 1, 500);
      LD_END_DATETIME   := CURRENT_TIMESTAMP();
      LN_IS_SUCCESS     := 0;

      CALL OP_ADMIN.OPERATIONS.SP_ETL_BATCH_EXECUTION_LOG_POPULATE (:LN_BATCH_ID,
                                                                    :LC_LOG_TYPE,
                                                                    :LC_LOG_LABEL,
                                                                    :LC_LOG_MESSAGE,
                                                                    :LC_ERROR_MESSAGE,
                                                                    :LD_START_DATETIME,
                                                                    :LD_END_DATETIME);

      UPDATE OP_ADMIN.OPERATIONS.ETL_JOB_BATCHES T
         SET T.END_DATETIME = :LD_END_DATETIME,
             T.IS_SUCCESS = :LN_IS_SUCCESS
       WHERE T.BATCH_ID = :LN_BATCH_ID;

      UPDATE OP_ADMIN.OPERATIONS.ETL_JOBS T
         SET T.IS_SUCCESS = :LN_IS_SUCCESS,
             T.CHECK_POINT_START = :LD_CHECK_POINT_START,
             T.CHECK_POINT_END = :LD_CHECK_POINT_END
       WHERE T.JOB_ID = :LN_JOB_ID;

      COMMIT;
      RETURN LC_ERROR_MESSAGE;

END;'
;
{% endset %}
{{ return(sql) }}
{% endmacro %}