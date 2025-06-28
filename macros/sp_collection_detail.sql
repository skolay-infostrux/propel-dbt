{% macro sp_collection_detail() %}
{% set prepare_database = get_prepare_database() %}
{% set sql %}

CREATE OR REPLACE PROCEDURE {{ prepare_database }}.DWH.SP_COLLECTION_DETAIL()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE 

    LN_JOB_ID                  INT;
    LN_BATCH_ID                INT;
    LN_ROW_COUNT               SMALLINT;
    LC_JOB_NAME                VARCHAR(200)   DEFAULT ''SP_COLLECTION_DETAIL'';
    LC_BATCH_TYPE              VARCHAR(100)   DEFAULT ''DELTA'';
    LC_BATCH_LABEL             VARCHAR(500)   DEFAULT ''Populate Collection Details in FCT_COLLECTION_DETAIL table'';
    LC_CHECK_POINT_TYPE        VARCHAR(50)    DEFAULT ''TIMESTAMP'';
    LD_CHECK_POINT_START       DATETIME;
    LD_CHECK_POINT_END         DATETIME;
    LN_IS_SUCCESS              SMALLINT;
    LD_START_DATETIME          DATETIME;
    LD_END_DATETIME            DATETIME;
    LC_ERROR_MESSAGE           VARCHAR(500);
    LC_TABLE_NAME              VARCHAR(100)   DEFAULT ''FCT_COLLECTION_DETAIL'';
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
    LC_TABLE_NAME        := ''FCT_COLLECTION_DETAIL'';
    LN_IS_SUCCESS        := NULL;
    LD_START_DATETIME    := CURRENT_TIMESTAMP();
    LD_END_DATETIME      := NULL;



    CALL OP_ADMIN.OPERATIONS.SP_ETL_JOB_BATCH_POPULATE (
                                                       :LN_BATCH_ID,
                                                       :LN_JOB_ID,
                                                       :LC_BATCH_TYPE,
                                                       :LC_BATCH_LABEL,
                                                       :LC_CHECK_POINT_TYPE,
                                                       :LD_CHECK_POINT_START,
                                                       :LD_CHECK_POINT_END,
                                                       :LN_IS_SUCCESS,
                                                       :LD_START_DATETIME,
                                                       :LD_END_DATETIME
                                                       );
													   
-- -------------------------------------------------------------------------------------------------------------------------------------------------------  
-- step 2 - Merge data from source tables into Fct tables
-- -------------------------------------------------------------------------------------------------------------------------------------------------------

		
-- Insert new collection data and update existing collections records

	MERGE INTO DWH_DEV.DWH.FCT_COLLECTION_DETAIL target USING (	
	WITH CTE AS (	
		 SELECT clm."collection_id" AS collection_id,
				clm."base_loan_id" AS base_loan_id,
				clm."payment_schedule_id" AS payment_schedule_id,
				clm."total_amount" AS total_amount,
				clm."principal_amount" AS principal_amount,
				clm."interest_amount" AS interest_amount,
				clm."fees_amount" AS fees_amount,
				clm."other_amount" AS other_amount,
				clm."delinquency_date" AS delinquency_date,
				clm."fully_defaulted_date" AS fully_defaulted_date, 
				c."customer_id" AS customer_id,
				c."status_code" AS status_code,
				c."sub_status_code" AS sub_status_code,
				c."static_note_id" AS static_note_id,
				c."escalation_level" AS escalation_level,
				c."settlement_flag" AS settlement_flag,
				c."settlement_amount" AS settlement_amount,
				c."settlement_discounted_amount" AS settlement_discounted_amount,
				c."settlement_approval_timestamp" AS settlement_approval_timestamp,
				c."request_to_rotate_flag" AS request_to_rotate_flag,
				c."created_by_user_id" AS created_by_user_id, 
				clm."create_timestamp" AS create_timestamp,
				clm."update_timestamp" AS update_timestamp, 
				CURRENT_TIMESTAMP AS process_timestamp 
		FROM DEV_ENTERPRISE_LANDING."jaglms"."collection_lms_loan_map" clm
			INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."collections" c on c."collection_id" = clm."collection_id"  
        --WHERE clm."update_timestamp" > :LD_CHECK_POINT_START AND clm."update_timestamp" <= :LD_CHECK_POINT_END
        WHERE (
                 (clm._SNOWFLAKE_INSERTED_AT > :LD_CHECK_POINT_START and clm._SNOWFLAKE_INSERTED_AT <= :LD_CHECK_POINT_END) OR
                 (clm._SNOWFLAKE_UPDATED_AT > :LD_CHECK_POINT_START and clm._SNOWFLAKE_UPDATED_AT <= :LD_CHECK_POINT_END)
              )
		 )
	   SELECT * FROM CTE
	) source
		ON TARGET.COLLECTION_ID = SOURCE.COLLECTION_ID
		WHEN MATCHED THEN 
			UPDATE SET
				TARGET.BASE_LOAN_ID = SOURCE.BASE_LOAN_ID ,
				TARGET.PAYMENT_SCHEDULE_ID = SOURCE.PAYMENT_SCHEDULE_ID,
				TARGET.TOTAL_AMOUNT = SOURCE.TOTAL_AMOUNT,
				TARGET.PRINCIPAL_AMOUNT = SOURCE.PRINCIPAL_AMOUNT,
				TARGET.INTEREST_AMOUNT = SOURCE.INTEREST_AMOUNT,
				TARGET.FEES_AMOUNT = SOURCE.FEES_AMOUNT,
				TARGET.OTHER_AMOUNT = SOURCE.OTHER_AMOUNT,
				TARGET.DELINQUENCY_DATE = SOURCE.DELINQUENCY_DATE,
				TARGET.FULLY_DEFAULTED_DATE = SOURCE.FULLY_DEFAULTED_DATE, 
				TARGET.CUSTOMER_ID = SOURCE.CUSTOMER_ID,
				TARGET.STATUS_CODE = SOURCE.STATUS_CODE,
				TARGET.SUB_STATUS_CODE = SOURCE.SUB_STATUS_CODE,
				TARGET.STATIC_NOTE_ID = SOURCE.STATIC_NOTE_ID,
				TARGET.ESCALATION_LEVEL = SOURCE.ESCALATION_LEVEL,
				TARGET.SETTLEMENT_FLAG = SOURCE.SETTLEMENT_FLAG,
				TARGET.SETTLEMENT_AMOUNT = SOURCE.SETTLEMENT_AMOUNT,
				TARGET.SETTLEMENT_DISCOUNTED_AMOUNT = SOURCE.SETTLEMENT_DISCOUNTED_AMOUNT,
				TARGET.SETTLEMENT_APPROVAL_TIMESTAMP = SOURCE.SETTLEMENT_APPROVAL_TIMESTAMP,
				TARGET.REQUEST_TO_ROTATE_FLAG = SOURCE.REQUEST_TO_ROTATE_FLAG,
				TARGET.CREATE_TIMESTAMP = SOURCE.CREATE_TIMESTAMP,
				TARGET.UPDATE_TIMESTAMP = SOURCE.UPDATE_TIMESTAMP, 
				TARGET.PROCESS_TIMESTAMP = SOURCE.PROCESS_TIMESTAMP
		WHEN NOT MATCHED THEN 
			INSERT (COLLECTION_ID,
					BASE_LOAN_ID,
					PAYMENT_SCHEDULE_ID,
					TOTAL_AMOUNT,
					PRINCIPAL_AMOUNT,
					INTEREST_AMOUNT,
					FEES_AMOUNT,
					OTHER_AMOUNT,
					DELINQUENCY_DATE,
					FULLY_DEFAULTED_DATE, 
					CUSTOMER_ID,
					STATUS_CODE,
					SUB_STATUS_CODE,
					STATIC_NOTE_ID,
					ESCALATION_LEVEL,
					SETTLEMENT_FLAG,
					SETTLEMENT_AMOUNT,
					SETTLEMENT_DISCOUNTED_AMOUNT,
					SETTLEMENT_APPROVAL_TIMESTAMP,
					REQUEST_TO_ROTATE_FLAG,
					CREATED_BY_USER_ID,
					CREATE_TIMESTAMP,
					UPDATE_TIMESTAMP,
					PROCESS_TIMESTAMP)
			 VALUES(SOURCE.COLLECTION_ID,
					SOURCE.BASE_LOAN_ID,
					SOURCE.PAYMENT_SCHEDULE_ID,
					SOURCE.TOTAL_AMOUNT,
					SOURCE.PRINCIPAL_AMOUNT,
					SOURCE.INTEREST_AMOUNT,
					SOURCE.FEES_AMOUNT,
					SOURCE.OTHER_AMOUNT,
					SOURCE.DELINQUENCY_DATE,
					SOURCE.FULLY_DEFAULTED_DATE, 
					SOURCE.CUSTOMER_ID,
					SOURCE.STATUS_CODE,
					SOURCE.SUB_STATUS_CODE,
					SOURCE.STATIC_NOTE_ID,
					SOURCE.ESCALATION_LEVEL,
					SOURCE.SETTLEMENT_FLAG,
					SOURCE.SETTLEMENT_AMOUNT,
					SOURCE.SETTLEMENT_DISCOUNTED_AMOUNT,
					SOURCE.SETTLEMENT_APPROVAL_TIMESTAMP,
					SOURCE.REQUEST_TO_ROTATE_FLAG,
					SOURCE.CREATED_BY_USER_ID, 
					SOURCE.CREATE_TIMESTAMP,
					SOURCE.UPDATE_TIMESTAMP, 
					SOURCE.PROCESS_TIMESTAMP);

	LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Merge'';
    LC_LOG_LABEL      := CONCAT(''Merge data into FCT_COLLECTION_DETAIL table'');
    LC_LOG_MESSAGE    := CONCAT(''Merge succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
    LC_ERROR_MESSAGE  := NULL;
    LD_END_DATETIME   := CURRENT_TIMESTAMP();
    LN_IS_SUCCESS     := 1;
   
    CALL OP_ADMIN.OPERATIONS.SP_ETL_BATCH_EXECUTION_LOG_POPULATE(
                                                                :LN_BATCH_ID,
                                                                :LC_LOG_TYPE,
                                                                :LC_LOG_LABEL,
                                                                :LC_LOG_MESSAGE,
                                                                :LC_ERROR_MESSAGE,
                                                                :LD_START_DATETIME,
                                                                :LD_END_DATETIME
                                                                );

    LC_TABLE_NAME        := ''FCT_COLLECTION_DETAIL'';
    LN_IS_SUCCESS        := NULL;
    LD_START_DATETIME    := CURRENT_TIMESTAMP();
    LD_END_DATETIME      := NULL; 
					
-- Remove those collection records are purged in production db

    DROP TABLE IF EXISTS PURGED_COLLECTIONS;
	CREATE TEMP TABLE IF NOT EXISTS PURGED_COLLECTIONS AS 
	(
		SELECT FC.COLLECTION_ID, FC.BASE_LOAN_ID 
		FROM DWH_DEV.DWH.FCT_COLLECTION_DETAIL FC
			LEFT JOIN DEV_ENTERPRISE_LANDING."jaglms"."collection_lms_loan_map" MAP ON FC.collection_id = MAP."collection_id" AND FC.BASE_LOAN_ID = MAP."base_loan_id"
		WHERE MAP."collection_id" IS NULL 
	);
    
	DELETE FROM DWH_DEV.DWH.FCT_COLLECTION_DETAIL WHERE COLLECTION_ID IN (SELECT COLLECTION_ID FROM PURGED_COLLECTIONS);

	LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Delete'';
    LC_LOG_LABEL      := CONCAT(''Remove those collection records are purged in production db FCT_COLLECTION_DETAIL table'');
    LC_LOG_MESSAGE    := CONCAT(''Delete succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
    LC_ERROR_MESSAGE  := NULL;
    LD_END_DATETIME   := CURRENT_TIMESTAMP();
    LN_IS_SUCCESS     := 1;
   
    CALL OP_ADMIN.OPERATIONS.SP_ETL_BATCH_EXECUTION_LOG_POPULATE(
                                                                :LN_BATCH_ID,
                                                                :LC_LOG_TYPE,
                                                                :LC_LOG_LABEL,
                                                                :LC_LOG_MESSAGE,
                                                                :LC_ERROR_MESSAGE,
                                                                :LD_START_DATETIME,
                                                                :LD_END_DATETIME
                                                                );
	
	
-- Include collected collection payment amount for active loan loans


	CREATE TEMP TABLE IF NOT EXISTS COLLECTION_PAYMENT_ACT AS 
	(
		SELECT PS."base_loan_id" AS BASE_LOAN_ID,
				SUM(PSI."total_amount") TOTAL_AMOUNT,
				SUM(PSI."amount_fee") AMOUNT_FEE,
				SUM(PSI."amount_prin") AMOUNT_PRIN,
				SUM(PSI."amount_int") AMOUNT_INT,
				SUM(PSI."amount_other") AMOUNT_OTHER
		FROM DEV_ENTERPRISE_LANDING."jaglms"."lms_payment_schedules" PS
		INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_base_loans" BL ON PS."base_loan_id" = BL."base_loan_id"
		INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_payment_schedule_items" PSI ON PS."payment_schedule_id" = PSI."payment_schedule_id" AND "is_collections" = 1 
		WHERE BL."loan_status" IN (''Originated'' , ''Delinquent'', ''DEFAULT'')
				AND PSI."status" IN (''Cleared'' , ''SENT'')
				AND PSI."item_type" = ''D'' 
				AND PSI."item_date" <= :LD_CHECK_POINT_START  
		GROUP BY PS."base_loan_id"
	);
    
	UPDATE DWH_DEV.DWH.FCT_COLLECTION_DETAIL FCD
	SET FCD.COLLECTED_TOTAL_AMOUNT = AA.TOTAL_AMOUNT,
		FCD.COLLECTED_PRINCIPAL_AMOUNT = AA.AMOUNT_FEE,
		FCD.COLLECTED_INTEREST_AMOUNT = AA.AMOUNT_PRIN,
		FCD.COLLECTED_FEES_AMOUNT = AA.AMOUNT_INT,
		FCD.COLLECTED_OTHER_AMOUNT = AA.AMOUNT_OTHER,
		FCD.PROCESS_TIMESTAMP = :LD_CHECK_POINT_START 
	FROM 
    DEV_ENTERPRISE_LANDING."jaglms"."lms_base_loans" BL
	INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."collection_lms_loan_map" MAP
	LEFT JOIN COLLECTION_PAYMENT_ACT AA  
    WHERE FCD.BASE_LOAN_ID = BL."base_loan_id"
        AND BL."base_loan_id" = MAP."base_loan_id"
        AND BL."base_loan_id" = AA.BASE_LOAN_ID
        AND BL."loan_status" IN (''Originated'' , ''Delinquent'', ''DEFAULT'');


	LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Update'';
    LC_LOG_LABEL      := CONCAT(''Include collected collection payment amount for active loan loans FCT_COLLECTION_DETAIL table'');
    LC_LOG_MESSAGE    := CONCAT(''Update succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
    LC_ERROR_MESSAGE  := NULL;
    LD_END_DATETIME   := CURRENT_TIMESTAMP();
    LN_IS_SUCCESS     := 1;
   
    CALL OP_ADMIN.OPERATIONS.SP_ETL_BATCH_EXECUTION_LOG_POPULATE(
                                                                :LN_BATCH_ID,
                                                                :LC_LOG_TYPE,
                                                                :LC_LOG_LABEL,
                                                                :LC_LOG_MESSAGE,
                                                                :LC_ERROR_MESSAGE,
                                                                :LD_START_DATETIME,
                                                                :LD_END_DATETIME
                                                                );
	
	
-- Include collected collection payment amount for non-active loans

	CREATE TEMP TABLE IF NOT EXISTS COLLECTION_PAYMENT_INACT AS 
	(
		SELECT PS."base_loan_id" AS BASE_LOAN_ID,
				SUM(PSI."total_amount") TOTAL_AMOUNT,
				SUM(PSI."amount_fee") AMOUNT_FEE,
				SUM(PSI."amount_prin") AMOUNT_PRIN,
				SUM(PSI."amount_int") AMOUNT_INT,
				SUM(PSI."amount_other") AMOUNT_OTHER
		FROM DEV_ENTERPRISE_LANDING."jaglms"."lms_payment_schedules" PS
		INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_base_loans" BL ON PS."base_loan_id" = BL."base_loan_id"
		INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_payment_schedule_items" PSI ON PS."payment_schedule_id" = PSI."payment_schedule_id"  AND "is_collections" = 1 
		WHERE BL."loan_status" NOT IN (''Originated'' , ''Delinquent'', ''DEFAULT'')
				AND PSI."status" IN (''Cleared'' , ''SENT'')
				AND PSI."item_type" = ''D''
				AND (PSI."item_date" >= :LD_CHECK_POINT_START
				OR PSI."updated_datetime" >= :LD_CHECK_POINT_START)
		GROUP BY PS."base_loan_id"
	);
    
	UPDATE DWH_DEV.DWH.FCT_COLLECTION_DETAIL FCD
        SET FCD.COLLECTED_TOTAL_AMOUNT = AA.TOTAL_AMOUNT,
            FCD.COLLECTED_PRINCIPAL_AMOUNT = AA.AMOUNT_FEE,
            FCD.COLLECTED_INTEREST_AMOUNT = AA.AMOUNT_PRIN,
            FCD.COLLECTED_FEES_AMOUNT = AA.AMOUNT_INT,
            FCD.COLLECTED_OTHER_AMOUNT = AA.AMOUNT_OTHER,
            FCD.PROCESS_TIMESTAMP = CURRENT_TIMESTAMP() 
    FROM DEV_ENTERPRISE_LANDING."jaglms"."lms_base_loans" BL
		INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."collection_lms_loan_map" MAP
		LEFT JOIN  COLLECTION_PAYMENT_INACT AA
	WHERE FCD.BASE_LOAN_ID = BL."base_loan_id"
        AND BL."base_loan_id" = MAP."base_loan_id"
        AND BL."base_loan_id" = AA.BASE_LOAN_ID
        AND BL."loan_status" NOT IN (''Originated'' , ''Delinquent'', ''DEFAULT'');


	LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Update'';
    LC_LOG_LABEL      := CONCAT(''Include collected collection payment amount for non-active loans FCT_COLLECTION_DETAIL table'');
    LC_LOG_MESSAGE    := CONCAT(''Update succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
    LC_ERROR_MESSAGE  := NULL;
    LD_END_DATETIME   := CURRENT_TIMESTAMP();
    LN_IS_SUCCESS     := 1;
   
    CALL OP_ADMIN.OPERATIONS.SP_ETL_BATCH_EXECUTION_LOG_POPULATE(
                                                                :LN_BATCH_ID,
                                                                :LC_LOG_TYPE,
                                                                :LC_LOG_LABEL,
                                                                :LC_LOG_MESSAGE,
                                                                :LC_ERROR_MESSAGE,
                                                                :LD_START_DATETIME,
                                                                :LD_END_DATETIME
                                                                );
	
	
-- Include outstanding collection payment amount

	UPDATE DWH_DEV.DWH.FCT_COLLECTION_DETAIL 
	SET OUTSTANDING_TOTAL_AMOUNT = TOTAL_AMOUNT - IFNULL(COLLECTED_TOTAL_AMOUNT,0),
		OUTSTANDING_PRINCIPAL_AMOUNT = IFNULL(PRINCIPAL_AMOUNT, 0) - IFNULL(COLLECTED_PRINCIPAL_AMOUNT,0),
		OUTSTANDING_INTEREST_AMOUNT = IFNULL(INTEREST_AMOUNT, 0) - IFNULL(COLLECTED_INTEREST_AMOUNT,0),
		OUTSTANDING_FEES_AMOUNT = IFNULL(FEES_AMOUNT, 0) - IFNULL(COLLECTED_FEES_AMOUNT,0),
		OUTSTANDING_OTHER_AMOUNT = IFNULL(OTHER_AMOUNT, 0) - IFNULL(COLLECTED_OTHER_AMOUNT,0),
		PROCESS_TIMESTAMP = CURRENT_TIMESTAMP() 
	WHERE PROCESS_TIMESTAMP >= :LD_CHECK_POINT_START;

	
	LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Update'';
    LC_LOG_LABEL      := CONCAT(''Include outstanding collection payment amount FCT_COLLECTION_DETAIL table'');
    LC_LOG_MESSAGE    := CONCAT(''Update succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
    LC_ERROR_MESSAGE  := NULL;
    LD_END_DATETIME   := CURRENT_TIMESTAMP();
    LN_IS_SUCCESS     := 1;
   
    CALL OP_ADMIN.OPERATIONS.SP_ETL_BATCH_EXECUTION_LOG_POPULATE(
                                                                :LN_BATCH_ID,
                                                                :LC_LOG_TYPE,
                                                                :LC_LOG_LABEL,
                                                                :LC_LOG_MESSAGE,
                                                                :LC_ERROR_MESSAGE,
                                                                :LD_START_DATETIME,
                                                                :LD_END_DATETIME
                                                                );


-- Add RTC cure info data - uncured RTC info

 
    -- Enable this block once "lms_deferred_outgoing_email" is added to DEV_ENTERPRISE_LANDING db.

	UPDATE DWH_DEV.DWH.FCT_COLLECTION_DETAIL FCD		   
        SET FCD.OPEN_CURE_INFO_ID = CCI."lms_collection_cure_info_id",
            FCD.OPEN_RTC_NOTICE_ID = CCI."rtc_notice",
            FCD.RTC_OUTGOING_DATE = DATE(DOE."outgoing_date"),
            FCD.RTC_DEFAULT_DATE = DATE(CCI."default_time"),
            FCD.RTC_SENT_FLAG = DOE."is_sent", 
            FCD.RTC_CURED_FLAG = CCI."is_cured",
            FCD.RTC_CURE_DATE = CCI."cure_date",
            FCD.PROCESS_TIMESTAMP = CURRENT_TIMESTAMP()
    FROM DEV_ENTERPRISE_LANDING."jaglms"."lms_base_loans" BL 
		   INNER JOIN  DEV_ENTERPRISE_LANDING."jaglms"."lms_collection_cure_info" CCI
		   INNER JOIN (SELECT CCI1."base_loan_id" BASE_LOAN_ID, MAX("lms_collection_cure_info_id") MAX_CURE_INFO_ID FROM DEV_ENTERPRISE_LANDING."jaglms"."lms_collection_cure_info" CCI1 
						 INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_base_loans" BL1 ON CCI1."base_loan_id" = BL1."base_loan_id"
						   WHERE BL1."loan_status" IN (''Originated'', ''Delinquent'', ''DEFAULT'') AND CCI1."is_cured" = 0 AND CCI1."is_enabled" = 1 GROUP BY CCI1."base_loan_id") AA
		   INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_deferred_outgoing_email" DOE
	WHERE FCD.BASE_LOAN_ID = BL."base_loan_id"
        AND BL."base_loan_id" = CCI."base_loan_id" AND CCI."is_cured" = 0 AND CCI."is_enabled" = 1
        AND CCI."base_loan_id" = AA.BASE_LOAN_ID AND CCI."lms_collection_cure_info_id" = AA.MAX_CURE_INFO_ID
        AND DOE."lms_deferred_outgoing_email_id" = CCI."rtc_notice"
        AND BL."loan_status" IN (''Originated'', ''Delinquent'', ''DEFAULT'') AND PROCESS_TIMESTAMP >= :LD_CHECK_POINT_START;


	LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Update'';
    LC_LOG_LABEL      := CONCAT(''Add RTC cure info data - uncured RTC info FCT_COLLECTION_DETAIL table'');
    LC_LOG_MESSAGE    := CONCAT(''Update succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
    LC_ERROR_MESSAGE  := NULL;
    LD_END_DATETIME   := CURRENT_TIMESTAMP();
    LN_IS_SUCCESS     := 1;
   
    CALL OP_ADMIN.OPERATIONS.SP_ETL_BATCH_EXECUTION_LOG_POPULATE(
                                                                :LN_BATCH_ID,
                                                                :LC_LOG_TYPE,
                                                                :LC_LOG_LABEL,
                                                                :LC_LOG_MESSAGE,
                                                                :LC_ERROR_MESSAGE,
                                                                :LD_START_DATETIME,
                                                                :LD_END_DATETIME
                                                                );
	
-- Sync this status code and sub code
    
	UPDATE DWH_DEV.DWH.FCT_COLLECTION_DETAIL FCD
		SET FCD.STATUS_CODE = C."status_code", 
			FCD.SUB_STATUS_CODE = C."sub_status_code"
	FROM DEV_ENTERPRISE_LANDING."jaglms"."collections" C
    WHERE FCD.COLLECTION_ID = C."collection_id" 
        AND (FCD.STATUS_CODE <> C."status_code" OR FCD.SUB_STATUS_CODE <> C."sub_status_code"); 

	  
	LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Update'';
    LC_LOG_LABEL      := CONCAT(''Sync ths status code and sub code in FCT_COLLECTION_DETAIL table'');
    LC_LOG_MESSAGE    := CONCAT(''Update succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
    LC_ERROR_MESSAGE  := NULL;
    LD_END_DATETIME   := CURRENT_TIMESTAMP();
    LN_IS_SUCCESS     := 1;
   
    CALL OP_ADMIN.OPERATIONS.SP_ETL_BATCH_EXECUTION_LOG_POPULATE(
                                                                :LN_BATCH_ID,
                                                                :LC_LOG_TYPE,
                                                                :LC_LOG_LABEL,
                                                                :LC_LOG_MESSAGE,
                                                                :LC_ERROR_MESSAGE,
                                                                :LD_START_DATETIME,
                                                                :LD_END_DATETIME
                                                                );

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
    

  	RETURN ''SP_COLLECTION_DETAIL : Job is done'';

	EXCEPTION

   	WHEN OTHER THEN

      LN_ROW_COUNT      := NULL;
      LC_LOG_TYPE       := ''Merge'';
      LC_LOG_LABEL      := CONCAT(''Merge data into '', :LC_TABLE_NAME, '' table'');
      LC_LOG_MESSAGE    := ''Merge failed'';
      LC_ERROR_MESSAGE  := SUBSTR(CONCAT(SQLCODE, '' - '', SQLERRM), 1, 500);
      LD_END_DATETIME   := CURRENT_TIMESTAMP();
      LN_IS_SUCCESS     := 0;

      CALL OP_ADMIN.OPERATIONS.SP_ETL_BATCH_EXECUTION_LOG_POPULATE (
                                                                    :LN_BATCH_ID,
                                                                    :LC_LOG_TYPE,
                                                                    :LC_LOG_LABEL,
                                                                    :LC_LOG_MESSAGE,
                                                                    :LC_ERROR_MESSAGE,
                                                                    :LD_START_DATETIME,
                                                                    :LD_END_DATETIME
                                                                    );

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