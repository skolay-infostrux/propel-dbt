{% macro sp_payment_schedule() %}

{% set prepare_database = get_prepare_database() %}
{% set sql %}

CREATE OR REPLACE PROCEDURE {{ prepare_database }}.DWH.SP_PAYMENT_SCHEDULE()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE 

    ORGANIZATION_ID            INTEGER;
    PAYMENT_SCHEDULE_ITEM_ID   INTEGER;
    LN_JOB_ID                  INT;
    LN_BATCH_ID                INT;
    LN_ROW_COUNT               SMALLINT;
    LC_JOB_NAME                VARCHAR(200)   DEFAULT ''SP_PAYMENT_SCHEDULE'';
    LC_BATCH_TYPE              VARCHAR(100)   DEFAULT ''DELTA'';
    LC_BATCH_LABEL             VARCHAR(500)   DEFAULT ''Populate Insurance Dim tables DIM_PAYMENT_SCHEDULE & DIM_PAYMENT_SCHEDULE_REMOVED'';
    LC_CHECK_POINT_TYPE        VARCHAR(50)    DEFAULT ''TIMESTAMP'';
    LD_CHECK_POINT_START       DATETIME;
    LD_CHECK_POINT_END         DATETIME;
    LN_IS_SUCCESS              SMALLINT;
    LD_START_DATETIME          DATETIME;
    LD_END_DATETIME            DATETIME;
    LC_ERROR_MESSAGE           VARCHAR(500);
    LC_TABLE_NAME              VARCHAR(100)   DEFAULT ''DIM_PAYMENT_SCHEDULE'';
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
    LC_TABLE_NAME        := ''DIM_PAYMENT_SCHEDULE'';
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
-- step 2 - Merge data from source tables into Dim tables
-- -------------------------------------------------------------------------------------------------------------------------------------------------------


-- Insert new payment schedule and update existing payment schedule for MK
        
    ORGANIZATION_ID := 1;   

    MERGE INTO DWH_DEV.DWH.dim_payment_schedule target USING (  
    WITH CTE AS (   
         SELECT psi."payment_schedule_item_id" AS payment_schedule_item_id,
                psi."payment_schedule_id" payment_schedule_id,
                psi."item_date" AS item_date,
                psi."payment_mode" AS payment_mode,
                psi."status" AS status,
                psi."total_amount" AS total_amount,
                psi."amount_fee" AS amount_fee,
                psi."amount_int" AS amount_int,
                psi."amount_prin" AS amount_prin,
                psi."amount_other" AS amount_other,
                (CASE 
                    WHEN psi."item_type" IS NULL AND psi."total_amount" >= 0 AND psi."status" NOT IN (''scheduled'', ''bypass'', ''cancelled'') 
                        THEN ''D''
                    WHEN psi."item_type" IS NULL AND psi."total_amount" < 0 AND psi."status" NOT IN (''scheduled'', ''bypass'', ''cancelled'') 
                        THEN ''C''
                 ELSE psi."item_type" END) AS item_type,
                psi."amount_disc" AS amount_disc,
                psi."outstanding_fee_amount" AS outstanding_fee_amount,
                psi."is_pif" AS is_pif,
                psi."is_offcycle" AS is_offcycle,
                psi."payment_sequence" AS payment_sequence,
                psi."cure_info_id" AS cure_info_id,
                psi."is_cure_master" AS is_cure_master,
                psi."payment_failed_date" AS payment_failed_date,
                psi."reference_text" AS reference_text,
                psi."is_created_by_customer_online" AS is_created_by_customer_online,
                psi."is_created_by_ivr_payment" AS is_created_by_ivr_payment,
                psi."original_item_date" AS original_item_date,
                psi."original_total_amount" AS original_total_amount,
                psi."original_fee_amount" AS original_fee_amount,
                psi."original_interest_amount" AS original_interest_amount,
                psi."original_principal_amount" AS original_principal_amount,
                psi."original_other_amount" AS original_other_amount,
                psi."original_outstanding_fee_amount" AS original_outstanding_fee_amount,
                psi."split_from_payment_schedule_item_id" AS split_from_payment_schedule_item_id,
                psi."online_payment_split_total_amount" AS online_payment_split_total_amount,
                psi."online_payment_split_fee_amount" AS online_payment_split_fee_amount,
                psi."online_payment_split_interest_amount" AS online_payment_split_interest_amount,
                psi."online_payment_split_principal_amount" AS online_payment_split_principal_amount,
                psi."online_payment_split_other_amount" AS online_payment_split_other_amount,
                ps."customer_id" AS customer_id,
                ps."base_loan_id" AS base_loan_id,
                ci."organization_id" AS organization_id,
                ps."initial_apr" AS initial_apr,
                IFNULL(ps."is_active",0) AS is_active ,
                IFNULL(ps."is_collections",0) AS is_collections,
                ps."stored_payment_use" AS stored_payment_use,
                ps."theoretical_sep_amt" AS theoretical_sep_amt,
                psi."auxiliary_type" AS auxiliary_type, -- DAT-6692
                psi."created_datetime" AS created_datetime,  
                psi."updated_datetime" AS updated_datetime,
                CURRENT_TIMESTAMP AS process_timestamp 
      FROM DEV_ENTERPRISE_LANDING."jaglms"."lms_payment_schedules" ps
        INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_base_loans" bl ON ps."base_loan_id" = bl."base_loan_id"
        INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_customer_info_flat" ci ON bl."customer_id" = ci."customer_id" 
        INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_payment_schedule_items" psi ON ps."payment_schedule_id" = psi."payment_schedule_id"
        WHERE ci."organization_id" = :ORGANIZATION_ID
        --AND psi."updated_datetime" > :LD_CHECK_POINT_START AND psi."updated_datetime" <= :LD_CHECK_POINT_END -- new incremental logic
        AND  (
                 (psi._SNOWFLAKE_INSERTED_AT > :LD_CHECK_POINT_START and psi._SNOWFLAKE_INSERTED_AT <= :LD_CHECK_POINT_END) OR
                 (psi._SNOWFLAKE_UPDATED_AT > :LD_CHECK_POINT_START and psi._SNOWFLAKE_UPDATED_AT <= :LD_CHECK_POINT_END)
              )
        )
       SELECT * FROM CTE
    ) source
        ON target.PAYMENT_SCHEDULE_ITEM_ID = source.PAYMENT_SCHEDULE_ITEM_ID
        WHEN MATCHED THEN 
            UPDATE SET         
                TARGET.PAYMENT_SCHEDULE_ID = SOURCE.PAYMENT_SCHEDULE_ID,
                TARGET.ITEM_DATE = SOURCE.ITEM_DATE,
                TARGET.PAYMENT_MODE = SOURCE.PAYMENT_MODE,
                TARGET.STATUS = SOURCE.STATUS,
                TARGET.TOTAL_AMOUNT = SOURCE.TOTAL_AMOUNT,
                TARGET.AMOUNT_FEE = SOURCE.AMOUNT_FEE,
                TARGET.AMOUNT_INT = SOURCE.AMOUNT_INT,
                TARGET.AMOUNT_PRIN = SOURCE.AMOUNT_PRIN,
                TARGET.AMOUNT_OTHER = SOURCE.AMOUNT_OTHER,
                TARGET.ITEM_TYPE = SOURCE.ITEM_TYPE,
                TARGET.AMOUNT_DISC = SOURCE.AMOUNT_DISC,
                TARGET.OUTSTANDING_FEE_AMOUNT = SOURCE.OUTSTANDING_FEE_AMOUNT,
                TARGET.IS_PIF = SOURCE.IS_PIF,
                TARGET.IS_OFFCYCLE = SOURCE.IS_OFFCYCLE,
                TARGET.PAYMENT_SEQUENCE =  SOURCE.PAYMENT_SEQUENCE,
                TARGET.CURE_INFO_ID = SOURCE.CURE_INFO_ID,
                TARGET.IS_CURE_MASTER = SOURCE.IS_CURE_MASTER,
                TARGET.PAYMENT_FAILED_DATE = SOURCE.PAYMENT_FAILED_DATE,
                TARGET.REFERENCE_TEXT = SOURCE.REFERENCE_TEXT,
                TARGET.IS_CREATED_BY_CUSTOMER_ONLINE = SOURCE.IS_CREATED_BY_CUSTOMER_ONLINE,
                TARGET.IS_CREATED_BY_IVR_PAYMENT  = SOURCE.IS_CREATED_BY_IVR_PAYMENT , -- DAT-4723
                TARGET.ORIGINAL_ITEM_DATE = SOURCE.ORIGINAL_ITEM_DATE,
                TARGET.ORIGINAL_TOTAL_AMOUNT = SOURCE.ORIGINAL_TOTAL_AMOUNT,
                TARGET.ORIGINAL_FEE_AMOUNT = SOURCE.ORIGINAL_FEE_AMOUNT,
                TARGET.ORIGINAL_INTEREST_AMOUNT = SOURCE.ORIGINAL_INTEREST_AMOUNT,
                TARGET.ORIGINAL_PRINCIPAL_AMOUNT = SOURCE.ORIGINAL_PRINCIPAL_AMOUNT,
                TARGET.ORIGINAL_OTHER_AMOUNT = SOURCE.ORIGINAL_OTHER_AMOUNT,
                TARGET.ORIGINAL_OUTSTANDING_FEE_AMOUNT = SOURCE.ORIGINAL_OUTSTANDING_FEE_AMOUNT,            
                TARGET.SPLIT_FROM_PAYMENT_SCHEDULE_ITEM_ID = SOURCE.SPLIT_FROM_PAYMENT_SCHEDULE_ITEM_ID,
                TARGET.ONLINE_PAYMENT_SPLIT_TOTAL_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_TOTAL_AMOUNT,
                TARGET.ONLINE_PAYMENT_SPLIT_FEE_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_FEE_AMOUNT,
                TARGET.ONLINE_PAYMENT_SPLIT_INTEREST_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_INTEREST_AMOUNT,
                TARGET.ONLINE_PAYMENT_SPLIT_PRINCIPAL_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_PRINCIPAL_AMOUNT,
                TARGET.ONLINE_PAYMENT_SPLIT_OTHER_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_OTHER_AMOUNT,
                TARGET.CUSTOMER_ID = SOURCE.CUSTOMER_ID,
                TARGET.BASE_LOAN_ID = SOURCE.BASE_LOAN_ID,
                TARGET.ORGANIZATION_ID = SOURCE.ORGANIZATION_ID,
                TARGET.INITIAL_APR = SOURCE.INITIAL_APR,
                TARGET.IS_ACTIVE = SOURCE.IS_ACTIVE,
                TARGET.IS_COLLECTIONS = SOURCE.IS_COLLECTIONS,
                TARGET.STORED_PAYMENT_USE = SOURCE.STORED_PAYMENT_USE,
                TARGET.AUXILIARY_TYPE = SOURCE.AUXILIARY_TYPE,
                TARGET.CREATED_DATETIME =  SOURCE.CREATED_DATETIME,  
                TARGET.UPDATED_DATETIME = SOURCE.UPDATED_DATETIME,
                TARGET.PROCESS_TIMESTAMP = SOURCE.PROCESS_TIMESTAMP
        WHEN NOT MATCHED THEN 
            INSERT (
                    PAYMENT_SCHEDULE_ITEM_ID,
                    PAYMENT_SCHEDULE_ID,
                    ITEM_DATE,
                    PAYMENT_MODE,
                    STATUS,
                    TOTAL_AMOUNT,
                    AMOUNT_FEE,
                    AMOUNT_INT,
                    AMOUNT_PRIN,
                    AMOUNT_OTHER,
                    ITEM_TYPE,
                    AMOUNT_DISC,
                    OUTSTANDING_FEE_AMOUNT,
                    IS_PIF,
                    IS_OFFCYCLE,
                    PAYMENT_SEQUENCE,
                    CURE_INFO_ID,
                    IS_CURE_MASTER,
                    PAYMENT_FAILED_DATE,
                    REFERENCE_TEXT,
                    IS_CREATED_BY_CUSTOMER_ONLINE,
                    IS_CREATED_BY_IVR_PAYMENT,
                    ORIGINAL_ITEM_DATE,
                    ORIGINAL_TOTAL_AMOUNT,
                    ORIGINAL_FEE_AMOUNT,
                    ORIGINAL_INTEREST_AMOUNT,
                    ORIGINAL_PRINCIPAL_AMOUNT,
                    ORIGINAL_OTHER_AMOUNT,
                    ORIGINAL_OUTSTANDING_FEE_AMOUNT,
                    SPLIT_FROM_PAYMENT_SCHEDULE_ITEM_ID,
                    ONLINE_PAYMENT_SPLIT_TOTAL_AMOUNT,
                    ONLINE_PAYMENT_SPLIT_FEE_AMOUNT,
                    ONLINE_PAYMENT_SPLIT_INTEREST_AMOUNT,
                    ONLINE_PAYMENT_SPLIT_PRINCIPAL_AMOUNT,
                    ONLINE_PAYMENT_SPLIT_OTHER_AMOUNT,
                    CUSTOMER_ID,
                    BASE_LOAN_ID,
                    ORGANIZATION_ID,
                    INITIAL_APR,
                    IS_ACTIVE,
                    IS_COLLECTIONS,
                    STORED_PAYMENT_USE,
                    THEORETICAL_SEP_AMT,
                    AUXILIARY_TYPE,
                    CREATED_DATETIME,  
                    UPDATED_DATETIME,  
                    PROCESS_TIMESTAMP)
             VALUES( SOURCE.PAYMENT_SCHEDULE_ITEM_ID,
                    SOURCE.PAYMENT_SCHEDULE_ID,
                    SOURCE.ITEM_DATE,
                    SOURCE.PAYMENT_MODE,
                    SOURCE.STATUS,
                    SOURCE.TOTAL_AMOUNT,
                    SOURCE.AMOUNT_FEE,
                    SOURCE.AMOUNT_INT,
                    SOURCE.AMOUNT_PRIN,
                    SOURCE.AMOUNT_OTHER,
                    SOURCE.ITEM_TYPE,
                    SOURCE.AMOUNT_DISC,
                    SOURCE.OUTSTANDING_FEE_AMOUNT,
                    SOURCE.IS_PIF,
                    SOURCE.IS_OFFCYCLE,
                    SOURCE.PAYMENT_SEQUENCE,
                    SOURCE.CURE_INFO_ID,
                    SOURCE.IS_CURE_MASTER,
                    SOURCE.PAYMENT_FAILED_DATE,
                    SOURCE.REFERENCE_TEXT,
                    SOURCE.IS_CREATED_BY_CUSTOMER_ONLINE,
                    SOURCE.IS_CREATED_BY_IVR_PAYMENT,
                    SOURCE.ORIGINAL_ITEM_DATE,
                    SOURCE.ORIGINAL_TOTAL_AMOUNT,
                    SOURCE.ORIGINAL_FEE_AMOUNT,
                    SOURCE.ORIGINAL_INTEREST_AMOUNT,
                    SOURCE.ORIGINAL_PRINCIPAL_AMOUNT,
                    SOURCE.ORIGINAL_OTHER_AMOUNT,
                    SOURCE.ORIGINAL_OUTSTANDING_FEE_AMOUNT,
                    SOURCE.SPLIT_FROM_PAYMENT_SCHEDULE_ITEM_ID,
                    SOURCE.ONLINE_PAYMENT_SPLIT_TOTAL_AMOUNT,
                    SOURCE.ONLINE_PAYMENT_SPLIT_FEE_AMOUNT,
                    SOURCE.ONLINE_PAYMENT_SPLIT_INTEREST_AMOUNT,
                    SOURCE.ONLINE_PAYMENT_SPLIT_PRINCIPAL_AMOUNT,
                    SOURCE.ONLINE_PAYMENT_SPLIT_OTHER_AMOUNT,
                    SOURCE.CUSTOMER_ID,
                    SOURCE.BASE_LOAN_ID,
                    SOURCE.ORGANIZATION_ID,
                    SOURCE.INITIAL_APR,
                    SOURCE.IS_ACTIVE,
                    SOURCE.IS_COLLECTIONS,
                    SOURCE.STORED_PAYMENT_USE,
                    SOURCE.THEORETICAL_SEP_AMT,
                    SOURCE.AUXILIARY_TYPE,
                    SOURCE.CREATED_DATETIME,  
                    SOURCE.UPDATED_DATETIME,  
                    SOURCE.PROCESS_TIMESTAMP);

    LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Merge'';
    LC_LOG_LABEL      := CONCAT(''Merge data into DIM_PAYMENT_SCHEDULE table for organization id = 1'');
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

    LC_TABLE_NAME        := ''DIM_PAYMENT_SCHEDULE'';
    LN_IS_SUCCESS        := NULL;
    LD_START_DATETIME    := CURRENT_TIMESTAMP();
    LD_END_DATETIME      := NULL;                                                            

-- Insert new payment schedule and update existing payment schedule for CF
        
    ORGANIZATION_ID := 2;   

    MERGE INTO DWH_DEV.DWH.dim_payment_schedule target USING (  
    WITH CTE AS (   
         SELECT psi."payment_schedule_item_id" AS payment_schedule_item_id,
                psi."payment_schedule_id" AS payment_schedule_id,
                psi."item_date" AS item_date,
                psi."payment_mode" AS payment_mode,
                psi."status" AS status,
                psi."total_amount" AS total_amount,
                psi."amount_fee" AS amount_fee,
                psi."amount_int" AS amount_int,
                psi."amount_prin" AS amount_prin,
                psi."amount_other" AS amount_other,
                (CASE 
                    WHEN psi."item_type" IS NULL AND psi."total_amount" >= 0 AND psi."status" NOT IN (''scheduled'', ''bypass'', ''cancelled'') 
                        THEN ''D''
                    WHEN psi."item_type" IS NULL AND psi."total_amount" < 0 AND psi."status" NOT IN (''scheduled'', ''bypass'', ''cancelled'') 
                        THEN ''C''
                 ELSE psi."item_type" end) AS item_type,
                psi."amount_disc" AS amount_disc,
                psi."outstanding_fee_amount" AS outstanding_fee_amount,
                psi."is_pif" AS is_pif,
                psi."is_offcycle" AS is_offcycle,
                psi."payment_sequence" AS payment_sequence,
                psi."cure_info_id" AS cure_info_id,
                psi."is_cure_master" AS is_cure_master,
                psi."payment_failed_date" AS payment_failed_date,
                psi."reference_text" AS reference_text,
                psi."is_created_by_customer_online" AS is_created_by_customer_online,
                psi."is_created_by_ivr_payment" AS is_created_by_ivr_payment,
                psi."original_item_date" AS original_item_date,
                psi."original_total_amount" AS original_total_amount,
                psi."original_fee_amount" AS original_fee_amount,
                psi."original_interest_amount" AS original_interest_amount,
                psi."original_principal_amount" AS original_principal_amount,
                psi."original_other_amount" AS original_other_amount,
                psi."original_outstanding_fee_amount" AS original_outstanding_fee_amount,
                psi."split_from_payment_schedule_item_id" AS split_from_payment_schedule_item_id,
                psi."online_payment_split_total_amount" AS online_payment_split_total_amount,
                psi."online_payment_split_fee_amount" AS online_payment_split_fee_amount,
                psi."online_payment_split_interest_amount" AS online_payment_split_interest_amount,
                psi."online_payment_split_principal_amount" AS online_payment_split_principal_amount,
                psi."online_payment_split_other_amount" AS online_payment_split_other_amount,
                ps."customer_id" AS customer_id,
                ps."base_loan_id" AS base_loan_id,
                ci."organization_id" AS organization_id,
                ps."initial_apr" AS initial_apr,
                IFNULL(ps."is_active",0) AS is_active,
                IFNULL(ps."is_collections",0) AS is_collections,
                ps."stored_payment_use" AS stored_payment_use,
                ps."theoretical_sep_amt" AS theoretical_sep_amt,
                psi."auxiliary_type" AS auxiliary_type, -- DAT-6692
                psi."created_datetime" AS created_datetime,  
                psi."updated_datetime" AS updated_datetime,
                CURRENT_TIMESTAMP AS process_timestamp 
      FROM DEV_ENTERPRISE_LANDING."jaglms"."lms_payment_schedules" ps
        INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_base_loans" bl ON ps."base_loan_id" = bl."base_loan_id"
        INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_customer_info_flat" ci ON bl."customer_id" = ci."customer_id" 
        INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_payment_schedule_items" psi ON ps."payment_schedule_id" = psi."payment_schedule_id"
        WHERE ci."organization_id" = :ORGANIZATION_ID
        -- AND psi."updated_datetime" > :LD_CHECK_POINT_START AND psi."updated_datetime" <= :LD_CHECK_POINT_END -- new incremental logic
        AND (
                 (psi._SNOWFLAKE_INSERTED_AT > :LD_CHECK_POINT_START and psi._SNOWFLAKE_INSERTED_AT <= :LD_CHECK_POINT_END) OR
                 (psi._SNOWFLAKE_UPDATED_AT > :LD_CHECK_POINT_START and psi._SNOWFLAKE_UPDATED_AT <= :LD_CHECK_POINT_END)
              )
        )
       SELECT * FROM CTE
    ) source
        ON target.PAYMENT_SCHEDULE_ITEM_ID = source.PAYMENT_SCHEDULE_ITEM_ID
        WHEN MATCHED THEN 
            UPDATE SET         
                TARGET.PAYMENT_SCHEDULE_ID = SOURCE.PAYMENT_SCHEDULE_ID,
                TARGET.ITEM_DATE = SOURCE.ITEM_DATE,
                TARGET.PAYMENT_MODE = SOURCE.PAYMENT_MODE,
                TARGET.STATUS = SOURCE.STATUS,
                TARGET.TOTAL_AMOUNT = SOURCE.TOTAL_AMOUNT,
                TARGET.AMOUNT_FEE = SOURCE.AMOUNT_FEE,
                TARGET.AMOUNT_INT = SOURCE.AMOUNT_INT,
                TARGET.AMOUNT_PRIN = SOURCE.AMOUNT_PRIN,
                TARGET.AMOUNT_OTHER = SOURCE.AMOUNT_OTHER,
                TARGET.ITEM_TYPE = SOURCE.ITEM_TYPE,
                TARGET.AMOUNT_DISC = SOURCE.AMOUNT_DISC,
                TARGET.OUTSTANDING_FEE_AMOUNT = SOURCE.OUTSTANDING_FEE_AMOUNT,
                TARGET.IS_PIF = SOURCE.IS_PIF,
                TARGET.IS_OFFCYCLE = SOURCE.IS_OFFCYCLE,    
                TARGET.PAYMENT_SEQUENCE =  SOURCE.PAYMENT_SEQUENCE,
                TARGET.CURE_INFO_ID = SOURCE.CURE_INFO_ID,
                TARGET.IS_CURE_MASTER = SOURCE.IS_CURE_MASTER,
                TARGET.PAYMENT_FAILED_DATE = SOURCE.PAYMENT_FAILED_DATE,
                TARGET.REFERENCE_TEXT = SOURCE.REFERENCE_TEXT,
                TARGET.IS_CREATED_BY_CUSTOMER_ONLINE = SOURCE.IS_CREATED_BY_CUSTOMER_ONLINE,
                TARGET.IS_CREATED_BY_IVR_PAYMENT  = SOURCE.IS_CREATED_BY_IVR_PAYMENT , -- DAT-4723
                TARGET.ORIGINAL_ITEM_DATE = SOURCE.ORIGINAL_ITEM_DATE,
                TARGET.ORIGINAL_TOTAL_AMOUNT = SOURCE.ORIGINAL_TOTAL_AMOUNT,
                TARGET.ORIGINAL_FEE_AMOUNT = SOURCE.ORIGINAL_FEE_AMOUNT,
                TARGET.ORIGINAL_INTEREST_AMOUNT = SOURCE.ORIGINAL_INTEREST_AMOUNT,
                TARGET.ORIGINAL_PRINCIPAL_AMOUNT = SOURCE.ORIGINAL_PRINCIPAL_AMOUNT,
                TARGET.ORIGINAL_OTHER_AMOUNT = SOURCE.ORIGINAL_OTHER_AMOUNT,
                TARGET.ORIGINAL_OUTSTANDING_FEE_AMOUNT = SOURCE.ORIGINAL_OUTSTANDING_FEE_AMOUNT,            
                TARGET.SPLIT_FROM_PAYMENT_SCHEDULE_ITEM_ID = SOURCE.SPLIT_FROM_PAYMENT_SCHEDULE_ITEM_ID,
                TARGET.ONLINE_PAYMENT_SPLIT_TOTAL_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_TOTAL_AMOUNT,
                TARGET.ONLINE_PAYMENT_SPLIT_FEE_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_FEE_AMOUNT,
                TARGET.ONLINE_PAYMENT_SPLIT_INTEREST_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_INTEREST_AMOUNT,
                TARGET.ONLINE_PAYMENT_SPLIT_PRINCIPAL_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_PRINCIPAL_AMOUNT,
                TARGET.ONLINE_PAYMENT_SPLIT_OTHER_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_OTHER_AMOUNT,
                TARGET.CUSTOMER_ID = SOURCE.CUSTOMER_ID,
                TARGET.BASE_LOAN_ID = SOURCE.BASE_LOAN_ID,
                TARGET.ORGANIZATION_ID = SOURCE.ORGANIZATION_ID,
                TARGET.INITIAL_APR = SOURCE.INITIAL_APR,
                TARGET.IS_ACTIVE = SOURCE.IS_ACTIVE,
                TARGET.IS_COLLECTIONS = SOURCE.IS_COLLECTIONS,
                TARGET.STORED_PAYMENT_USE = SOURCE.STORED_PAYMENT_USE,
                TARGET.AUXILIARY_TYPE = SOURCE.AUXILIARY_TYPE,
                TARGET.CREATED_DATETIME =  SOURCE.CREATED_DATETIME,  
                TARGET.UPDATED_DATETIME = SOURCE.UPDATED_DATETIME,
                TARGET.PROCESS_TIMESTAMP = SOURCE.PROCESS_TIMESTAMP
        WHEN NOT MATCHED THEN 
            INSERT (
                    PAYMENT_SCHEDULE_ITEM_ID,
                    PAYMENT_SCHEDULE_ID,
                    ITEM_DATE,
                    PAYMENT_MODE,
                    STATUS,
                    TOTAL_AMOUNT,
                    AMOUNT_FEE,
                    AMOUNT_INT,
                    AMOUNT_PRIN,
                    AMOUNT_OTHER,
                    ITEM_TYPE,
                    AMOUNT_DISC,
                    OUTSTANDING_FEE_AMOUNT,
                    IS_PIF,
                    IS_OFFCYCLE,
                    PAYMENT_SEQUENCE,
                    CURE_INFO_ID,
                    IS_CURE_MASTER,
                    PAYMENT_FAILED_DATE,
                    REFERENCE_TEXT,
                    IS_CREATED_BY_CUSTOMER_ONLINE,
                    IS_CREATED_BY_IVR_PAYMENT,
                    ORIGINAL_ITEM_DATE,
                    ORIGINAL_TOTAL_AMOUNT,
                    ORIGINAL_FEE_AMOUNT,
                    ORIGINAL_INTEREST_AMOUNT,
                    ORIGINAL_PRINCIPAL_AMOUNT,
                    ORIGINAL_OTHER_AMOUNT,
                    ORIGINAL_OUTSTANDING_FEE_AMOUNT,
                    SPLIT_FROM_PAYMENT_SCHEDULE_ITEM_ID,
                    ONLINE_PAYMENT_SPLIT_TOTAL_AMOUNT,
                    ONLINE_PAYMENT_SPLIT_FEE_AMOUNT,
                    ONLINE_PAYMENT_SPLIT_INTEREST_AMOUNT,
                    ONLINE_PAYMENT_SPLIT_PRINCIPAL_AMOUNT,
                    ONLINE_PAYMENT_SPLIT_OTHER_AMOUNT,
                    CUSTOMER_ID,
                    BASE_LOAN_ID,
                    ORGANIZATION_ID,
                    INITIAL_APR,
                    IS_ACTIVE,
                    IS_COLLECTIONS,
                    STORED_PAYMENT_USE,
                    THEORETICAL_SEP_AMT,
                    AUXILIARY_TYPE,
                    CREATED_DATETIME,  
                    UPDATED_DATETIME,  
                    PROCESS_TIMESTAMP)
             VALUES( SOURCE.PAYMENT_SCHEDULE_ITEM_ID,
                    SOURCE.PAYMENT_SCHEDULE_ID,
                    SOURCE.ITEM_DATE,
                    SOURCE.PAYMENT_MODE,
                    SOURCE.STATUS,
                    SOURCE.TOTAL_AMOUNT,
                    SOURCE.AMOUNT_FEE,
                    SOURCE.AMOUNT_INT,
                    SOURCE.AMOUNT_PRIN,
                    SOURCE.AMOUNT_OTHER,
                    SOURCE.ITEM_TYPE,
                    SOURCE.AMOUNT_DISC,
                    SOURCE.OUTSTANDING_FEE_AMOUNT,
                    SOURCE.IS_PIF,
                    SOURCE.IS_OFFCYCLE,
                    SOURCE.PAYMENT_SEQUENCE,
                    SOURCE.CURE_INFO_ID,
                    SOURCE.IS_CURE_MASTER,
                    SOURCE.PAYMENT_FAILED_DATE,
                    SOURCE.REFERENCE_TEXT,
                    SOURCE.IS_CREATED_BY_CUSTOMER_ONLINE,
                    SOURCE.IS_CREATED_BY_IVR_PAYMENT,
                    SOURCE.ORIGINAL_ITEM_DATE,
                    SOURCE.ORIGINAL_TOTAL_AMOUNT,
                    SOURCE.ORIGINAL_FEE_AMOUNT,
                    SOURCE.ORIGINAL_INTEREST_AMOUNT,
                    SOURCE.ORIGINAL_PRINCIPAL_AMOUNT,
                    SOURCE.ORIGINAL_OTHER_AMOUNT,
                    SOURCE.ORIGINAL_OUTSTANDING_FEE_AMOUNT,
                    SOURCE.SPLIT_FROM_PAYMENT_SCHEDULE_ITEM_ID,
                    SOURCE.ONLINE_PAYMENT_SPLIT_TOTAL_AMOUNT,
                    SOURCE.ONLINE_PAYMENT_SPLIT_FEE_AMOUNT,
                    SOURCE.ONLINE_PAYMENT_SPLIT_INTEREST_AMOUNT,
                    SOURCE.ONLINE_PAYMENT_SPLIT_PRINCIPAL_AMOUNT,
                    SOURCE.ONLINE_PAYMENT_SPLIT_OTHER_AMOUNT,
                    SOURCE.CUSTOMER_ID,
                    SOURCE.BASE_LOAN_ID,
                    SOURCE.ORGANIZATION_ID,
                    SOURCE.INITIAL_APR,
                    SOURCE.IS_ACTIVE,
                    SOURCE.IS_COLLECTIONS,
                    SOURCE.STORED_PAYMENT_USE,
                    SOURCE.THEORETICAL_SEP_AMT,
                    SOURCE.AUXILIARY_TYPE,
                    SOURCE.CREATED_DATETIME,  
                    SOURCE.UPDATED_DATETIME,  
                    SOURCE.PROCESS_TIMESTAMP);

    LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Merge'';
    LC_LOG_LABEL      := CONCAT(''Merge data into DIM_PAYMENT_SCHEDULE table for organization id = 2'');
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

    LC_TABLE_NAME        := ''DIM_PAYMENT_SCHEDULE'';
    LN_IS_SUCCESS        := NULL;
    LD_START_DATETIME    := CURRENT_TIMESTAMP();
    LD_END_DATETIME      := NULL;                                                             


-- Insert new payment schedule and update existing payment schedule for CC/FEB
        
    -- ORGANIZATION_ID IN (3,4);    

    MERGE INTO DWH_DEV.DWH.dim_payment_schedule target USING (  
    WITH CTE AS (   
         SELECT psi."payment_schedule_item_id" AS payment_schedule_item_id,
                psi."payment_schedule_id" AS payment_schedule_id,
                psi."item_date" AS item_date,
                psi."payment_mode" AS payment_mode,
                psi."status" AS status,
                psi."total_amount" AS total_amount,
                psi."amount_fee" AS amount_fee,
                psi."amount_int" AS amount_int,
                psi."amount_prin" AS amount_prin,
                psi."amount_other" AS amount_other,
                (CASE 
                    WHEN psi."item_type" IS NULL AND psi."total_amount" >= 0 AND psi."status" NOT IN (''scheduled'', ''bypass'', ''cancelled'') 
                        THEN ''D''
                    WHEN psi."item_type" IS NULL AND psi."total_amount" < 0 AND psi."status" NOT IN (''scheduled'', ''bypass'', ''cancelled'') 
                        THEN ''C''
                 ELSE psi."item_type" END) AS item_type,
                psi."amount_disc" AS amount_disc,
                psi."outstanding_fee_amount" AS outstanding_fee_amount,
                psi."is_pif" AS is_pif,
                psi."is_offcycle" AS is_offcycle,
                psi."payment_sequence" AS payment_sequence,
                psi."cure_info_id" AS cure_info_id,
                psi."is_cure_master" AS is_cure_master,
                psi."payment_failed_date" AS payment_failed_date,
                psi."reference_text" AS reference_text,
                psi."is_created_by_customer_online" AS is_created_by_customer_online,
                psi."is_created_by_ivr_payment" AS is_created_by_ivr_payment,
                psi."original_item_date" AS original_item_date,
                psi."original_total_amount" AS original_total_amount,
                psi."original_fee_amount" AS original_fee_amount,
                psi."original_interest_amount" AS original_interest_amount,
                psi."original_principal_amount" AS original_principal_amount,
                psi."original_other_amount" AS original_other_amount,
                psi."original_outstanding_fee_amount" AS original_outstanding_fee_amount,
                psi."split_from_payment_schedule_item_id" AS split_from_payment_schedule_item_id,
                psi."online_payment_split_total_amount" AS online_payment_split_total_amount,
                psi."online_payment_split_fee_amount" AS online_payment_split_fee_amount,
                psi."online_payment_split_interest_amount" AS online_payment_split_interest_amount,
                psi."online_payment_split_principal_amount" AS online_payment_split_principal_amount,
                psi."online_payment_split_other_amount" AS online_payment_split_other_amount,
                ps."customer_id" AS customer_id,
                ps."base_loan_id" AS base_loan_id,
                ci."organization_id" AS organization_id,
                ps."initial_apr" AS initial_apr,
                IFNULL(ps."is_active",0) AS is_active,
                IFNULL(ps."is_collections",0) AS is_collections,
                ps."stored_payment_use" AS stored_payment_use,
                ps."theoretical_sep_amt" AS theoretical_sep_amt,
                psi."auxiliary_type" AS auxiliary_type, -- DAT-6692
                psi."created_datetime" AS created_datetime,  
                psi."updated_datetime" AS updated_datetime,
                CURRENT_TIMESTAMP AS process_timestamp 
      FROM DEV_ENTERPRISE_LANDING."jaglms"."lms_payment_schedules" ps
        INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_base_loans" bl ON ps."base_loan_id" = bl."base_loan_id"
        INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_customer_info_flat" ci ON bl."customer_id" = ci."customer_id"
        INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_payment_schedule_items" psi ON ps."payment_schedule_id" = psi."payment_schedule_id"
        WHERE ci."organization_id" IN (3,4)
        -- AND psi."updated_datetime" > :LD_CHECK_POINT_START AND psi."updated_datetime" <= :LD_CHECK_POINT_END -- new incremental logic
        AND (
                 (psi._SNOWFLAKE_INSERTED_AT > :LD_CHECK_POINT_START and psi._SNOWFLAKE_INSERTED_AT <= :LD_CHECK_POINT_END) OR
                 (psi._SNOWFLAKE_UPDATED_AT > :LD_CHECK_POINT_START and psi._SNOWFLAKE_UPDATED_AT <= :LD_CHECK_POINT_END)
              )
        )
       SELECT * FROM CTE
    ) source
        ON target.PAYMENT_SCHEDULE_ITEM_ID = source.PAYMENT_SCHEDULE_ITEM_ID
        WHEN MATCHED THEN 
            UPDATE SET         
                TARGET.PAYMENT_SCHEDULE_ID = SOURCE.PAYMENT_SCHEDULE_ID,
                TARGET.ITEM_DATE = SOURCE.ITEM_DATE,
                TARGET.PAYMENT_MODE = SOURCE.PAYMENT_MODE,
                TARGET.STATUS = SOURCE.STATUS,
                TARGET.TOTAL_AMOUNT = SOURCE.TOTAL_AMOUNT,
                TARGET.AMOUNT_FEE = SOURCE.AMOUNT_FEE,
                TARGET.AMOUNT_INT = SOURCE.AMOUNT_INT,
                TARGET.AMOUNT_PRIN = SOURCE.AMOUNT_PRIN,
                TARGET.AMOUNT_OTHER = SOURCE.AMOUNT_OTHER,
                TARGET.ITEM_TYPE = SOURCE.ITEM_TYPE,
                TARGET.AMOUNT_DISC = SOURCE.AMOUNT_DISC,
                TARGET.OUTSTANDING_FEE_AMOUNT = SOURCE.OUTSTANDING_FEE_AMOUNT,
                TARGET.IS_PIF = SOURCE.IS_PIF,
                TARGET.IS_OFFCYCLE = SOURCE.IS_OFFCYCLE,    
                TARGET.PAYMENT_SEQUENCE = SOURCE.PAYMENT_SEQUENCE,
                TARGET.CURE_INFO_ID = SOURCE.CURE_INFO_ID,
                TARGET.IS_CURE_MASTER = SOURCE.IS_CURE_MASTER,
                TARGET.PAYMENT_FAILED_DATE = SOURCE.PAYMENT_FAILED_DATE,
                TARGET.REFERENCE_TEXT = SOURCE.REFERENCE_TEXT,
                TARGET.IS_CREATED_BY_CUSTOMER_ONLINE = SOURCE.IS_CREATED_BY_CUSTOMER_ONLINE,
                TARGET.IS_CREATED_BY_IVR_PAYMENT  = SOURCE.IS_CREATED_BY_IVR_PAYMENT , -- DAT-4723
                TARGET.ORIGINAL_ITEM_DATE = SOURCE.ORIGINAL_ITEM_DATE,
                TARGET.ORIGINAL_TOTAL_AMOUNT = SOURCE.ORIGINAL_TOTAL_AMOUNT,
                TARGET.ORIGINAL_FEE_AMOUNT = SOURCE.ORIGINAL_FEE_AMOUNT,
                TARGET.ORIGINAL_INTEREST_AMOUNT = SOURCE.ORIGINAL_INTEREST_AMOUNT,
                TARGET.ORIGINAL_PRINCIPAL_AMOUNT = SOURCE.ORIGINAL_PRINCIPAL_AMOUNT,
                TARGET.ORIGINAL_OTHER_AMOUNT = SOURCE.ORIGINAL_OTHER_AMOUNT,
                TARGET.ORIGINAL_OUTSTANDING_FEE_AMOUNT = SOURCE.ORIGINAL_OUTSTANDING_FEE_AMOUNT,            
                TARGET.SPLIT_FROM_PAYMENT_SCHEDULE_ITEM_ID = SOURCE.SPLIT_FROM_PAYMENT_SCHEDULE_ITEM_ID,
                TARGET.ONLINE_PAYMENT_SPLIT_TOTAL_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_TOTAL_AMOUNT,
                TARGET.ONLINE_PAYMENT_SPLIT_FEE_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_FEE_AMOUNT,
                TARGET.ONLINE_PAYMENT_SPLIT_INTEREST_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_INTEREST_AMOUNT,
                TARGET.ONLINE_PAYMENT_SPLIT_PRINCIPAL_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_PRINCIPAL_AMOUNT,
                TARGET.ONLINE_PAYMENT_SPLIT_OTHER_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_OTHER_AMOUNT,
                TARGET.CUSTOMER_ID = SOURCE.CUSTOMER_ID,
                TARGET.BASE_LOAN_ID = SOURCE.BASE_LOAN_ID,
                TARGET.ORGANIZATION_ID = SOURCE.ORGANIZATION_ID,
                TARGET.INITIAL_APR = SOURCE.INITIAL_APR,
                TARGET.IS_ACTIVE = SOURCE.IS_ACTIVE,
                TARGET.IS_COLLECTIONS = SOURCE.IS_COLLECTIONS,
                TARGET.STORED_PAYMENT_USE = SOURCE.STORED_PAYMENT_USE,
                TARGET.AUXILIARY_TYPE = SOURCE.AUXILIARY_TYPE,
                TARGET.CREATED_DATETIME =  SOURCE.CREATED_DATETIME,  
                TARGET.UPDATED_DATETIME = SOURCE.UPDATED_DATETIME,
                TARGET.PROCESS_TIMESTAMP = SOURCE.PROCESS_TIMESTAMP
        WHEN NOT MATCHED THEN 
            INSERT (
                    PAYMENT_SCHEDULE_ITEM_ID,
                    PAYMENT_SCHEDULE_ID,
                    ITEM_DATE,
                    PAYMENT_MODE,
                    STATUS,
                    TOTAL_AMOUNT,
                    AMOUNT_FEE,
                    AMOUNT_INT,
                    AMOUNT_PRIN,
                    AMOUNT_OTHER,
                    ITEM_TYPE,
                    AMOUNT_DISC,
                    OUTSTANDING_FEE_AMOUNT,
                    IS_PIF,
                    IS_OFFCYCLE,
                    PAYMENT_SEQUENCE,
                    CURE_INFO_ID,
                    IS_CURE_MASTER,
                    PAYMENT_FAILED_DATE,
                    REFERENCE_TEXT,
                    IS_CREATED_BY_CUSTOMER_ONLINE,
                    IS_CREATED_BY_IVR_PAYMENT,
                    ORIGINAL_ITEM_DATE,
                    ORIGINAL_TOTAL_AMOUNT,
                    ORIGINAL_FEE_AMOUNT,
                    ORIGINAL_INTEREST_AMOUNT,
                    ORIGINAL_PRINCIPAL_AMOUNT,
                    ORIGINAL_OTHER_AMOUNT,
                    ORIGINAL_OUTSTANDING_FEE_AMOUNT,
                    SPLIT_FROM_PAYMENT_SCHEDULE_ITEM_ID,
                    ONLINE_PAYMENT_SPLIT_TOTAL_AMOUNT,
                    ONLINE_PAYMENT_SPLIT_FEE_AMOUNT,
                    ONLINE_PAYMENT_SPLIT_INTEREST_AMOUNT,
                    ONLINE_PAYMENT_SPLIT_PRINCIPAL_AMOUNT,
                    ONLINE_PAYMENT_SPLIT_OTHER_AMOUNT,
                    CUSTOMER_ID,
                    BASE_LOAN_ID,
                    ORGANIZATION_ID,
                    INITIAL_APR,
                    IS_ACTIVE,
                    IS_COLLECTIONS,
                    STORED_PAYMENT_USE,
                    THEORETICAL_SEP_AMT,
                    AUXILIARY_TYPE,
                    CREATED_DATETIME,  
                    UPDATED_DATETIME,  
                    PROCESS_TIMESTAMP)
             VALUES( SOURCE.PAYMENT_SCHEDULE_ITEM_ID,
                    SOURCE.PAYMENT_SCHEDULE_ID,
                    SOURCE.ITEM_DATE,
                    SOURCE.PAYMENT_MODE,
                    SOURCE.STATUS,
                    SOURCE.TOTAL_AMOUNT,
                    SOURCE.AMOUNT_FEE,
                    SOURCE.AMOUNT_INT,
                    SOURCE.AMOUNT_PRIN,
                    SOURCE.AMOUNT_OTHER,
                    SOURCE.ITEM_TYPE,
                    SOURCE.AMOUNT_DISC,
                    SOURCE.OUTSTANDING_FEE_AMOUNT,
                    SOURCE.IS_PIF,
                    SOURCE.IS_OFFCYCLE,
                    SOURCE.PAYMENT_SEQUENCE,
                    SOURCE.CURE_INFO_ID,
                    SOURCE.IS_CURE_MASTER,
                    SOURCE.PAYMENT_FAILED_DATE,
                    SOURCE.REFERENCE_TEXT,
                    SOURCE.IS_CREATED_BY_CUSTOMER_ONLINE,
                    SOURCE.IS_CREATED_BY_IVR_PAYMENT,
                    SOURCE.ORIGINAL_ITEM_DATE,
                    SOURCE.ORIGINAL_TOTAL_AMOUNT,
                    SOURCE.ORIGINAL_FEE_AMOUNT,
                    SOURCE.ORIGINAL_INTEREST_AMOUNT,
                    SOURCE.ORIGINAL_PRINCIPAL_AMOUNT,
                    SOURCE.ORIGINAL_OTHER_AMOUNT,
                    SOURCE.ORIGINAL_OUTSTANDING_FEE_AMOUNT,
                    SOURCE.SPLIT_FROM_PAYMENT_SCHEDULE_ITEM_ID,
                    SOURCE.ONLINE_PAYMENT_SPLIT_TOTAL_AMOUNT,
                    SOURCE.ONLINE_PAYMENT_SPLIT_FEE_AMOUNT,
                    SOURCE.ONLINE_PAYMENT_SPLIT_INTEREST_AMOUNT,
                    SOURCE.ONLINE_PAYMENT_SPLIT_PRINCIPAL_AMOUNT,
                    SOURCE.ONLINE_PAYMENT_SPLIT_OTHER_AMOUNT,
                    SOURCE.CUSTOMER_ID,
                    SOURCE.BASE_LOAN_ID,
                    SOURCE.ORGANIZATION_ID,
                    SOURCE.INITIAL_APR,
                    SOURCE.IS_ACTIVE,
                    SOURCE.IS_COLLECTIONS,
                    SOURCE.STORED_PAYMENT_USE,
                    SOURCE.THEORETICAL_SEP_AMT,
                    SOURCE.AUXILIARY_TYPE,
                    SOURCE.CREATED_DATETIME,  
                    SOURCE.UPDATED_DATETIME,  
                    SOURCE.PROCESS_TIMESTAMP);

    LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Merge'';
    LC_LOG_LABEL      := CONCAT(''Merge data into DIM_PAYMENT_SCHEDULE table for organization id in 3 & 4'');
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

    LC_TABLE_NAME        := ''DIM_PAYMENT_SCHEDULE'';
    LN_IS_SUCCESS        := NULL;
    LD_START_DATETIME    := CURRENT_TIMESTAMP();
    LD_END_DATETIME      := NULL;                   
                    
-- Insert new payment schedule and update existing payment schedule for CC/FEB
        
    -- ORGANIZATION_ID IN (5,6);    

    MERGE INTO DWH_DEV.DWH.dim_payment_schedule target USING (  
    WITH CTE AS (   
         SELECT psi."payment_schedule_item_id" AS payment_schedule_item_id,
                psi."payment_schedule_id" AS payment_schedule_id,
                psi."item_date" AS item_date,
                psi."payment_mode" AS payment_mode,
                psi."status" AS status,
                psi."total_amount" AS total_amount,
                psi."amount_fee" AS amount_fee,
                psi."amount_int" AS amount_int,
                psi."amount_prin" AS amount_prin,
                psi."amount_other" AS amount_other,
                (CASE 
                    WHEN psi."item_type" IS NULL AND psi."total_amount" >= 0 AND psi."status" NOT IN (''scheduled'', ''bypass'', ''cancelled'') 
                        THEN ''D''
                    WHEN psi."item_type" IS NULL AND psi."total_amount" < 0 AND psi."status" NOT IN (''scheduled'', ''bypass'', ''cancelled'') 
                        THEN ''C''
                 ELSE psi."item_type" END) AS item_type,
                psi."amount_disc" AS amount_disc,
                psi."outstanding_fee_amount" AS outstanding_fee_amount,
                psi."is_pif" AS is_pif,
                psi."is_offcycle" AS is_offcycle,
                psi."payment_sequence" AS payment_sequence,
                psi."cure_info_id" AS cure_info_id,
                psi."is_cure_master" AS is_cure_master,
                psi."payment_failed_date" AS payment_failed_date,
                psi."reference_text" AS reference_text,
                psi."is_created_by_customer_online" AS is_created_by_customer_online,
                psi."is_created_by_ivr_payment" AS is_created_by_ivr_payment,
                psi."original_item_date" AS original_item_date,
                psi."original_total_amount" AS original_total_amount,
                psi."original_fee_amount" AS original_fee_amount,
                psi."original_interest_amount" AS original_interest_amount,
                psi."original_principal_amount" AS original_principal_amount,
                psi."original_other_amount" AS original_other_amount,
                psi."original_outstanding_fee_amount" AS original_outstanding_fee_amount,
                psi."split_from_payment_schedule_item_id" AS split_from_payment_schedule_item_id,
                psi."online_payment_split_total_amount" AS online_payment_split_total_amount,
                psi."online_payment_split_fee_amount" AS online_payment_split_fee_amount,
                psi."online_payment_split_interest_amount" AS online_payment_split_interest_amount,
                psi."online_payment_split_principal_amount" AS online_payment_split_principal_amount,
                psi."online_payment_split_other_amount" AS online_payment_split_other_amount,
                ps."customer_id" AS customer_id,
                ps."base_loan_id" AS base_loan_id,
                ci."organization_id" AS organization_id,
                ps."initial_apr" AS initial_apr,
                IFNULL(ps."is_active",0) AS is_active,
                IFNULL(ps."is_collections",0) AS is_collections,
                ps."stored_payment_use" AS stored_payment_use,
                ps."theoretical_sep_amt" AS theoretical_sep_amt,
                psi."auxiliary_type" AS auxiliary_type, -- DAT-6692
                psi."created_datetime" AS created_datetime,  
                psi."updated_datetime" AS updated_datetime,
                CURRENT_TIMESTAMP AS process_timestamp 
      FROM DEV_ENTERPRISE_LANDING."jaglms"."lms_payment_schedules" ps
        INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_base_loans" bl ON ps."base_loan_id" = bl."base_loan_id"
        INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_customer_info_flat" ci ON bl."customer_id" = ci."customer_id" 
        INNER JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_payment_schedule_items" psi ON ps."payment_schedule_id" = psi."payment_schedule_id"
        WHERE ci."organization_id" >= 5
        -- AND psi."updated_datetime" > :LD_CHECK_POINT_START AND psi."updated_datetime" <= :LD_CHECK_POINT_END -- new incremental logic
        AND (
                 (psi._SNOWFLAKE_INSERTED_AT > :LD_CHECK_POINT_START and psi._SNOWFLAKE_INSERTED_AT <= :LD_CHECK_POINT_END) OR
                 (psi._SNOWFLAKE_UPDATED_AT > :LD_CHECK_POINT_START and psi._SNOWFLAKE_UPDATED_AT <= :LD_CHECK_POINT_END)
              )
        )
       SELECT * FROM CTE
    ) source
        ON target.PAYMENT_SCHEDULE_ITEM_ID = source.PAYMENT_SCHEDULE_ITEM_ID
        WHEN MATCHED THEN 
            UPDATE SET         
                TARGET.PAYMENT_SCHEDULE_ID = SOURCE.PAYMENT_SCHEDULE_ID,
                TARGET.ITEM_DATE = SOURCE.ITEM_DATE,
                TARGET.PAYMENT_MODE = SOURCE.PAYMENT_MODE,
                TARGET.STATUS = SOURCE.STATUS,
                TARGET.TOTAL_AMOUNT = SOURCE.TOTAL_AMOUNT,
                TARGET.AMOUNT_FEE = SOURCE.AMOUNT_FEE,
                TARGET.AMOUNT_INT = SOURCE.AMOUNT_INT,
                TARGET.AMOUNT_PRIN = SOURCE.AMOUNT_PRIN,
                TARGET.AMOUNT_OTHER = SOURCE.AMOUNT_OTHER,
                TARGET.ITEM_TYPE = SOURCE.ITEM_TYPE,
                TARGET.AMOUNT_DISC = SOURCE.AMOUNT_DISC,
                TARGET.OUTSTANDING_FEE_AMOUNT = SOURCE.OUTSTANDING_FEE_AMOUNT,
                TARGET.IS_PIF = SOURCE.IS_PIF,
                TARGET.IS_OFFCYCLE = SOURCE.IS_OFFCYCLE,    
                TARGET.PAYMENT_SEQUENCE =  SOURCE.PAYMENT_SEQUENCE,
                TARGET.CURE_INFO_ID = SOURCE.CURE_INFO_ID,
                TARGET.IS_CURE_MASTER = SOURCE.IS_CURE_MASTER,
                TARGET.PAYMENT_FAILED_DATE = SOURCE.PAYMENT_FAILED_DATE,
                TARGET.REFERENCE_TEXT = SOURCE.REFERENCE_TEXT,
                TARGET.IS_CREATED_BY_CUSTOMER_ONLINE = SOURCE.IS_CREATED_BY_CUSTOMER_ONLINE,
                TARGET.IS_CREATED_BY_IVR_PAYMENT  = SOURCE.IS_CREATED_BY_IVR_PAYMENT , -- DAT-4723
                TARGET.ORIGINAL_ITEM_DATE = SOURCE.ORIGINAL_ITEM_DATE,
                TARGET.ORIGINAL_TOTAL_AMOUNT = SOURCE.ORIGINAL_TOTAL_AMOUNT,
                TARGET.ORIGINAL_FEE_AMOUNT = SOURCE.ORIGINAL_FEE_AMOUNT,
                TARGET.ORIGINAL_INTEREST_AMOUNT = SOURCE.ORIGINAL_INTEREST_AMOUNT,
                TARGET.ORIGINAL_PRINCIPAL_AMOUNT = SOURCE.ORIGINAL_PRINCIPAL_AMOUNT,
                TARGET.ORIGINAL_OTHER_AMOUNT = SOURCE.ORIGINAL_OTHER_AMOUNT,
                TARGET.ORIGINAL_OUTSTANDING_FEE_AMOUNT = SOURCE.ORIGINAL_OUTSTANDING_FEE_AMOUNT,            
                TARGET.SPLIT_FROM_PAYMENT_SCHEDULE_ITEM_ID = SOURCE.SPLIT_FROM_PAYMENT_SCHEDULE_ITEM_ID,
                TARGET.ONLINE_PAYMENT_SPLIT_TOTAL_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_TOTAL_AMOUNT,
                TARGET.ONLINE_PAYMENT_SPLIT_FEE_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_FEE_AMOUNT,
                TARGET.ONLINE_PAYMENT_SPLIT_INTEREST_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_INTEREST_AMOUNT,
                TARGET.ONLINE_PAYMENT_SPLIT_PRINCIPAL_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_PRINCIPAL_AMOUNT,
                TARGET.ONLINE_PAYMENT_SPLIT_OTHER_AMOUNT = SOURCE.ONLINE_PAYMENT_SPLIT_OTHER_AMOUNT,
                TARGET.CUSTOMER_ID = SOURCE.CUSTOMER_ID,
                TARGET.BASE_LOAN_ID = SOURCE.BASE_LOAN_ID,
                TARGET.ORGANIZATION_ID = SOURCE.ORGANIZATION_ID,
                TARGET.INITIAL_APR = SOURCE.INITIAL_APR,
                TARGET.IS_ACTIVE = SOURCE.IS_ACTIVE,
                TARGET.IS_COLLECTIONS = SOURCE.IS_COLLECTIONS,
                TARGET.STORED_PAYMENT_USE = SOURCE.STORED_PAYMENT_USE,
                TARGET.AUXILIARY_TYPE = SOURCE.AUXILIARY_TYPE,
                TARGET.CREATED_DATETIME =  SOURCE.CREATED_DATETIME,  
                TARGET.UPDATED_DATETIME = SOURCE.UPDATED_DATETIME,
                TARGET.PROCESS_TIMESTAMP = SOURCE.PROCESS_TIMESTAMP
        WHEN NOT MATCHED THEN 
            INSERT (
                    PAYMENT_SCHEDULE_ITEM_ID,
                    PAYMENT_SCHEDULE_ID,
                    ITEM_DATE,
                    PAYMENT_MODE,
                    STATUS,
                    TOTAL_AMOUNT,
                    AMOUNT_FEE,
                    AMOUNT_INT,
                    AMOUNT_PRIN,
                    AMOUNT_OTHER,
                    ITEM_TYPE,
                    AMOUNT_DISC,
                    OUTSTANDING_FEE_AMOUNT,
                    IS_PIF,
                    IS_OFFCYCLE,
                    PAYMENT_SEQUENCE,
                    CURE_INFO_ID,
                    IS_CURE_MASTER,
                    PAYMENT_FAILED_DATE,
                    REFERENCE_TEXT,
                    IS_CREATED_BY_CUSTOMER_ONLINE,
                    IS_CREATED_BY_IVR_PAYMENT,
                    ORIGINAL_ITEM_DATE,
                    ORIGINAL_TOTAL_AMOUNT,
                    ORIGINAL_FEE_AMOUNT,
                    ORIGINAL_INTEREST_AMOUNT,
                    ORIGINAL_PRINCIPAL_AMOUNT,
                    ORIGINAL_OTHER_AMOUNT,
                    ORIGINAL_OUTSTANDING_FEE_AMOUNT,
                    SPLIT_FROM_PAYMENT_SCHEDULE_ITEM_ID,
                    ONLINE_PAYMENT_SPLIT_TOTAL_AMOUNT,
                    ONLINE_PAYMENT_SPLIT_FEE_AMOUNT,
                    ONLINE_PAYMENT_SPLIT_INTEREST_AMOUNT,
                    ONLINE_PAYMENT_SPLIT_PRINCIPAL_AMOUNT,
                    ONLINE_PAYMENT_SPLIT_OTHER_AMOUNT,
                    CUSTOMER_ID,
                    BASE_LOAN_ID,
                    ORGANIZATION_ID,
                    INITIAL_APR,
                    IS_ACTIVE,
                    IS_COLLECTIONS,
                    STORED_PAYMENT_USE,
                    THEORETICAL_SEP_AMT,
                    AUXILIARY_TYPE,
                    CREATED_DATETIME,  
                    UPDATED_DATETIME,  
                    PROCESS_TIMESTAMP)
             VALUES( SOURCE.PAYMENT_SCHEDULE_ITEM_ID,
                    SOURCE.PAYMENT_SCHEDULE_ID,
                    SOURCE.ITEM_DATE,
                    SOURCE.PAYMENT_MODE,
                    SOURCE.STATUS,
                    SOURCE.TOTAL_AMOUNT,
                    SOURCE.AMOUNT_FEE,
                    SOURCE.AMOUNT_INT,
                    SOURCE.AMOUNT_PRIN,
                    SOURCE.AMOUNT_OTHER,
                    SOURCE.ITEM_TYPE,
                    SOURCE.AMOUNT_DISC,
                    SOURCE.OUTSTANDING_FEE_AMOUNT,
                    SOURCE.IS_PIF,
                    SOURCE.IS_OFFCYCLE,
                    SOURCE.PAYMENT_SEQUENCE,
                    SOURCE.CURE_INFO_ID,
                    SOURCE.IS_CURE_MASTER,
                    SOURCE.PAYMENT_FAILED_DATE,
                    SOURCE.REFERENCE_TEXT,
                    SOURCE.IS_CREATED_BY_CUSTOMER_ONLINE,
                    SOURCE.IS_CREATED_BY_IVR_PAYMENT,
                    SOURCE.ORIGINAL_ITEM_DATE,
                    SOURCE.ORIGINAL_TOTAL_AMOUNT,
                    SOURCE.ORIGINAL_FEE_AMOUNT,
                    SOURCE.ORIGINAL_INTEREST_AMOUNT,
                    SOURCE.ORIGINAL_PRINCIPAL_AMOUNT,
                    SOURCE.ORIGINAL_OTHER_AMOUNT,
                    SOURCE.ORIGINAL_OUTSTANDING_FEE_AMOUNT,
                    SOURCE.SPLIT_FROM_PAYMENT_SCHEDULE_ITEM_ID,
                    SOURCE.ONLINE_PAYMENT_SPLIT_TOTAL_AMOUNT,
                    SOURCE.ONLINE_PAYMENT_SPLIT_FEE_AMOUNT,
                    SOURCE.ONLINE_PAYMENT_SPLIT_INTEREST_AMOUNT,
                    SOURCE.ONLINE_PAYMENT_SPLIT_PRINCIPAL_AMOUNT,
                    SOURCE.ONLINE_PAYMENT_SPLIT_OTHER_AMOUNT,
                    SOURCE.CUSTOMER_ID,
                    SOURCE.BASE_LOAN_ID,
                    SOURCE.ORGANIZATION_ID,
                    SOURCE.INITIAL_APR,
                    SOURCE.IS_ACTIVE,
                    SOURCE.IS_COLLECTIONS,
                    SOURCE.STORED_PAYMENT_USE,
                    SOURCE.THEORETICAL_SEP_AMT,
                    SOURCE.AUXILIARY_TYPE,
                    SOURCE.CREATED_DATETIME,  
                    SOURCE.UPDATED_DATETIME,  
                    SOURCE.PROCESS_TIMESTAMP);
                    
    LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Merge'';
    LC_LOG_LABEL      := CONCAT(''Merge data into DIM_PAYMENT_SCHEDULE table for organization id in 5 & 6'');
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
                                                                

-- Remove the payment failed date if payment is cleared
                
    UPDATE DWH_DEV.DWH.DIM_PAYMENT_SCHEDULE dps 
       SET PAYMENT_FAILED_DATE = NULL
    WHERE  (ITEM_DATE >= :LD_CHECK_POINT_START OR PROCESS_TIMESTAMP >= :LD_CHECK_POINT_START)
        AND STATUS IN (''Cleared'', ''SENT'')
        AND PAYMENT_FAILED_DATE IS NOT NULL;

    LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Update'';
    LC_LOG_LABEL      := CONCAT(''Remove the payment failed date if payment is cleared from DIM_PAYMENT_SCHEDULE table'');
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
        
        
-- Add the payment failed date for card payment if missed
        
    UPDATE DWH_DEV.DWH.DIM_PAYMENT_SCHEDULE dps 
       SET PAYMENT_FAILED_DATE = (ITEM_DATE + 3)
    WHERE  (ITEM_DATE >= :LD_CHECK_POINT_START OR PROCESS_TIMESTAMP >= :LD_CHECK_POINT_START)
        AND (PAYMENT_MODE = ''NON-ACH'' OR PAYMENT_MODE LIKE ''%card%'') 
        AND STATUS IN (''MISSED'', ''Return'')
        AND PAYMENT_FAILED_DATE IS NULL; 

    LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Update'';
    LC_LOG_LABEL      := CONCAT(''Add the payment failed date for card payment if missed from DIM_PAYMENT_SCHEDULE table'');
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

-- Add the payment failed date for ach payment if missed 
        
    UPDATE DWH_DEV.DWH.DIM_PAYMENT_SCHEDULE DPS 
       SET DPS.PAYMENT_FAILED_DATE = DATE(ATR."update_datetime")
    FROM DEV_ENTERPRISE_LANDING."jaglms"."ach_transactions" ATR 
    WHERE DPS.PAYMENT_SCHEDULE_ITEM_ID = ATR."lms_payment_schedule_item_id"
        AND (DPS.ITEM_DATE >= :LD_CHECK_POINT_START OR PROCESS_TIMESTAMP >= :LD_CHECK_POINT_START)
        AND DPS.STATUS IN (''Return'' , ''MISSED'')   
        AND DPS.PAYMENT_FAILED_DATE IS NULL; 
             
    UPDATE DWH_DEV.DWH.DIM_PAYMENT_SCHEDULE DPS 
       SET DPS.PAYMENT_FAILED_DATE = DPS.ITEM_DATE
    WHERE (DPS.ITEM_DATE >= :LD_CHECK_POINT_START OR PROCESS_TIMESTAMP >= :LD_CHECK_POINT_START)
        AND DPS.STATUS IN (''MISSED'')   
        AND DPS.PAYMENT_FAILED_DATE IS NULL; 

    UPDATE DWH_DEV.DWH.DIM_PAYMENT_SCHEDULE DPS 
       SET DPS.PAYMENT_FAILED_DATE = TRY_TO_TIMESTAMP(FPR."return_time"::STRING)
    FROM DEV_ENTERPRISE_LANDING."jaglms"."funding_payment_return" FPR 
    WHERE DPS.PAYMENT_SCHEDULE_ITEM_ID = FPR."payment_schedule_item_id"
        AND (DPS.ITEM_DATE >= :LD_CHECK_POINT_START OR PROCESS_TIMESTAMP >= :LD_CHECK_POINT_START)
        AND DPS.STATUS IN (''Return'' , ''MISSED'')   
        AND DPS.PAYMENT_FAILED_DATE IS NULL;    

    LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Update'';
    LC_LOG_LABEL      := CONCAT(''Add the payment failed date for ach payment if missed from DIM_PAYMENT_SCHEDULE table'');
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
        
-- Add auxiliary_type and subid_tracking_id
        
    UPDATE DWH_DEV.DWH.DIM_PAYMENT_SCHEDULE DPS 
        SET DPS.AUXILIARY_TYPE = PSI."auxiliary_type" 
    FROM DEV_ENTERPRISE_LANDING."jaglms"."lms_payment_schedule_items" PSI 
    WHERE DPS.PAYMENT_SCHEDULE_ITEM_ID = PSI."payment_schedule_item_id"
        AND DPS.PROCESS_TIMESTAMP >= :LD_CHECK_POINT_START;
                          
    UPDATE DWH_DEV.DWH.DIM_PAYMENT_SCHEDULE DPS 
        SET DPS.SUBID_TRACKING_ID = PSIM."value"
    FROM DEV_ENTERPRISE_LANDING."jaglms"."lms_payment_schedule_item_metadata" PSIM 
    WHERE DPS.PAYMENT_SCHEDULE_ITEM_ID = PSIM."payment_schedule_item_id"
        AND DPS.PROCESS_TIMESTAMP >= :LD_CHECK_POINT_START
        AND PSIM."name" = ''subIdTrackingId'';
    
    LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Update'';
    LC_LOG_LABEL      := CONCAT(''Add auxiliary_type and subid_tracking_id from DIM_PAYMENT_SCHEDULE table'');
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

    LC_TABLE_NAME        := ''DIM_PAYMENT_SCHEDULE_REMOVED'';
    LN_IS_SUCCESS        := NULL;
    LD_START_DATETIME    := CURRENT_TIMESTAMP();
    LD_END_DATETIME      := NULL;                                                               
    
-- Archive all psi which has been removed from primary database

    INSERT INTO DWH_DEV.DWH.DIM_PAYMENT_SCHEDULE_REMOVED  
        SELECT DPS.*       
        FROM DWH.DIM_PAYMENT_SCHEDULE DPS
          LEFT JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_payment_schedule_items" PSI ON DPS.PAYMENT_SCHEDULE_ITEM_ID = PSI."payment_schedule_item_id"
          LEFT JOIN DWH.DIM_PAYMENT_SCHEDULE_REMOVED DPSR ON DPS.PAYMENT_SCHEDULE_ITEM_ID = DPSR.PAYMENT_SCHEDULE_ITEM_ID
        WHERE DPS.ITEM_DATE >= :LD_CHECK_POINT_START
            AND PSI."payment_schedule_item_id" IS NULL AND DPSR.PAYMENT_SCHEDULE_ITEM_ID IS NULL;    
            
    LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Insert'';
    LC_LOG_LABEL      := CONCAT(''Archive all psi which has been removed from primary database from DIM_PAYMENT_SCHEDULE_REMOVED table'');
    LC_LOG_MESSAGE    := CONCAT(''Insert succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
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

    LC_TABLE_NAME        := ''DIM_PAYMENT_SCHEDULE''; 
    LN_IS_SUCCESS        := NULL;
    LD_START_DATETIME    := CURRENT_TIMESTAMP();
    LD_END_DATETIME      := NULL; 

-- Remove those psi which has been removed from primary database
    
    --DROP TABLE IF EXISTS STG_REMOVED_PSI;
	CREATE TEMP TABLE IF NOT EXISTS STG_REMOVED_PSI AS 
	(
        SELECT DS.PAYMENT_SCHEDULE_ITEM_ID 
            FROM DWH.DIM_PAYMENT_SCHEDULE DS  
              LEFT JOIN DEV_ENTERPRISE_LANDING."jaglms"."lms_payment_schedule_items" PSI ON DS.PAYMENT_SCHEDULE_ITEM_ID = PSI."payment_schedule_item_id"
            WHERE   PSI."payment_schedule_item_id" IS NULL   
     );    
    --  Set payment_schedule_item_id
    SELECT MIN(PAYMENT_SCHEDULE_ITEM_ID) INTO :PAYMENT_SCHEDULE_ITEM_ID FROM DWH_DEV.DWH.STG_REMOVED_PSI;
        
 
    DELETE FROM DWH_DEV.DWH.DIM_PAYMENT_SCHEDULE
     WHERE PAYMENT_SCHEDULE_ITEM_ID >= :PAYMENT_SCHEDULE_ITEM_ID
        AND PAYMENT_SCHEDULE_ITEM_ID IN (SELECT PAYMENT_SCHEDULE_ITEM_ID FROM DWH_DEV.DWH.STG_REMOVED_PSI );    

    LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Delete'';
    LC_LOG_LABEL      := CONCAT(''Remove those psi which has been removed from primary database from DIM_PAYMENT_SCHEDULE table'');
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
	
    RETURN ''SP_PAYMENT_SCHEDULE : Job is done'';

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