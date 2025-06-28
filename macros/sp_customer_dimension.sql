{% macro sp_customer_dimension() %}
{% set prepare_database = get_prepare_database() %}
{% set sql %}
CREATE OR REPLACE PROCEDURE {{ prepare_database }}.DWH.SP_CUSTOMER_DIMENSION()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE 

        LN_JOB_ID                  INT;
    	LN_BATCH_ID                INT;
    	LN_ROW_COUNT               SMALLINT;
    	LC_JOB_NAME                VARCHAR(200)   DEFAULT ''SP_CUSTOMER_DIMENSION'';
    	LC_BATCH_TYPE              VARCHAR(100)   DEFAULT ''DELTA'';
    	LC_BATCH_LABEL             VARCHAR(500)   DEFAULT ''Populate Insurance Dim table DIM_CUSTOMER'';
    	LC_CHECK_POINT_TYPE        VARCHAR(50)    DEFAULT ''TIMESTAMP'';
    	LD_CHECK_POINT_START       DATETIME;
    	LD_CHECK_POINT_END         DATETIME;
    	LN_IS_SUCCESS              SMALLINT;
    	LD_START_DATETIME          DATETIME;
    	LD_END_DATETIME            DATETIME;
    	LC_ERROR_MESSAGE           VARCHAR(500);
    	LC_TABLE_NAME              VARCHAR(100)   DEFAULT ''DIM_CUSTOMER'';
    	LC_LOG_TYPE                VARCHAR(50);
    	LC_LOG_LABEL               VARCHAR(200);
    	LC_LOG_MESSAGE             VARCHAR(500);
    
    BEGIN

    -------------------------
    -- step 1 - Log start ---
    -------------------------

    LN_BATCH_ID          := (SELECT OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_ID.NEXTVAL FROM DUAL);

    LN_JOB_ID            := (SELECT JOB_ID FROM OP_ADMIN.OPERATIONS.ETL_JOBS WHERE JOB_NAME = :LC_JOB_NAME);

    LD_CHECK_POINT_START := (SELECT MAX(CAST(T.CHECK_POINT_END AS DATETIME))
                              FROM OP_ADMIN.OPERATIONS.ETL_JOB_BATCHES T
                             WHERE T.JOB_ID = :LN_JOB_ID
                               AND T.IS_SUCCESS = 1);

    LD_CHECK_POINT_START := NVL(:LD_CHECK_POINT_START, CAST(''2000-01-01 12:00:00'' AS DATETIME));

    LD_CHECK_POINT_END   := CURRENT_TIMESTAMP();
    LC_TABLE_NAME        := ''DIM_CUSTOMER'';
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


    -------------------------
    -- step 2 - Merge ---
    -------------------------

    MERGE INTO DWH_DEV.DWH.DIM_CUSTOMER target
    USING (
                select c."customer_id" as customer_id,
                ''JAG'' as lms_code,
                c."lastname" as lastname,
                c."firstname" as firstname,
                c."address" as address,
                c."address2" as address2,
                c."city" as city,
                c."state" as state,
                c."zip" as zip,
                c."homephone" as homephone,
                c."cellphone" as cellphone,
                c."alt_phone" as alt_phone,
                TO_VARCHAR(TO_NUMBER(NULLIF(TRIM(REPLACE(c."grossmonthly", '','',''''),''$''),''''),38,2)) as grossmonthly,
                c."incometype" as incometype,
                c."ismilitary" as ismilitary,
                c."mla_flag" as mla_flag,
                lower(c."email") as email,
                lower(c."email2") as email2,
                c."bankname" as bankname,
                c."employer" as employer,
                c."emp_industry" as emp_industry,
                c."bankphone" as bankphone,
                c."empphone" as empphone,
                c."workphone" as workphone,
                c."otherphone" as otherphone,
                c."supervisor" as supervisor,
                c."title" as title,
                c."dob" as dob,
                c."hiredate" as hiredate,
                c."lastpaydate" as lastpaydate,
                c."nextpaydate" as nextpaydate,
                c."next_paydate_computed" as next_paydate_computed,
                c."secondpaydate" as secondpaydate,
                c."payfrequency" as payfrequency,
                c."paytype" as paytype,         
                                        TO_VARCHAR(CASE 
                                            WHEN TO_NUMBER(NULLIF(TRIM(REPLACE(c."paycheck_amount", '','',''''),''$''),''''),38,2) >  0 THEN TO_NUMBER(NULLIF(TRIM(REPLACE(c."paycheck_amount", '','' ,''''),''$''),''''),38,2)
                    ELSE
                        ROUND(TO_NUMBER(NULLIF(TRIM(REPLACE(c."nmi", '','' ,''''),''$''),''''),38,2)
                                                /
                                                (CASE 
                                                    WHEN c."payfrequency" =''B'' then 2.16667
                                                    WHEN c."payfrequency" =''W'' then 4.3333
                                                    WHEN c."payfrequency" =''S'' then 2
                                                    ELSE 1
                                                    END),2)
                                            END) as paycheck_amount,
                c."rent_amount" as rent_amount,
                TO_VARCHAR(TO_NUMBER(NULLIF(TRIM(REPLACE(c."nmi", '','' ,''''),''$''),''''),38,2)) as nmi,         
                c."dow" as dow,
                c."day1" as day1,
                c."day2" as day2,
                c."movedir" as movedir,
                c."language" as language,
                c."optout_account_email" as optout_account_email,
                c."optout_account_sms" as optout_account_sms,
                c."optout_marketing_sms" as optout_marketing_sms,
                c."optout_marketing_email" as optout_marketing_email,
                c."dom1" as dom1,
                c."dom2" as dom2,
                c."accounttype" as accounttype,
                c."routingnumber" as routingnumber,
                c."bank_institution_code" as bank_institution_code,
                c."bank_transit_code" as bank_transit_code,
                c."wire_aba" as wire_aba,
                c."is_online_bank" as is_online_bank,
                c."account_number" as account_number,
                c."organization_id" as organization_id,
                uo."organization_name" as organization_name,
                uo."is_active" as is_active,
                uo."email_domain" as email_domain,
                c."social_security_number" as social_security_number,
                c."ssn_last_four" as ssn_last_four,
                c."electronic_consent_signed" as electronic_consent_signed,
                c."electronic_consent_signed_timestamp" as electronic_consent_signed_timestamp,
                c."marital_status" as marital_status,
                c."spouse_first_name" as spouse_first_name,
                c."spouse_last_name" as spouse_last_name,
                c."spouse_email" as spouse_email,
                c."spouse_address_1" as spouse_address_1,
                c."spouse_address_2" as spouse_address_2,
                c."spouse_city" as spouse_city,
                c."spouse_state" as spouse_state,
                c."spouse_zip_code" as spouse_zip_code,
                c."is_account_on_hold" as is_account_on_hold,
                c."created_date"::TIMESTAMP_NTZ as created_date,
                c."last_update" as last_update,
                CURRENT_TIMESTAMP as process_timestamp                     
                            from DEV_ENTERPRISE_LANDING."jaglms"."lms_customer_info_flat" c 
                            inner join DEV_ENTERPRISE_LANDING."jaglms"."upm_organizations" uo 
                            on c."organization_id" = uo."organization_id") 
    as source
    ---
    ON target.customer_id=source.customer_id AND target.lms_code=source.lms_code
    --- 
    WHEN MATCHED THEN 
        UPDATE SET
        target.lastname =  source.lastname,
        target.firstname  =  source.firstname,
        target.address =  source.address,
        target.address2 =  source.address2,
        target.city  =  source.city,
        target.state =  source.state,
        target.zip =  source.zip,
        target.homephone =  source.homephone,
        target.cellphone =  source.cellphone,
        target.alt_phone  =  source.alt_phone,
        target.grossmonthly = source.grossmonthly,
        target.incometype =  source.incometype,
        target.ismilitary =  source.ismilitary,
        target.mla_flag =  source.mla_flag,
        target.email = source.email,
        target.email2 = source.email2,
        target.bankname =  source.bankname,
        target.employer =  source.employer,
        target.emp_industry =  source.emp_industry,
        target.bankphone =  source.bankphone,
        target.empphone =  source.empphone,
        target.workphone =  source.workphone,
        target.otherphone =  source.otherphone,
        target.supervisor =  source.supervisor,
        target.title =  source.title,
        target.dob =  source.dob,
        target.hiredate =  source.hiredate,
        target.lastpaydate =  source.lastpaydate,
        target.nextpaydate =  source.nextpaydate,
        target.next_paydate_computed =  source.next_paydate_computed,
        target.secondpaydate =  source.secondpaydate,
        target.payfrequency =  source.payfrequency,
        target.paytype =  source.paytype,
        target.paycheck_amount =  source.paycheck_amount,
        target.dow =  source.dow,
        target.day1 =  source.day1,
        target.day2 =  source.day2,
        target.movedir =  source.movedir,
        target.language =  source.language,
        target.optout_account_email =  source.optout_account_email,
        target.optout_account_sms =  source.optout_account_sms,
        target.optout_marketing_sms =  source.optout_marketing_sms,
        target.optout_marketing_email =  source.optout_marketing_email,
        target.dom1 =  source.dom1,
        target.dom2 =  source.dom2,
        target.accounttype =  source.accounttype,
        target.routingnumber =  source.routingnumber,
        target.bank_institution_code =  source.bank_institution_code,
        target.bank_transit_code =  source.bank_transit_code,
        target.wire_aba =  source.wire_aba,
        target.is_online_bank =  source.is_online_bank,
        target.account_number =  source.account_number,
        target.organization_id =  source.organization_id,
        target.organization_name = source.organization_name,
        target.is_active = source.is_active,
        target.email_domain = source.email_domain,
        target.social_security_number =  source.social_security_number,
        target.ssn_last_four =  source.ssn_last_four,
        target.electronic_consent_signed =  source.electronic_consent_signed,
        target.electronic_consent_signed_timestamp =  source.electronic_consent_signed_timestamp,
        target.marital_status =  source.marital_status,
        target.spouse_first_name =  source.spouse_first_name,
        target.spouse_last_name =  source.spouse_last_name,
        target.spouse_email =  source.spouse_email,
        target.spouse_address_1 =  source.spouse_address_1,
        target.spouse_address_2 =  source.spouse_address_2,
        target.spouse_city =  source.spouse_city,
        target.spouse_state =  source.spouse_state,
        target.spouse_zip_code =  source.spouse_zip_code,
        target.is_account_on_hold =  source.is_account_on_hold,
        target.created_date =  source.created_date,
        target.last_update =  source.last_update,
        target.process_timestamp = source.process_timestamp
    ---
    WHEN NOT MATCHED THEN
    INSERT
    (customer_id,
    lms_code,
    lastname,
    firstname,
    address,
    address2,
    city,
    state,
    zip,
    homephone,
    cellphone,
    alt_phone,
    grossmonthly,
    incometype,
    ismilitary,
    mla_flag,
    email,
    email2,
    bankname,
    employer,
    emp_industry,
    bankphone,
    empphone,
    workphone,
    otherphone,
    supervisor,
    title,
    dob,
    hiredate,
    lastpaydate,
    nextpaydate,
    next_paydate_computed,
    secondpaydate,
    payfrequency,
    paytype,
    paycheck_amount,
    rent_amount, 
    nmi,
    dow,
    day1,
    day2,
    movedir,
    language,
    optout_account_email,
    optout_account_sms,
    optout_marketing_sms,
    optout_marketing_email,
    dom1,
    dom2,
    accounttype,
    routingnumber,
    bank_institution_code, 
    bank_transit_code, 
    wire_aba,
    is_online_bank,
    account_number,
    organization_id,
    organization_name,
    is_active,
    email_domain,
    social_security_number,
    ssn_last_four,
    electronic_consent_signed,
    electronic_consent_signed_timestamp,
    marital_status,
    spouse_first_name,
    spouse_last_name,
    spouse_email,
    spouse_address_1,
    spouse_address_2,
    spouse_city,
    spouse_state,
    spouse_zip_code,
    is_account_on_hold,
    created_date,
    last_update,
    process_timestamp)
    VALUES
    (
    source.customer_id,
    source.lms_code,
    source.lastname,
    source.firstname,
    source.address,
    source.address2,
    source.city,
    source.state,
    source.zip,
    source.homephone,
    source.cellphone,
    source.alt_phone,
    source.grossmonthly,
    source.incometype,
    source.ismilitary,
    source.mla_flag,
    source.email,
    source.email2,
    source.bankname,
    source.employer,
    source.emp_industry,
    source.bankphone,
    source.empphone,
    source.workphone,
    source.otherphone,
    source.supervisor,
    source.title,
    source.dob,
    source.hiredate,
    source.lastpaydate,
    source.nextpaydate,
    source.next_paydate_computed,
    source.secondpaydate,
    source.payfrequency,
    source.paytype,
    source.paycheck_amount,
    source.rent_amount, 
    source.nmi,
    source.dow,
    source.day1,
    source.day2,
    source.movedir,
    source.language,
    source.optout_account_email,
    source.optout_account_sms,
    source.optout_marketing_sms,
    source.optout_marketing_email,
    source.dom1,
    source.dom2,
    source.accounttype,
    source.routingnumber,
    source.bank_institution_code, 
    source.bank_transit_code, 
    source.wire_aba,
    source.is_online_bank,
    source.account_number,
    source.organization_id,
    source.organization_name,
    source.is_active,
    source.email_domain,
    source.social_security_number,
    source.ssn_last_four,
    source.electronic_consent_signed,
    source.electronic_consent_signed_timestamp,
    source.marital_status,
    source.spouse_first_name,
    source.spouse_last_name,
    source.spouse_email,
    source.spouse_address_1,
    source.spouse_address_2,
    source.spouse_city,
    source.spouse_state,
    source.spouse_zip_code,
    source.is_account_on_hold,
    source.created_date,
    source.last_update,
    source.process_timestamp);

    LN_ROW_COUNT      := SQLROWCOUNT;
    LC_LOG_TYPE       := ''Merge'';
    LC_LOG_LABEL      := CONCAT(''Merge data into DIM_CUSTOMER table'');
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

    ----------------------------------------------------------------
    -- step 3 - Log end - update batch status and end_datetime
    ----------------------------------------------------------------

    UPDATE OP_ADMIN.OPERATIONS.ETL_JOB_BATCHES T
    SET T.END_DATETIME = :LD_END_DATETIME,
          T.IS_SUCCESS = :LN_IS_SUCCESS
    WHERE T.BATCH_ID = :LN_BATCH_ID;


    ---------------------------------------------------------------------------------------------------------------------------------------------------------
    -- step 4 - Log end - update batch status in ETL_JOBS table
    ---------------------------------------------------------------------------------------------------------------------------------------------------------
    UPDATE OP_ADMIN.OPERATIONS.ETL_JOBS T
      SET T.IS_SUCCESS = :LN_IS_SUCCESS,
          T.CHECK_POINT_START = :LD_CHECK_POINT_START,
          T.CHECK_POINT_END = :LD_CHECK_POINT_END
    WHERE T.JOB_ID = :LN_JOB_ID;

    COMMIT;
    RETURN ''SP_CUSTOMER_DIMENSION : Job is done'';

  
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
