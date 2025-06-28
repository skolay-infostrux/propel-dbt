{% macro sp_load_loans() %}
{% set prepare_database = get_prepare_database() %}

    {% set sql %}

    CREATE OR REPLACE PROCEDURE {{ prepare_database }}.DWH.SP_LOAD_LOANS()
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
    AS 'DECLARE 
        
            -- RUNHOUR INTEGER;
            -- INTERVALDAYS INTEGER;
        
            LN_JOB_ID                  INT;
            LN_BATCH_ID                INT;
            LN_ROW_COUNT               SMALLINT;
            LC_JOB_NAME                VARCHAR(200)   DEFAULT ''SP_LOAD_LOANS'';
            LC_BATCH_TYPE              VARCHAR(100)   DEFAULT ''DELTA'';
            LC_BATCH_LABEL             VARCHAR(500)   DEFAULT ''Populate Dim table DIM_LOANS'';
            LC_CHECK_POINT_TYPE        VARCHAR(50)    DEFAULT ''TIMESTAMP'';
            LD_CHECK_POINT_START       DATETIME;
            LD_CHECK_POINT_END         DATETIME;
            LN_IS_SUCCESS              SMALLINT;
            LD_START_DATETIME          DATETIME;
            LD_END_DATETIME            DATETIME;
            LC_ERROR_MESSAGE           VARCHAR(500);
            LC_TABLE_NAME              VARCHAR(100)   DEFAULT ''DIM_LOANS'';
            LC_LOG_TYPE                VARCHAR(50);
            LC_LOG_LABEL               VARCHAR(200);
            LC_LOG_MESSAGE             VARCHAR(500);
            ETL_START_DATE             DATETIME DEFAULT ''2021-08-04''; 

        
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

    ---------------------------------------------------------------------------------------------------------------------------------------------------------
    -- step 2 - merge data into DIM_LOAN
    ---------------------------------------------------------------------------------------------------------------------------------------------------------

    ld_start_datetime := current_timestamp();                                                          

    MERGE INTO DWH_DEV.DWH.DIM_LOAN target
    USING (
    select bl.base_loan_id,
                ''JAG'' as lms_code,
                bl.customer_id,
                bl.loan_header_id,
                date(ifnull(bl.origination_date, lh.origination_timestamp)) as origination_date,
                lh.origination_timestamp,
                bl.effective_date,
                bl.due_date,
                bl.loan_type,
                bl.lms_entity_id,
                le.entity_name,
                le.organization_id,
                uo.organization_name,
                bl.stated_apr,
                bl.max_installments,
                bl.max_payments,
                bl.loan_fee_pct,
                -- bl.loan_interest_pct,
                bl.additional_fees,
                bl.loan_owner,
                (case when lh.status = ''Withdraw'' and bl.loan_status in (''Approve'',''Bank QA Error'',''Pending'',''Pending Bank Approval'',''QA Error'',''Void'') then
                ''Withdraw''
                else 
                bl.loan_status
                end) as loan_status,
                lh.status as lms_status,
                bl.funding_method,
                bl.loan_amount,
                bl.requested_loan_amount,
                bl.is_paying  ,
                bl.last_due_date,
                bl.is_deleted  ,
                bl.paid_off_date,
                bl.first_pd,
                bl.is_e_consent,
                bl.is_online_draw_enabled ,
                bl.cancellation_type,  
                bl.is_technology_fee_charged,
                bl.is_cach_allowed, -- DAT-4723
                lh.created_date,
                lh.last_update ,
                lh.followup_date,
                lh.first_name, 
                lh.last_name,
                lh.state,
                lh.country,
                lh.required_items,
                lh.is_locked,
                lh.is_returning,
                lh.last_csr,
                lh.last_csr_name,
                lh.lead_sequence_id,
                lh.priority_note,
                lh.language,
                lh.status_extended,
                lh.approver_id,
                lh.originator_id,
                lh.withdrawer_id,
                lh.contact_attempts,
                lh.last_contact_date,
                lh.bad_contact_info_flag,
                lh.n_outstanding_items,
                lh.task_remaining_flat,
                lh.approval_timestamp,
                lh.qa_timestamp,
                lh.payment_type,
                lh.payment_type_authorized,
                lh.campaign_id,
                lh.campaign_name,
                lh.q_check_valid,
                lh.q_check_agent,
                lh.is_nobankcall,
                lh.queue_priority,
                lh.last_express_time,
                lh.spousal_notice,
                lh.cure_version,
                lh.cure_datafixed,
                lh.withdrawn_timestamp,
                lh.kyc_start_timestamp,
                lh.dl_status,
                lh.web_lasttouch,
                lh.web_lastpage,
                lh.is_dm_customer,
                lh.is_limit_override_approved ,
                lh.qa_type,
                lh.is_dummy ,
                ifnull(lh.is_nc_auto_fund, 0) as is_nc_auto_fund,
                ifnull(lh.is_nc_auto_fund_flow,0) as is_nc_auto_fund_flow,
                ifnull(is_auto_fund,0) as is_auto_fund, -- DAT-4788
                lh.nc_average_payment_amount,
                lh.rc_average_payment_amount,             
                lh.risk_level_id,
                rl.risk_code,
                rl.description risk_level_description ,
                lh.bv_system_id,
                lh.tls_loan_id,
                lh.is_test_loan, lh.is_auto_fund_agent_assisted , lh.referrer_id, -- DAT-7421
                (select llh.action_time  
                    from OPENFLOW_LANDING.JAGLMS.LMS_LOANHEADER_STATE llh
                    where llh.loan_header_id = lh.loan_header_id and llh.action = ''open''  order by id limit 1) as agent_open_time, -- DAT-8710*/
                bl.created_datetime,
                bl.updated_datetime,
                CURRENT_TIMESTAMP() as process_timestamp 
                FROM OPENFLOW_LANDING.JAGLMS.LMS_BASE_LOANS bl
                inner join OPENFLOW_LANDING.JAGLMS.LMS_LOAN_HEADER lh on bl.loan_header_id =  lh.loan_header_id
                left join OPENFLOW_LANDING.JAGLMS.LMS_ENTITIES le on lh.lms_entity_id = le.lms_entity_id
                left join OPENFLOW_LANDING.JAGLMS.UPM_ORGANIZATIONS uo on le.organization_id = uo.organization_id
                left join OPENFLOW_LANDING.JAGLMS.RISK_LEVEL rl on lh.risk_level_id = rl.id
                -- where (
                --          (cl._snowflake_inserted_at > :ld_check_point_start and cl._snowflake_inserted_at <= :ld_check_point_end) or
                --          (cl._snowflake_updated_at > :ld_check_point_start and cl._snowflake_updated_at <= :ld_check_point_end)
                --       )
                where (
                        (lh.last_update > :ld_check_point_start and lh.last_update <= :ld_check_point_end) or
                        (bl.updated_datetime > :ld_check_point_start and bl.updated_datetime <= :ld_check_point_end)
                    )
                
                ) as source
        -- ADD THE WHERE STUFF
        on target.base_loan_id=source.base_loan_id
        
        WHEN MATCHED THEN 
            UPDATE SET
            target.customer_id = source.customer_id,
            target.loan_header_id = source.loan_header_id,
            target.origination_date = source.origination_date,
            target.origination_timestamp = source.origination_timestamp,
            target.effective_date = source.effective_date,
            target.due_date = source.due_date,
            target.loan_type = source.loan_type,
            target.lms_entity_id = source.lms_entity_id,
            target.entity_name = source.entity_name,
            target.organization_id = source.organization_id,
            target.organization_name = source.organization_name,
            target.stated_apr = source.stated_apr,
            target.max_installments = source.max_installments,
            target.max_payments = source.max_payments,
            target.loan_fee_pct = source.loan_fee_pct,
            -- target.loan_interest_pct = source.loan_interest_pct,
            target.additional_fees = source.additional_fees,
            target.loan_owner = source.loan_owner,
            target.loan_status = source.loan_status,
            target.lms_status = source.lms_status,
            target.funding_method = source.funding_method,
            target.loan_amount = source.loan_amount,
            target.requested_loan_amount = source.requested_loan_amount,
            target.is_paying = source.is_paying,
            target.last_due_date = source.last_due_date,
            target.is_deleted = source.is_deleted,
            target.paid_off_date = source.paid_off_date,
            target.first_pd = source.first_pd,
            target.is_e_consent = source.is_e_consent,
            target.is_online_draw_enabled = source.is_online_draw_enabled,
            target.cancellation_type = source.cancellation_type,
            target.is_technology_fee_charged = source.is_technology_fee_charged,
            target.is_cach_allowed = source.is_cach_allowed,
            target.created_date = source.created_date,
            target.last_update = source.last_update,
            target.followup_date = source.followup_date,
            target.first_name = source.first_name,
            target.last_name = source.last_name,
            target.state = source.state,
            target.country = source.country,
            target.required_items = source.required_items,
            target.is_locked = source.is_locked,
            target.is_returning = source.is_returning,
            target.last_csr = source.last_csr,
            target.last_csr_name = source.last_csr_name,
            target.lead_sequence_id = source.lead_sequence_id,
            target.priority_note = source.priority_note,
            target.language = source.language,
            target.status_extended = source.status_extended,
            target.approver_id = source.approver_id,
            target.originator_id = source.originator_id,
            target.withdrawer_id = source.withdrawer_id,
            target.contact_attempts = source.contact_attempts,
            target.last_contact_date = source.last_contact_date,
            target.bad_contact_info_flag = source.bad_contact_info_flag,
            target.n_outstanding_items = source.n_outstanding_items,
            target.task_remaining_flat = source.task_remaining_flat,
            target.approval_timestamp = source.approval_timestamp,
            target.qa_timestamp = source.qa_timestamp,
            target.payment_type = source.payment_type,
            target.payment_type_authorized = source.payment_type_authorized,
            target.campaign_id = source.campaign_id,
            target.campaign_name = source.campaign_name,
            target.q_check_valid = source.q_check_valid,
            target.q_check_agent = source.q_check_agent,
            target.is_nobankcall = source.is_nobankcall,
            target.queue_priority = source.queue_priority,
            target.last_express_time = source.last_express_time,
            target.spousal_notice = source.spousal_notice,
            target.cure_version = source.cure_version,
            target.cure_datafixed = source.cure_datafixed,
            target.withdrawn_timestamp = source.withdrawn_timestamp,
            target.kyc_start_timestamp = source.kyc_start_timestamp,
            target.dl_status = source.dl_status,
            target.web_lasttouch = source.web_lasttouch,
            target.web_lastpage = source.web_lastpage,
            target.is_dm_customer = source.is_dm_customer,
            target.is_limit_override_approved = source.is_limit_override_approved,
            target.qa_type = source.qa_type,
            target.is_dummy = source.is_dummy,
            target.is_nc_auto_fund = source.is_nc_auto_fund,
            target.is_nc_auto_fund_flow = source.is_nc_auto_fund_flow,
            target.is_auto_fund = source.is_auto_fund,
            target.nc_average_payment_amount = source.nc_average_payment_amount,
            target.rc_average_payment_amount = source.rc_average_payment_amount,
            target.risk_level_id = source.risk_level_id,
            target.risk_code = source.risk_code,
            target.risk_level_description = source.risk_level_description,
            target.bv_system_id = source.bv_system_id,
            target.tls_loan_id = source.tls_loan_id,
            target.is_test_loan = source.is_test_loan,
            target.is_auto_fund_agent_assisted = source.is_auto_fund_agent_assisted,
            target.referrer_id = source.referrer_id,
            target.agent_open_time = source.agent_open_time,
            target.created_datetime = source.created_datetime,
            target.updated_datetime = source.updated_datetime,
            target.process_timestamp = source.process_timestamp
            ---
        WHEN NOT MATCHED THEN
        INSERT
        (base_loan_id,
        lms_code,
        customer_id,
        loan_header_id,
        origination_date,
        origination_timestamp,
        effective_date,
        due_date,
        loan_type,
        lms_entity_id,
        entity_name,
        organization_id,
        organization_name,
        stated_apr,
        max_installments,
        max_payments,
        loan_fee_pct,
        -- loan_interest_pct,
        additional_fees,
        loan_owner,
        loan_status,
        lms_status,
        funding_method,
        loan_amount,
        requested_loan_amount, 
        is_paying,
        last_due_date,
        is_deleted,
        paid_off_date,
        first_pd,
        is_e_consent,
        is_online_draw_enabled,
        cancellation_type,
        is_technology_fee_charged,
        is_cach_allowed, -- DAT-4723
        created_date,
        last_update,
        followup_date,
        first_name, 
        last_name,
        state,
        country,
        required_items,
        is_locked,
        is_returning,
        last_csr,
        last_csr_name,
        lead_sequence_id,
        priority_note,
        language,
        status_extended,
        approver_id,
        originator_id,
        withdrawer_id,
        contact_attempts,
        last_contact_date,
        bad_contact_info_flag,
        n_outstanding_items,
        task_remaining_flat,
        approval_timestamp,
        qa_timestamp,
        payment_type,
        payment_type_authorized,
        campaign_id,
        campaign_name,
        q_check_valid,
        q_check_agent,
        is_nobankcall,
        queue_priority,
        last_express_time,
        spousal_notice,
        cure_version,
        cure_datafixed,
        withdrawn_timestamp,
        kyc_start_timestamp,
        dl_status,
        web_lasttouch,
        web_lastpage,
        is_dm_customer,
        is_limit_override_approved,
        qa_type,
        is_dummy,
        is_nc_auto_fund,
        is_nc_auto_fund_flow, 
        is_auto_fund, -- DAT-4788
        nc_average_payment_amount,
        rc_average_payment_amount,
        risk_level_id,
        risk_code,
        risk_level_description,
        bv_system_id, -- DAT-2723
        tls_loan_id,
        is_test_loan,
        is_auto_fund_agent_assisted,
        referrer_id, -- DAT-7421
        agent_open_time, -- DAT-8710,
        created_datetime,
        updated_datetime,
        process_timestamp)
        VALUES
        (
        source.base_loan_id,
        source.lms_code,
        source.customer_id,
        source.loan_header_id,
        source.origination_date,
        source.origination_timestamp,
        source.effective_date,
        source.due_date,
        source.loan_type,
        source.lms_entity_id,
        source.entity_name,
        source.organization_id,
        source.organization_name,
        source.stated_apr,
        source.max_installments,
        source.max_payments,
        source.loan_fee_pct,
        -- source.loan_interest_pct,
        source.additional_fees,
        source.loan_owner,
        source.loan_status,
        source.lms_status,
        source.funding_method,
        source.loan_amount,
        source.requested_loan_amount, 
        source.is_paying,
        source.last_due_date,
        source.is_deleted,
        source.paid_off_date,
        source.first_pd,
        source.is_e_consent,
        source.is_online_draw_enabled,
        source.cancellation_type,
        source.is_technology_fee_charged,
        source.is_cach_allowed, 
        source.created_date,
        source.last_update,
        source.followup_date,
        source.first_name, 
        source.last_name,
        source.state,
        source.country,
        source.required_items,
        source.is_locked,
        source.is_returning,
        source.last_csr,
        source.last_csr_name,
        source.lead_sequence_id,
        source.priority_note,
        source.language,
        source.status_extended,
        source.approver_id,
        source.originator_id,
        source.withdrawer_id,
        source.contact_attempts,
        source.last_contact_date,
        source.bad_contact_info_flag,
        source.n_outstanding_items,
        source.task_remaining_flat,
        source.approval_timestamp,
        source.qa_timestamp,
        source.payment_type,
        source.payment_type_authorized,
        source.campaign_id,
        source.campaign_name,
        source.q_check_valid,
        source.q_check_agent,
        source.is_nobankcall,
        source.queue_priority,
        source.last_express_time,
        source.spousal_notice,
        source.cure_version,
        source.cure_datafixed,
        source.withdrawn_timestamp,
        source.kyc_start_timestamp,
        source.dl_status,
        source.web_lasttouch,
        source.web_lastpage,
        source.is_dm_customer,
        source.is_limit_override_approved,
        source.qa_type,
        source.is_dummy,
        source.is_nc_auto_fund,
        source.is_nc_auto_fund_flow, 
        source.is_auto_fund, 
        source.nc_average_payment_amount,
        source.rc_average_payment_amount,
        source.risk_level_id,
        source.risk_code,
        source.risk_level_description,
        source.bv_system_id, 
        source.tls_loan_id,
        source.is_test_loan,
        source.is_auto_fund_agent_assisted,
        source.referrer_id, 
        source.agent_open_time, 
        source.created_datetime,
        source.updated_datetime,
        source.process_timestamp
        );

        -- Logging to OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_EXECUTION_LOGS:
            
        LN_ROW_COUNT      := SQLROWCOUNT;
        LC_LOG_TYPE       := ''Merge'';
        LC_LOG_LABEL      := CONCAT(''Merge data into DIM_LOANS table'');
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

    -- 403-412
    ld_start_datetime := current_timestamp();     

    UPDATE DWH_DEV.DWH.DIM_LOAN dl
    SET campaign_name = (
        SELECT ls.description
        FROM OPENFLOW_LANDING.JAGLMS.LEAD_SOURCE ls
        WHERE dl.campaign_id = ls.lead_source_id
    )
    WHERE
    dl.process_timestamp >= :ld_check_point_start
    AND dl.process_timestamp >= :ETL_START_DATE --- seems redundant
    AND dl.campaign_id > 0
    AND dl.campaign_name IS NULL;

        -- Logging to OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_EXECUTION_LOGS:
            
        LN_ROW_COUNT      := SQLROWCOUNT;
        LC_LOG_TYPE       := ''UPDATE'';
        LC_LOG_LABEL      := CONCAT(''UPDATE DIM_LOANS table [new entries] based on JAGLMS.LEAD_SOURCE'');
        LC_LOG_MESSAGE    := CONCAT(''UPDATE succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
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

    -- 437-447
    ld_start_datetime := current_timestamp();     

    UPDATE DWH_DEV.DWH.DIM_LOAN dl 
    SET 
        dl.loan_status = ''Withdraw''
    WHERE
    dl.process_timestamp >= :ld_check_point_start
    AND dl.lms_status = ''Withdraw''
    AND dl.loan_status IN (''Approve'' , ''Bank QA Error'', ''Pending'', ''Pending Bank Approval'', ''QA Error'', ''Void'');

        -- Logging to OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_EXECUTION_LOGS:
            
        LN_ROW_COUNT      := SQLROWCOUNT;
        LC_LOG_TYPE       := ''UPDATE'';
        LC_LOG_LABEL      := CONCAT(''UPDATE DIM_LOANS table [new entries] for Withdraw lms_status and certain loan_status values'');
        LC_LOG_MESSAGE    := CONCAT(''UPDATE succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
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


    -- 472-483
    ld_start_datetime := current_timestamp();     

    UPDATE DWH_DEV.DWH.DIM_LOAN target
    SET 
        lms_status = source.status_new,
        loan_status = source.loan_status_new
    FROM (

        SELECT dl.base_loan_id,
        lh.status as status_new,
        bl.loan_status  as loan_status_new
        FROM DWH_DEV.DWH.DIM_LOAN dl
        JOIN OPENFLOW_LANDING.JAGLMS.LMS_LOAN_HEADER lh ON dl.loan_header_id = lh.loan_header_id
        JOIN OPENFLOW_LANDING.JAGLMS.LMS_BASE_LOANS bl ON dl.base_loan_id = bl.base_loan_id
        WHERE dl.lms_status <> lh.status
        OR dl.loan_status <> bl.loan_status
        ) as source

    WHERE target.base_loan_id = source.base_loan_id
    AND target.process_timestamp >= :ld_check_point_start;                                                                

        -- Logging to OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_EXECUTION_LOGS:
            
        LN_ROW_COUNT      := SQLROWCOUNT;
        LC_LOG_TYPE       := ''UPDATE'';
        LC_LOG_LABEL      := CONCAT(''UPDATE DIM_LOANS table [new entries] when lms_status or loan_status differ with LMS_LOAN_HEADER or LMS_BASE_LOANS'');
        LC_LOG_MESSAGE    := CONCAT(''UPDATE succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
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

    --- 484 -491
    --- This part seems quite redundant but let''s ask:

    ld_start_datetime := current_timestamp();     

    UPDATE DWH_DEV.DWH.DIM_LOAN target
    SET 
        lms_status = source.status_new,
        loan_status = source.loan_status_new
    FROM (

        SELECT dl.base_loan_id,
        lh.status as status_new,
        bl.loan_status  as loan_status_new
        FROM DWH_DEV.DWH.DIM_LOAN dl
        JOIN OPENFLOW_LANDING.JAGLMS.LMS_LOAN_HEADER lh ON dl.loan_header_id = lh.loan_header_id
        JOIN OPENFLOW_LANDING.JAGLMS.LMS_BASE_LOANS bl ON dl.base_loan_id = bl.base_loan_id
        WHERE dl.lms_status <> lh.status
        OR dl.loan_status <> bl.loan_status
        ) as source

    WHERE target.base_loan_id = source.base_loan_id 
    and hour(current_timestamp())=3;

        -- Logging to OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_EXECUTION_LOGS:
            
        LN_ROW_COUNT      := SQLROWCOUNT;
        LC_LOG_TYPE       := ''UPDATE'';
        LC_LOG_LABEL      := CONCAT(''UPDATE DIM_LOANS table [hour=3 - whole table] when lms_status or loan_status differ with LMS_LOAN_HEADER or LMS_BASE_LOANS'');
        LC_LOG_MESSAGE    := CONCAT(''UPDATE succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
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


    --- 544-551

    ld_start_datetime := current_timestamp();     

    UPDATE  DWH_DEV.DWH.DIM_LOAN dl 
    SET 
        withdrawn_timestamp = last_update
    WHERE
    loan_status = ''Withdraw''
    AND withdrawn_timestamp IS NULL
    AND dl.process_timestamp >= :ld_check_point_start;                                                               

        -- Logging to OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_EXECUTION_LOGS:
            
        LN_ROW_COUNT      := SQLROWCOUNT;
        LC_LOG_TYPE       := ''UPDATE'';
        LC_LOG_LABEL      := CONCAT(''UPDATE DIM_LOANS table [new entries] with Withdraw loan_status and NULL withdrawn_timestamp'');
        LC_LOG_MESSAGE    := CONCAT(''UPDATE succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
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

    -- 577-589

    ld_start_datetime := current_timestamp();

    UPDATE DWH_DEV.DWH.reject_log  rl 
    SET 
        rl.is_fixed = 1,
        rl.fixed_date = current_date
    WHERE
    rl.table_name = ''lms_loan_header''
    AND rl.column_name = ''loan_header_id''
    AND (SELECT COUNT(*) FROM OPENFLOW_LANDING.JAGLMS.LMS_LOAN_HEADER WHERE loan_header_id = rl.column_value) = 0;

        -- Logging to OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_EXECUTION_LOGS:
            
        LN_ROW_COUNT      := SQLROWCOUNT;
        LC_LOG_TYPE       := ''UPDATE'';
        LC_LOG_LABEL      := CONCAT(''UPDATE DWH.REJECT_LOG table'');
        LC_LOG_MESSAGE    := CONCAT(''UPDATE succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
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

    -- 613-619      

    ld_start_datetime := current_timestamp();

    UPDATE DWH_DEV.DWH.DIM_LOAN dl
    SET dl.loan_amount = bl.loan_amount
    FROM OPENFLOW_LANDING.JAGLMS.LMS_BASE_LOANS bl
    WHERE dl.base_loan_id = bl.base_loan_id
    AND dl.loan_amount <> bl.loan_amount;


        -- Logging to OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_EXECUTION_LOGS:
            
        LN_ROW_COUNT      := SQLROWCOUNT;
        LC_LOG_TYPE       := ''UPDATE'';
        LC_LOG_LABEL      := CONCAT(''UPDATE DIM_LOAN table [whole table] when loan_amount diverge with LMS_BASE_LOANS'');
        LC_LOG_MESSAGE    := CONCAT(''UPDATE succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
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

    -- TO-DO: 644-658

    ld_start_datetime := current_timestamp();

    insert into DWH_DEV.DWH.reject_log (table_name, column_name, column_value, reject_reason, process_datetime )
    select  ''lms_loan_header'', 
            ''loan_header_id'',
            lh.loan_header_id,
            ''No base loan record'',
            current_date
    from OPENFLOW_LANDING.JAGLMS.LMS_LOAN_HEADER lh
    left join OPENFLOW_LANDING.JAGLMS.LMS_BASE_LOANS  bl on lh.loan_header_id = bl.loan_header_id
    where bl.loan_header_id is null
    and lh.created_date >= :ld_check_point_start;  

        -- Logging to OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_EXECUTION_LOGS:
            
        LN_ROW_COUNT      := SQLROWCOUNT;
        LC_LOG_TYPE       := ''INSERT'';
        LC_LOG_LABEL      := CONCAT(''INSERT INTO DWH.REJECT_LOG table'');
        LC_LOG_MESSAGE    := CONCAT(''INSERT succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
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


    -- 721-729

    ld_start_datetime := current_timestamp();

    UPDATE DWH_DEV.DWH.DIM_LOAN dl
    SET 
        dl.delinquent_date = NULL,
        dl.default_date = NULL
    FROM OPENFLOW_LANDING.JAGLMS.LMS_BASE_LOANS bl
    WHERE 
    dl.base_loan_id = bl.base_loan_id
    AND bl.loan_status = ''Originated''
    AND dl.process_timestamp >= :ld_check_point_start;

    -- Logging to OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_EXECUTION_LOGS:
            
        LN_ROW_COUNT      := SQLROWCOUNT;
        LC_LOG_TYPE       := ''UPDATE'';
        LC_LOG_LABEL      := CONCAT(''UPDATE DIM_LOAN table [new entries] when loan_status is Originated in LMS_BASE_LOANS'');
        LC_LOG_MESSAGE    := CONCAT(''UPDATE succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
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


    -- 730-743:

    ld_start_datetime := current_timestamp();

    UPDATE DWH_DEV.DWH.DIM_LOAN target
    SET 
        target.delinquent_date = DATE(IFNULL(source.delinquency_date,
                    source.create_timestamp)),
        target.default_date = NULL
    FROM (

        SELECT dl.base_loan_id,
        map.delinquency_date as delinquency_date,
        map.create_timestamp  as create_timestamp
        FROM DWH_DEV.DWH.DIM_LOAN dl
        
        JOIN OPENFLOW_LANDING.JAGLMS.LMS_BASE_LOANS bl ON dl.base_loan_id = bl.base_loan_id
        JOIN OPENFLOW_LANDING.JAGLMS.COLLECTION_LMS_LOAN_MAP map ON bl.base_loan_id = map.base_loan_id 
        WHERE
        bl.loan_status = ''Delinquent''
        and bl.updated_datetime >= :ld_check_point_start
        ) as source

    WHERE target.base_loan_id = source.base_loan_id;

    -- Logging to OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_EXECUTION_LOGS:
            
        LN_ROW_COUNT      := SQLROWCOUNT;
        LC_LOG_TYPE       := ''UPDATE'';
        LC_LOG_LABEL      := CONCAT(''UPDATE DIM_LOAN table [new entries] when loan_status is Delinquent in LMS_BASE_LOANS'');
        LC_LOG_MESSAGE    := CONCAT(''UPDATE succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
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




    -- 746-770:

    ld_start_datetime := current_timestamp();

    UPDATE DWH_DEV.DWH.DIM_LOAN target
    SET 
        target.delinquent_date = DATE(IFNULL(source.delinquency_date, (SELECT MAX(lh.change_date) FROM OPENFLOW_LANDING.JAGLMS.LOAN_STATUS_CHANGE_HISTORY lh WHERE lh.loan_header_id = target.loan_header_id AND to_status = ''Delinquent''))),
        target.default_date = DATE(IFNULL(source.fully_defaulted_date, (SELECT MAX(lh.change_date) FROM OPENFLOW_LANDING.JAGLMS.LOAN_STATUS_CHANGE_HISTORY lh WHERE lh.loan_header_id = target.loan_header_id AND to_status LIKE ''DEFAULT%'')))
    FROM (

        SELECT dl.base_loan_id,
        map.delinquency_date as delinquency_date,
        map.create_timestamp  as create_timestamp,
        map.fully_defaulted_date as fully_defaulted_date
        FROM DWH_DEV.DWH.DIM_LOAN dl
        
        JOIN OPENFLOW_LANDING.JAGLMS.LMS_BASE_LOANS bl ON dl.base_loan_id = bl.base_loan_id
        JOIN OPENFLOW_LANDING.JAGLMS.COLLECTION_LMS_LOAN_MAP map ON bl.base_loan_id = map.base_loan_id 
        WHERE
        bl.loan_status LIKE ''DEFAULT%''
        AND bl.updated_datetime >= :ld_check_point_start
        ) as source

    WHERE target.base_loan_id = source.base_loan_id;

    -- Logging to OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_EXECUTION_LOGS:
            
        LN_ROW_COUNT      := SQLROWCOUNT;
        LC_LOG_TYPE       := ''UPDATE'';
        LC_LOG_LABEL      := CONCAT(''UPDATE DIM_LOAN table [new entries] when loan_status starts with DEFAULT in LMS_BASE_LOANS'');
        LC_LOG_MESSAGE    := CONCAT(''UPDATE succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
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






    -- 795-803:   

    ld_start_datetime := current_timestamp();


    UPDATE DWH_DEV.DWH.DIM_LOAN dl
    SET 
        dl.loan_interest_pct = bl.loan_interest_pct
    FROM OPENFLOW_LANDING.JAGLMS.LMS_BASE_LOANS bl
    WHERE 
    dl.organization_id = 1
    AND dl.base_loan_id = bl.base_loan_id
    AND bl.loan_type = ''install''
    AND bl.effective_date >= :ld_check_point_start;

    -- Logging to OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_EXECUTION_LOGS:
            
        LN_ROW_COUNT      := SQLROWCOUNT;
        LC_LOG_TYPE       := ''UPDATE'';
        LC_LOG_LABEL      := CONCAT(''UPDATE DIM_LOAN table [new entries] when organization_id is 1 and install loan_type in LMS_BASE_LOANS'');
        LC_LOG_MESSAGE    := CONCAT(''UPDATE succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
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


    -- 827-874:

    ld_start_datetime := current_timestamp();

    TRUNCATE IF EXISTS DWH_DEV.DWH.stg_loan_interest_pct;

    insert into DWH_DEV.DWH.stg_loan_interest_pct(base_loan_id, loan_interest_pct)
    -- Scenario 1  risk_level_id != 1 but loan missed record in loc_fee_structure
    SELECT
    lbl.base_loan_id,
    llep.value as loan_interest_pct
    FROM OPENFLOW_LANDING.JAGLMS.LMS_BASE_LOANS lbl
    INNER JOIN  OPENFLOW_LANDING.JAGLMS.LMS_CUSTOMER_INFO_FLAT lcif ON lcif.customer_id = lbl.customer_id
    INNER JOIN OPENFLOW_LANDING.JAGLMS.LMS_LOAN_HEADER llh ON lbl.loan_header_id = llh.loan_header_id
    INNER JOIN OPENFLOW_LANDING.JAGLMS.LOC_FEE_STRUCTURE lfs ON lbl.base_loan_id = lfs.base_loan_id
    INNER JOIN OPENFLOW_LANDING.JAGLMS.LMS_LOAN_ENTITY_PARAMETERS llep ON lbl.base_loan_id = llep.base_loan_id
    inner join (select base_loan_id, max(id) max_id from OPENFLOW_LANDING.JAGLMS.LOC_FEE_STRUCTURE group by base_loan_id) lfs1 on lfs.id = lfs1.max_id and lfs.base_loan_id = lfs1.base_loan_id
    WHERE lcif.organization_id IN (1, 5, 6, 7) and lbl.loan_type = ''loc'' -- not for SEP and other orgs
    AND llh.risk_level_id != 1
    AND llep.parameter_name = lfs.daily_interest_code
    and llh.created_date >= :ld_check_point_start
    union 
    -- Scenario 2 risk_level_id = 1 but loan missed record in loc_fee_structure
    SELECT
    lbl.base_loan_id,
    llep1.value as loan_interest_pct
    FROM OPENFLOW_LANDING.JAGLMS.LMS_BASE_LOANS  lbl
    INNER JOIN  OPENFLOW_LANDING.JAGLMS.LMS_CUSTOMER_INFO_FLAT lcif ON lcif.customer_id = lbl.customer_id
    INNER JOIN OPENFLOW_LANDING.JAGLMS.LMS_LOAN_HEADER llh ON lbl.loan_header_id = llh.loan_header_id
    INNER JOIN OPENFLOW_LANDING.JAGLMS.LMS_LOAN_ENTITY_PARAMETERS llep ON lbl.base_loan_id = llep.base_loan_id
    inner join OPENFLOW_LANDING.JAGLMS.LMS_LOAN_ENTITY_PARAMETERS llep1 ON lbl.base_loan_id = llep1.base_loan_id
    WHERE lcif.organization_id IN (1, 5, 6, 7) and lbl.loan_type = ''loc'' -- not for SEP and other orgs
    and not exists(select 1 from OPENFLOW_LANDING.JAGLMS.LOC_FEE_STRUCTURE l where l.base_loan_id = lbl.base_loan_id)
    AND llh.risk_level_id = 1
    AND llep.parameter_name = ''default_calculation_model'' and llep.value = llep1.parameter_name
    AND llep1.parameter_name like ''interest%'' 
    and llh.created_date >= :ld_check_point_start
    union
    -- Scenario 3 risk_level_id = 1 and loan has record in loc_fee_structure
    SELECT
    lbl.base_loan_id,
    llep.value as loan_interest_pct
    FROM OPENFLOW_LANDING.JAGLMS.LMS_BASE_LOANS lbl
    INNER JOIN OPENFLOW_LANDING.JAGLMS.LMS_CUSTOMER_INFO_FLAT lcif ON lcif.customer_id = lbl.customer_id
    INNER JOIN OPENFLOW_LANDING.JAGLMS.LMS_LOAN_HEADER llh ON lbl.loan_header_id = llh.loan_header_id
    INNER JOIN OPENFLOW_LANDING.JAGLMS.LOC_FEE_STRUCTURE lfs ON lbl.base_loan_id = lfs.base_loan_id
    INNER JOIN OPENFLOW_LANDING.JAGLMS.LMS_LOAN_ENTITY_PARAMETERS llep ON lbl.base_loan_id = llep.base_loan_id
    inner join (select base_loan_id, max(id) max_id from OPENFLOW_LANDING.JAGLMS.LMS_LOAN_ENTITY_PARAMETERS group by base_loan_id) lfs1 on lfs.id = lfs1.max_id and lfs.base_loan_id = lfs1.base_loan_id
    WHERE lcif.organization_id IN (1, 5, 6, 7) and lbl.loan_type = ''loc''  
    AND llh.risk_level_id = 1
    AND llep.parameter_name = lfs.daily_interest_code
    AND llh.created_date >= :ld_check_point_start;

    -- Logging to OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_EXECUTION_LOGS:
            
        LN_ROW_COUNT      := SQLROWCOUNT;
        LC_LOG_TYPE       := ''INSERT'';
        LC_LOG_LABEL      := CONCAT(''INSERT INTO stg_loan_interest_pct'');
        LC_LOG_MESSAGE    := CONCAT(''INSERT succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
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

    -- 875-882:

    ld_start_datetime := current_timestamp();

    UPDATE DWH_DEV.DWH.DIM_LOAN dl
    SET 
        dl.loan_interest_pct = aa.loan_interest_pct
    FROM DWH_DEV.DWH.stg_loan_interest_pct aa
    WHERE 
    dl.organization_id IN (1 , 5, 6, 7)
    AND dl.base_loan_id = aa.base_loan_id
    AND dl.loan_type = ''loc'';

    -- Logging to OP_ADMIN.OPERATIONS.ETL_JOB_BATCH_EXECUTION_LOGS:
            
        LN_ROW_COUNT      := SQLROWCOUNT;
        LC_LOG_TYPE       := ''UPDATE'';
        LC_LOG_LABEL      := CONCAT(''UPDATE DIM_LOAN table [whole table] when organization_id IN 1,5,6,7 using stg_loan_interest_pct table'');
        LC_LOG_MESSAGE    := CONCAT(''UPDATE succeeded, row count = '', NVL(LN_ROW_COUNT, 0));
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
        RETURN ''SP_LOAD_LOANS : Job is done'';

    
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


    END';
    {% endset %}
    {{ return(sql) }}
{% endmacro %}







