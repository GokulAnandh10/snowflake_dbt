create or replace task PROD.MAITENANCE_DWH_DATA.DAILY_0530_CET_TASK_FAILURE_ALERT_MONITOR
	warehouse=COMPUTE_WH
	schedule='USING CRON 30 5 * * * Europe/Berlin'
	COMMENT='Runs Snowflake task failure capture and email alert processing daily at 05:30 Europe/Berlin time'
	as CALL PROD.MAITENANCE_DWH_DATA.RUN_TASK_FAILURE_MONITORING();

CREATE OR REPLACE PROCEDURE PROD.MAITENANCE_DWH_DATA.CAPTURE_TASK_FAILURES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    inserted_count NUMBER DEFAULT 0;
BEGIN

    INSERT INTO PROD.MAITENANCE_DWH_DATA.TASK_FAILURE_MONITOR
    (
        TASK_NAME,
        SCHEDULE_TYPE,
        FAILURE_TIME,
        SCHEDULED_TIME,
        QUERY_START_TIME,
        COMPLETED_TIME,
        ERROR_CODE,
        ERROR_MESSAGE,
        QUERY_ID,
        SCHEDULED_FROM
    )
    SELECT
        TH.NAME AS TASK_NAME,
        TH.SCHEDULED_FROM AS SCHEDULE_TYPE,

        COALESCE(
            TH.COMPLETED_TIME,
            TH.QUERY_START_TIME,
            TH.SCHEDULED_TIME
        ) AS FAILURE_TIME,

        TH.SCHEDULED_TIME,
        TH.QUERY_START_TIME,
        TH.COMPLETED_TIME,
        TH.ERROR_CODE,
        TH.ERROR_MESSAGE,
        TH.QUERY_ID,
        TH.SCHEDULED_FROM

    FROM TABLE(
        SNOWFLAKE.INFORMATION_SCHEMA.TASK_HISTORY(
            SCHEDULED_TIME_RANGE_START =>
                DATEADD(''day'', -3, CURRENT_TIMESTAMP()),

            SCHEDULED_TIME_RANGE_END =>
                CURRENT_TIMESTAMP(),

            RESULT_LIMIT => 10000
        )
    ) TH

    WHERE EXISTS
    (
        SELECT 1
        FROM PROD.MAITENANCE_DWH_DATA.TASK_NOTIFICATION_RECIPIENTS MT
        WHERE MT.TASK_NAME = TH.NAME
    )

      AND TH.STATE = ''FAILED''

      AND TH.QUERY_ID IS NOT NULL

      AND NOT EXISTS
      (
          SELECT 1
          FROM PROD.MAITENANCE_DWH_DATA.TASK_FAILURE_MONITOR M
          WHERE M.QUERY_ID = TH.QUERY_ID
      );

    inserted_count := SQLROWCOUNT;

    RETURN ''CAPTURE_TASK_FAILURES completed. Rows inserted: ''
           || inserted_count;

END;
';

CREATE OR REPLACE PROCEDURE PROD.MAITENANCE_DWH_DATA.SEND_TASK_FAILURE_ALERTS()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    v_total_failures NUMBER DEFAULT 0;
    v_recipient_count NUMBER DEFAULT 0;
    v_recipient_index NUMBER DEFAULT 1;
    v_group_failure_count NUMBER DEFAULT 0;
    v_group_count NUMBER DEFAULT 0;

    v_recipient_mail VARCHAR;
    v_failure_details VARCHAR;
    v_email_subject VARCHAR;
    v_email_content VARCHAR;

BEGIN

    ----------------------------------------------------------------
    -- 1. Prepare unsent failures and split comma-separated emails
    --    into one row per individual recipient
    ----------------------------------------------------------------

    CREATE OR REPLACE TEMPORARY TABLE TEMP_TASK_FAILURE_ALERT_BATCH AS
    SELECT DISTINCT
        M.TASK_NAME,
        M.FAILURE_TIME,
        M.ERROR_MESSAGE,
        M.QUERY_ID,
        M.SCHEDULED_FROM,
        M.CREATED_AT,
        TRIM(SPLIT_EMAIL.VALUE::STRING) AS MAIL
    FROM PROD.MAITENANCE_DWH_DATA.TASK_FAILURE_MONITOR M
    INNER JOIN PROD.MAITENANCE_DWH_DATA.TASK_NOTIFICATION_RECIPIENTS MT
        ON MT.TASK_NAME = M.TASK_NAME,
    LATERAL SPLIT_TO_TABLE(MT.MAIL, '','') SPLIT_EMAIL
    WHERE M.ALERT_SENT = FALSE
      AND MT.MAIL IS NOT NULL
      AND TRIM(MT.MAIL) <> ''''
      AND TRIM(SPLIT_EMAIL.VALUE::STRING) <> '''';


    ----------------------------------------------------------------
    -- 2. Count distinct unsent task failures
    ----------------------------------------------------------------

    SELECT COUNT(DISTINCT QUERY_ID)
    INTO :v_total_failures
    FROM TEMP_TASK_FAILURE_ALERT_BATCH;


    IF (v_total_failures = 0) THEN

        DROP TABLE IF EXISTS TEMP_TASK_FAILURE_ALERT_BATCH;

        RETURN ''No new failures'';

    END IF;


    ----------------------------------------------------------------
    -- 3. Create one recipient group per individual email address
    ----------------------------------------------------------------

    CREATE OR REPLACE TEMPORARY TABLE TEMP_TASK_ALERT_RECIPIENTS AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY MAIL) AS RN,
        MAIL
    FROM
    (
        SELECT DISTINCT MAIL
        FROM TEMP_TASK_FAILURE_ALERT_BATCH
        WHERE MAIL IS NOT NULL
          AND TRIM(MAIL) <> ''''
    );


    SELECT COUNT(*)
    INTO :v_recipient_count
    FROM TEMP_TASK_ALERT_RECIPIENTS;


    ----------------------------------------------------------------
    -- 4. Process each individual recipient
    ----------------------------------------------------------------

    WHILE (v_recipient_index <= v_recipient_count) DO

        SELECT MAIL
        INTO :v_recipient_mail
        FROM TEMP_TASK_ALERT_RECIPIENTS
        WHERE RN = :v_recipient_index;


        ----------------------------------------------------------------
        -- 5. Count distinct failures applicable to this recipient
        ----------------------------------------------------------------

        SELECT COUNT(DISTINCT QUERY_ID)
        INTO :v_group_failure_count
        FROM TEMP_TASK_FAILURE_ALERT_BATCH
        WHERE MAIL = :v_recipient_mail;


        ----------------------------------------------------------------
        -- 6. Build consolidated card-style failure details
        ----------------------------------------------------------------

        SELECT LISTAGG(

            ''<div style="font-family: Arial, sans-serif;'' ||
            '' font-size: 14px;'' ||
            '' line-height: 1.6;'' ||
            '' margin-bottom: 24px;'' ||
            '' padding: 16px;'' ||
            '' border: 1px solid #d9d9d9;'' ||
            '' border-left: 5px solid #d13438;'' ||
            '' border-radius: 4px;">'' ||

            ''<div><strong>Failed Task Name:</strong> '' ||
            COALESCE(TASK_NAME, ''N/A'') ||
            ''</div>'' ||

            ''<div><strong>Error Message:</strong><br>'' ||
            COALESCE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            ERROR_MESSAGE,
                            ''&'',
                            ''&amp;''
                        ),
                        ''<'',
                        ''&lt;''
                    ),
                    ''>'',
                    ''&gt;''
                ),
                ''N/A''
            ) ||
            ''</div>'' ||

            ''<div><strong>Query ID:</strong> '' ||
            COALESCE(QUERY_ID, ''N/A'') ||
            ''</div>'' ||

            ''<div><strong>Triggered From:</strong> '' ||
            COALESCE(SCHEDULED_FROM, ''N/A'') ||
            ''</div>'' ||

            ''<div><strong>Failure Time (CET/CEST):</strong> '' ||
            COALESCE(
                TO_VARCHAR(
                    CONVERT_TIMEZONE(
                        ''Europe/Berlin'',
                        FAILURE_TIME
                    ),
                    ''YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM''
                ),
                ''N/A''
            ) ||
            ''</div>'' ||

            ''</div>''

       , '''')
        WITHIN GROUP
        (
            ORDER BY CREATED_AT, TASK_NAME
        )
        INTO :v_failure_details
        FROM TEMP_TASK_FAILURE_ALERT_BATCH
        WHERE MAIL = :v_recipient_mail;


        ----------------------------------------------------------------
        -- 7. Build email subject for this recipient
        ----------------------------------------------------------------

        v_email_subject :=
            ''Snowflake Notifications - '' ||
            v_group_failure_count ||
            CASE
                WHEN v_group_failure_count = 1
                    THEN '' Task Failure''
                ELSE '' Task Failures''
            END;


        ----------------------------------------------------------------
        -- 8. Build consolidated HTML email content
        ----------------------------------------------------------------

        v_email_content :=
            ''<html>'' ||

            ''<body style="font-family: Arial, sans-serif;'' ||
            '' font-size: 14px;'' ||
            '' color: #242424;">'' ||

            ''<h2 style="font-family: Arial, sans-serif;'' ||
            '' color: #21145a;'' ||
            '' font-size: 22px;'' ||
            '' font-weight: 700;'' ||
            '' margin: 0 0 12px 0;">'' ||
            ''Snowflake Notifications'' ||
            ''</h2>'' ||

            ''<p>Snowflake detected '' ||
            v_group_failure_count ||
            CASE
                WHEN v_group_failure_count = 1
                    THEN '' failed task execution.''
                ELSE '' failed task executions.''
            END ||
            ''</p>'' ||

            ''<p style="font-size: 12px; color: #616161;">'' ||
            ''Note: All failure timestamps are displayed in '' ||
            ''Europe/Berlin time and automatically follow CET/CEST.'' ||
            ''</p>'' ||

            v_failure_details ||

            ''</body>'' ||
            ''</html>'';


        ----------------------------------------------------------------
        -- 9. Send one consolidated email to this recipient
        ----------------------------------------------------------------

        CALL SYSTEM$SEND_EMAIL(
            ''TASK_EMAIL_INT'',
            :v_recipient_mail,
            :v_email_subject,
            :v_email_content,
            ''text/html''
        );


        v_group_count := v_group_count + 1;
        v_recipient_index := v_recipient_index + 1;

    END WHILE;


    ----------------------------------------------------------------
    -- 10. Mark failures as sent only after all recipient emails
    --     have been processed successfully
    ----------------------------------------------------------------

    UPDATE PROD.MAITENANCE_DWH_DATA.TASK_FAILURE_MONITOR AS M
    SET ALERT_SENT = TRUE
    WHERE M.QUERY_ID IN
    (
        SELECT DISTINCT QUERY_ID
        FROM TEMP_TASK_FAILURE_ALERT_BATCH
    );


    ----------------------------------------------------------------
    -- 11. Cleanup
    ----------------------------------------------------------------

    DROP TABLE IF EXISTS TEMP_TASK_FAILURE_ALERT_BATCH;
    DROP TABLE IF EXISTS TEMP_TASK_ALERT_RECIPIENTS;


    ----------------------------------------------------------------
    -- 12. Return execution status
    ----------------------------------------------------------------

    RETURN
        ''Snowflake notification sent. Total distinct failures: ''
        || v_total_failures
        || '', Individual recipient groups: ''
        || v_group_count;

END;
';
