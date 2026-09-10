create or replace task PROD.MAITENANCE_DWH_DATA.DAILY_0530_CET_TASK_FAILURE_ALERT_MONITOR
	warehouse=COMPUTE_WH
	schedule='USING CRON 30 5 * * * Europe/Berlin'
	COMMENT='Runs Snowflake task failure capture and email alert processing daily at 05:30 Europe/Berlin time'
	as CALL PROD.MAITENANCE_DWH_DATA.RUN_TASK_FAILURE_MONITORING();

CREATE OR REPLACE PROCEDURE PROD.MAITENANCE_DWH_DATA.RUN_TASK_FAILURE_MONITORING()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    v_capture_result VARCHAR;
    v_alert_result VARCHAR;
BEGIN

    CALL PROD.MAITENANCE_DWH_DATA.CAPTURE_TASK_FAILURES()
        INTO :v_capture_result;

    CALL PROD.MAITENANCE_DWH_DATA.SEND_TASK_FAILURE_ALERTS()
        INTO :v_alert_result;

    RETURN ''Monitoring completed. Capture result: ''
           || v_capture_result
           || '' | Alert result: ''
           || v_alert_result;

END;
';

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

    WHERE TH.NAME IN
    (
        ''DAILY_0400_CET_CORE_MODELS_REFRESH'',
        ''DAILY_0400_CET_DIM_MODELS_REFRESH'',
        ''DAILY_0400_CET_FACT_MODELS_REFRESH'',
        ''DAILY_0400_CET_PREP_MODELS_REFRESH'',
        ''DAILY_0400_CET_PREP_SNAPSHOTS_REFRESH'',
        ''DAILY_0400_CET_SEEDS_REFRESH'',
        ''MONTH1ST_0500_CET_FACT_SALES_ORDER_SNAPSHOT''
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
    v_count NUMBER DEFAULT 0;
    v_email_subject VARCHAR;
    v_failure_details VARCHAR;
    v_email_content VARCHAR;
BEGIN

    CREATE OR REPLACE TEMPORARY TABLE TEMP_TASK_FAILURE_ALERT_BATCH AS
    SELECT
        TASK_NAME,
        FAILURE_TIME,
        ERROR_MESSAGE,
        QUERY_ID,
        SCHEDULED_FROM,
        CREATED_AT
    FROM PROD.MAITENANCE_DWH_DATA.TASK_FAILURE_MONITOR
    WHERE ALERT_SENT = FALSE;

    SELECT COUNT(*)
    INTO :v_count
    FROM TEMP_TASK_FAILURE_ALERT_BATCH;

    IF (v_count = 0) THEN
        DROP TABLE IF EXISTS TEMP_TASK_FAILURE_ALERT_BATCH;

        RETURN ''No new failures'';
    END IF;

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
                    REPLACE(ERROR_MESSAGE, ''&'', ''&amp;''),
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

        ''</div>'',
        ''''
    )
    WITHIN GROUP (
        ORDER BY CREATED_AT
    )
    INTO :v_failure_details
    FROM TEMP_TASK_FAILURE_ALERT_BATCH;

    v_email_subject :=
        ''Snowflake Notifications - '' ||
        v_count ||
        CASE
            WHEN v_count = 1 THEN '' Task Failure''
            ELSE '' Task Failures''
        END;

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
        v_count ||
        CASE
            WHEN v_count = 1 THEN '' failed task execution.''
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

    CALL SYSTEM$SEND_EMAIL(
        ''TASK_EMAIL_INT'',
        ''gokulanandh@jmangroup.com,gorlilavanya@jmangroup.com'',
        :v_email_subject,
        :v_email_content,
        ''text/html''
    );

    UPDATE PROD.MAITENANCE_DWH_DATA.TASK_FAILURE_MONITOR AS M
    SET ALERT_SENT = TRUE
    WHERE EXISTS
    (
        SELECT 1
        FROM TEMP_TASK_FAILURE_ALERT_BATCH AS B
        WHERE B.QUERY_ID = M.QUERY_ID
    );

    DROP TABLE IF EXISTS TEMP_TASK_FAILURE_ALERT_BATCH;

    RETURN ''Snowflake notification sent. Failures included: '' ||
           v_count;

END;
';
