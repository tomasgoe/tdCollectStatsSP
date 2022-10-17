REPLACE PROCEDURE SP_RULE_CREATE_STATS_NEW
     (
/*****************************************
* --Example of CALL to COLLECT (make sure there are no spaces in any of the string parameters):
* CALL SP_RULE_CREATE_STATS_NEW ('P_DHV_LMS', 'MI_PROD_SPEC_02', 'LEGACY_PROCEDURE_CODE;REFERENCE_DATE;DEAL_ID,L_CUSTOMER_ID', 2, 20, 10, 1, 'y', 'n', out_sql_code, out_sql_state, out_return_message);
* will result in:
*  COLLECT STATISTICS
*        USING SAMPLE    2.00 PERCENT FOR CURRENT --2% sample
*        AND MAXVALUELENGTH          20 FOR CURRENT --20 bytes long histogram
*        AND THRESHOLD    10.00 PERCENT FOR CURRENT --recollect if data changed by 10% or more
*        AND THRESHOLD           1 DAYS FOR CURRENT --recollect if statistics older than 1 day
*   COLUMN (LEGACY_PROCEDURE_CODE),
*   COLUMN (REFERENCE_DATE),
*   COLUMN (DEAL_ID,L_CUSTOMER_ID)
*  ON P_DHV_LMS.MI_PROD_SPEC_02;
*
*
* --Example of CALL to DROP (make sure there are no spaces in any of the string parameters):
* CALL SP_RULE_CREATE_STATS_NEW ('P_DHV_LMS', 'MI_PROD_SPEC_02', 'REFERENCE_DATE;DEAL_ID,L_CUSTOMER_ID', null, null, null, null, null, 'y', out_sql_code, out_sql_state, out_return_message);
* will result in:
*  DROP STATISTICS
*   COLUMN (REFERENCE_DATE),
*   COLUMN (DEAL_ID,L_CUSTOMER_ID)
*  ON P_DHV_LMS.MI_PROD_SPEC_02;
*
*
* PURPOSE: Collect statistics on a table.
* INPUT PARAMS DESC.:
*   input_db:             Databasename
*   input_table:          Tablename
*   input_columns:        List of colummns to collect statistics on. Separated by ";".
*                         Columns in multicolumn stats separated by ",".
*                         If 'DROP ALL', then drops all statistics on given table. Other attributes are disregarded.
*                         If 'RECOLLECT ALL', then recollects all statistics on given table. Other attributes are disregarded.
*                         If 'RECOLLECT GTT', then recollects all statistics on given GTT (ON TEMPORARY). Other attributes are disregarded.
*   input_sample:         Percentage in case one collects sampled statistics.
*                         Valid values: 2.00-100.00; null to leave system default.
*   input_maxvaluelength: Maximum size for histogram values. For single-character statistics on CHARACTER and VARCHAR columns, n specifies the number of characters. For all other options, n specifies number of bytes.
*                         Valid values: INTEGER; null to leave system default.
*   input_threshold_perc: Recollect statistics if the percentage of change in the data exceeds the specified percentage.
*                         Valid values: 0.00-9999.99; null to leave system default. If "0" then "NO THRESHOLD PERCENT".
*   input_threshold_days: Recollect statistics if the age of the statistic is greater than or equal to the number of days specified.
*                         Valid values: 0-9999; null to leave system default. If "0" then "NO THRESHOLD DAYS"
*   input_curr_fg:        If the USING clause should be used only for the current collect, put 'y'. Otherwise, the change to the USING clause will be permanent.
*   input_delete_fg:      Drop statistics on defined columns. If 'y', all other input parameters will be ignored.
*                         Valid values: "y" - drop statistics.
*                                       Other values or null - collect statistics.
******************************************/
     IN input_db VARCHAR(20),
	 IN input_table VARCHAR(50),
     IN input_columns VARCHAR(10000),
     IN input_sample DECIMAL (5,2),
     IN input_maxvaluelength INTEGER,
     IN input_threshold_perc DECIMAL (6,2),
     IN input_threshold_days INTEGER,
     IN input_curr_fg CHAR(1),
     IN input_delete_fg CHAR(1),
     OUT out_sql_code INTEGER,  --return code of SP: 0 is 'Ok' else 'Error'
     OUT out_sql_state CHAR(5),   --return state of SP
     OUT out_return_message VARCHAR(256)  --return message of SP
     )

EXECUTE_PROC: BEGIN
  DECLARE lv_sql_request_txt  VARCHAR (32000) DEFAULT '';  --string where the whole dynamic script  to be executed will be loaded
  DECLARE lv_sql_sample_txt VARCHAR (45) DEFAULT ''; --sample setting
  DECLARE lv_sql_maxvaluelength_txt VARCHAR (45) DEFAULT ''; --maxvaluelength setting
  DECLARE lv_sql_threshold_perc_txt VARCHAR (45) DEFAULT ''; --threshold percent setting. If 0, then NO THRESHOLD
  DECLARE lv_sql_threshold_days_txt VARCHAR (45) DEFAULT ''; --threshold days setting. If 0, then NO THRESHOLD
  DECLARE lv_sql_for_current_txt VARCHAR (13) DEFAULT ''; --IF input_curr_fg = 'Y' THEN FOR CURRENT ELSE FOREVER
  DECLARE ColumnName_Counter    SMALLINT        DEFAULT  0;
  DECLARE ColumnName_Str          VARCHAR(2000);
  
  
  DECLARE EXIT HANDLER FOR SqlException, NOT FOUND
  BEGIN
  	SET out_sql_code=SqlCode;
  	SET out_sql_state=SqlState;
  	--SET OutReturnCode = SqlCode;
  	SET out_return_message = 'Failed!';
  END;

  STATS_RUN: BEGIN

    --drop statistics on table
    IF UPPER(input_columns) IN ('DROP ALL')  THEN
        BEGIN
        SET lv_sql_request_txt = 'DROP STATISTICS ';
        SET out_return_message = 'Drop all statistics.';
        LEAVE STATS_RUN;
        END;
    END IF;

    --recollect all statistics on table
    IF UPPER(input_columns) IN ('RECOLLECT ALL', 'RECOLLECT GTT')  THEN
        BEGIN
        SET lv_sql_request_txt = 'COLLECT STATISTICS ';
        SET out_return_message = 'Recollect all statistics.';
        LEAVE STATS_RUN;
        END;
    END IF;

        STATS_PROPERTIES: BEGIN

        --drop statistics on column(s)
        IF input_delete_fg IN ('y', 'Y')  THEN
            BEGIN
            SET lv_sql_request_txt = 'DROP STATISTICS ';
            SET out_return_message = 'Drop statistics.';
            LEAVE STATS_PROPERTIES;
            END;
        END IF;

        IF input_sample IS NULL THEN SET lv_sql_sample_txt = 'SYSTEM SAMPLE';
            ELSE SET lv_sql_sample_txt = 'SAMPLE '|| input_sample || ' PERCENT';
        END IF;

        IF input_maxvaluelength IS NULL THEN SET lv_sql_maxvaluelength_txt = 'SYSTEM MAXVALUELENGTH';
            ELSE SET lv_sql_maxvaluelength_txt = 'MAXVALUELENGTH '|| input_maxvaluelength;
        END IF;

        IF input_threshold_perc IS NULL THEN SET lv_sql_threshold_perc_txt = 'SYSTEM THRESHOLD PERCENT';
            ELSEIF input_threshold_perc = 0 THEN SET lv_sql_threshold_perc_txt = 'NO THRESHOLD PERCENT';
            ELSE SET lv_sql_threshold_perc_txt = 'THRESHOLD '|| input_threshold_perc || ' PERCENT';
        END IF;

        IF input_threshold_days IS NULL THEN SET lv_sql_threshold_days_txt = 'SYSTEM THRESHOLD DAYS';
            ELSEIF input_threshold_days = 0 THEN SET lv_sql_threshold_days_txt = 'NO THRESHOLD DAYS';
            ELSE SET lv_sql_threshold_days_txt = 'THRESHOLD '|| input_threshold_days || ' DAYS';
        END IF;

        IF input_curr_fg IN ('y', 'Y') THEN SET lv_sql_for_current_txt = 'FOR CURRENT';
            ELSE SET lv_sql_for_current_txt = '';
        END IF;


        SET lv_sql_request_txt = 'COLLECT STATISTICS
        USING '|| lv_sql_sample_txt || ' ' || lv_sql_for_current_txt  || '
        AND ' || lv_sql_maxvaluelength_txt || ' ' || lv_sql_for_current_txt  || '
        AND ' || lv_sql_threshold_perc_txt || ' ' || lv_sql_for_current_txt  || '
        AND ' || lv_sql_threshold_days_txt || ' ' || lv_sql_for_current_txt;

        END STATS_PROPERTIES;

    LoopA:
    LOOP
        SET ColumnName_Counter = ColumnName_Counter + 1;

        SET ColumnName_Str = Trim( StrTok(input_columns , ';', ColumnName_Counter) );

        IF (ColumnName_Str IS NULL) THEN
            LEAVE LoopA;
        END IF;

        IF (Trim (ColumnName_Str) = '') THEN
            ITERATE LoopA;
        END IF;

        IF (ColumnName_Counter > 1) THEN
            SET lv_sql_request_txt = lv_sql_request_txt || ',';
        END IF;

        SET lv_sql_request_txt = lv_sql_request_txt || Chr(10) || '  COLUMN (' || ColumnName_Str || ')';
    END LOOP LoopA;

  END STATS_RUN;

      IF UPPER(input_columns) IN ('RECOLLECT GTT')  THEN
        SET lv_sql_request_txt = lv_sql_request_txt || Chr(10) || ' ON TEMPORARY '|| input_db || '.' || input_table;
      ELSE
        SET lv_sql_request_txt = lv_sql_request_txt || Chr(10) || ' ON '|| input_db || '.' || input_table;
    END IF;


        CALL DBC.SysExecSQL(lv_sql_request_txt);
            
        SET out_sql_code = SqlCode;
		SET out_sql_state = SqlState;
        SET out_return_message = 'Completed.';


END EXECUTE_PROC;