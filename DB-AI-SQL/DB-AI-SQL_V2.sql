DECLARE
    v_sql       CLOB;
    v_stmt      VARCHAR2(32767);
    cur_id      INTEGER;
    col_cnt     INTEGER;
    desc_tab    DBMS_SQL.DESC_TAB;
    col_val     VARCHAR2(4000);
    col_widths  DBMS_SQL.NUMBER_TABLE;
    v_status    INTEGER;
    v_line      VARCHAR2(4000);
BEGIN
    -- Step 1: Generate SQL using DBMS_CLOUD_AI
    v_sql := DBMS_CLOUD_AI.GENERATE(
                prompt       => 'List the properties in Brooklyn under $30M from REAL_ESTATE_LISTINGS_V13',
                profile_name => 'GENAI',
                action       => 'showsql');

    -- Step 2: Convert CLOB to VARCHAR2
    v_stmt := DBMS_LOB.SUBSTR(v_sql, 32767, 1);

    -- Step 3: Open a cursor
    cur_id := DBMS_SQL.OPEN_CURSOR;

    -- Step 4: Parse the SQL statement
    DBMS_SQL.PARSE(cur_id, v_stmt, DBMS_SQL.NATIVE);

    -- Step 5: Describe the columns
    DBMS_SQL.DESCRIBE_COLUMNS(cur_id, col_cnt, desc_tab);

    -- Step 6: Initialize column widths based on column names
    FOR i IN 1 .. col_cnt LOOP
        col_widths(i) := LENGTH(desc_tab(i).col_name);
        DBMS_SQL.DEFINE_COLUMN(cur_id, i, col_val, 4000);
    END LOOP;

    -- Step 7: Execute the statement
    v_status := DBMS_SQL.EXECUTE(cur_id);

    -- Step 8: Determine maximum width for each column
    WHILE DBMS_SQL.FETCH_ROWS(cur_id) > 0 LOOP
        FOR i IN 1 .. col_cnt LOOP
            DBMS_SQL.COLUMN_VALUE(cur_id, i, col_val);
            IF LENGTH(col_val) > col_widths(i) THEN
                col_widths(i) := LENGTH(col_val);
            END IF;
        END LOOP;
    END LOOP;

    -- Step 9: Re-parse and re-execute the statement for actual data retrieval
    DBMS_SQL.CLOSE_CURSOR(cur_id);
    cur_id := DBMS_SQL.OPEN_CURSOR;
    DBMS_SQL.PARSE(cur_id, v_stmt, DBMS_SQL.NATIVE);
    FOR i IN 1 .. col_cnt LOOP
        DBMS_SQL.DEFINE_COLUMN(cur_id, i, col_val, 4000);
    END LOOP;
    v_status := DBMS_SQL.EXECUTE(cur_id);

    -- Step 10: Print header
    v_line := '';
    FOR i IN 1 .. col_cnt LOOP
        v_line := v_line || RPAD(desc_tab(i).col_name, col_widths(i) + 2);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE(v_line);

    -- Step 11: Print separator
    v_line := '';
    FOR i IN 1 .. col_cnt LOOP
        v_line := v_line || RPAD('-', col_widths(i) + 2, '-');
    END LOOP;
    DBMS_OUTPUT.PUT_LINE(v_line);

    -- Step 12: Fetch and display the results
    WHILE DBMS_SQL.FETCH_ROWS(cur_id) > 0 LOOP
        v_line := '';
        FOR i IN 1 .. col_cnt LOOP
            DBMS_SQL.COLUMN_VALUE(cur_id, i, col_val);
            v_line := v_line || RPAD(NVL(col_val, 'NULL'), col_widths(i) + 2);
        END LOOP;
        DBMS_OUTPUT.PUT_LINE(v_line);
    END LOOP;

    -- Step 13: Close the cursor
    DBMS_SQL.CLOSE_CURSOR(cur_id);

EXCEPTION
    WHEN OTHERS THEN
        IF DBMS_SQL.IS_OPEN(cur_id) THEN
            DBMS_SQL.CLOSE_CURSOR(cur_id);
        END IF;
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/
