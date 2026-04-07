DECLARE
    v_sql       CLOB;
    v_stmt      VARCHAR2(32767);
    cur_id      INTEGER;
    col_cnt     INTEGER;
    desc_tab    DBMS_SQL.DESC_TAB;
    col_val     VARCHAR2(4000);
    col_num     NUMBER;
    v_status    INTEGER;
BEGIN
    -- Step 1: Generate SQL using DBMS_CLOUD_AI
    v_sql := DBMS_CLOUD_AI.GENERATE(
                prompt       => 'List all details from property like in Brooklyn between 599000 and 5799000 from REAL_ESTATE_LISTINGS_V13',
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

    -- Step 6: Define columns
    FOR i IN 1 .. col_cnt LOOP
        DBMS_SQL.DEFINE_COLUMN(cur_id, i, col_val, 4000);
    END LOOP;

    -- Step 7: Execute the statement
    v_status := DBMS_SQL.EXECUTE(cur_id);

    -- Step 8: Fetch and display the results
    WHILE DBMS_SQL.FETCH_ROWS(cur_id) > 0 LOOP
        FOR i IN 1 .. col_cnt LOOP
            DBMS_SQL.COLUMN_VALUE(cur_id, i, col_val);
            DBMS_OUTPUT.PUT_LINE(desc_tab(i).col_name || ': ' || col_val);
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('---');
    END LOOP;

    -- Step 9: Close the cursor
    DBMS_SQL.CLOSE_CURSOR(cur_id);

EXCEPTION
    WHEN OTHERS THEN
        IF DBMS_SQL.IS_OPEN(cur_id) THEN
            DBMS_SQL.CLOSE_CURSOR(cur_id);
        END IF;
        DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END;
/


