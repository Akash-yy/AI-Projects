Create a Select AI profile.

begin    
    -- Drop the profile in case it already exists
    dbms_cloud_ai.drop_profile(
        profile_name => 'genai',
        force => true
    );    

    -- Create an AI profile that uses the default Gemini model
    DBMS_CLOUD_AI.CREATE_PROFILE(
        profile_name => 'GENAI',
        attributes   => '{
            "provider": "google",
            "credential_name": "gemini_credential",
            "comments": "true",
            "object_list": [
                {"owner": "username", "name": "Table_Name"} # this is to share Metadata of your table to AI
            ]
        }'
    );
END;
/

Test the AI Profile

SELECT DBMS_CLOUD_AI.GENERATE(
    prompt       => 'List properties available like in Brooklyn under $300,000000 from REAL_ESTATE_LISTINGS_V13',
    profile_name => 'GENAI',
    action       => 'showsql')
FROM dual;
