**Search in you database using AI profiles**

This is for OCI ATP, Since we are using here Gemini API, "run-sql" action is not available, So we will be writing the code to manually fetch the SQL, Format it and execute it in Database.

1. Make Sure Policy and ACL is created, For step check out file : Prerequestities.md

2. Create AI Profile and use Authentication Key :

Google Gemini
You will need a Google AI Studio account and an API key in order to use Google Gemini.

https://ai.google.dev/  # AI Studio Account
https://aistudio.google.com/app/apikey  #API Key

For more information on other options check : https://apexapps.oracle.com/pls/apex/r/dbpm/livelabs/run-workshop?p210_wid=3831&p210_wec=&session=106018010204687

3. Test the AI profile - refer file : ProfileCreation.sql

4. In our case "run-sql" is not working and According to Oracle's documentation, the supported actions for this function include:

'showsql'
'explainsql'
'narrate'
'chat'
The 'run_sql' action is not among the supported options.

So we will be using custom code to fetch the SQL from API , format it and execute and display the results in Database.

Check DB-AI-SQL_V1 and DB-AI-SQL_V2 for the code.


**Note**
#To make AI more accurate, Use inputs like " Show me properties in location Like Brooklyn" , Here using "Like" will force AI to write the SQL using Like operation for more vast results.

# Add comments on table columns before adding metadata to AI profile. In this case AI will know what kind of data columns store. Like "For columns price: add comment "This is final non-negotiable price for all listed properties". So when you write something like "Show me all properties in location like Gurgaon for non-negotiable price range 120000 and 6000000" AI will know for price which needs to be used, Same can be done for all other columns as well.
