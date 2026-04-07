#CredsCreation
#Creds creation for DB - API authentication

BEGIN                                                                          
dbms_cloud.create_credential (                                                 
    credential_name => 'gemini_credential',                                            
    username => 'google',                                                 
    password => 'your-api-key-goes-here'
);                             
END;                                                                           
/