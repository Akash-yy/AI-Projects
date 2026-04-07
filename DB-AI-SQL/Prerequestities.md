#Prerequestities

Policy In OCI root:

allow any-user to manage generative-ai-family in tenancy

#Since I am working in OCI ATP, We will be using above policy, for other clouds process is same.

API Key : <API_KEY_OF_GEMINI>>

1. Grant the ADMIN network access to the Google Gemini endpoint.

BEGIN
DBMS_NETWORK_ACL_ADMIN.APPEND_HOST_ACE(
    host => 'generativelanguage.googleapis.com',
    ace  => xs$ace_type(privilege_list => xs$name_list('http'),
                        principal_name => 'username',
                        principal_type => xs_acl.ptype_db)
);
END;
/