# Oracle NLQ Studio

## How it actually works 

```
You type: "Show me top 10 employees by salary"
                    ↓
         Flask Backend receives message
                    ↓
    Anthropic API call with SQLcl MCP tools attached
    (list-connections, connect, disconnect, run-sql, run-sqlcl)
                    ↓
         Claude decides what tools to call:
           1. connect → "claude_hr"
           2. run-sql → "SELECT * FROM USER_TABLES"  (explores schema)
           3. run-sql → "SELECT /* Oracle NLQ Studio */ ..."  (actual query)
                    ↓
         Each tool call executes against real  sql -mcp  subprocess
         (official MCP Python SDK: StdioServerParameters + ClientSession)
                    ↓
         Results fed back to Claude → Claude writes final answer
                    ↓
         UI shows: tool call pills + markdown answer + result table
```

This is **identical** to how Claude Desktop + SQLcl works. The only difference:
- Claude Desktop config: `{ "command": "/path/sql", "args": ["-mcp"] }`
- This app: `StdioServerParameters(command=sqlcl_path, args=["-mcp"])`
Same subprocess, same MCP protocol, same tool calls.

---

## Setup

### 1. Install SQLcl 25.2+
Download from https://www.oracle.com/database/sqldeveloper/technologies/sqlcl/

### 2. Save your Oracle connections (one-time)
```bash
sql /nolog
SQL> conn -save HCM_DEV  -savepwd hcm_user/pass@host:1521/hcmpdb
SQL> conn -save GL_PROD  -savepwd gl_user/pass@host:1521/glpdb
SQL> conn -save AP_UAT   -savepwd ap_user/pass@host:1521/appdb
SQL> connmgr list   # verify
SQL> exit
```

### 3. Install Python deps
```bash
pip install flask flask-cors "mcp[cli]" anthropic openpyxl reportlab
```

### 4. Start backend
```bash
ANTHROPIC_API_KEY=sk-ant-...  SQLCL_PATH=/opt/sqlcl/bin/sql  python app.py
```

### 5. Open frontend
Open `frontend/index.html` in any browser (or serve via `python -m http.server 8080 --directory frontend`)

---

## Usage

1. Click **⚙ Configure** → set SQLcl path + Anthropic key + backend URL → Save
2. Click **Start MCP** → SQLcl subprocess launches, MCP handshake completes
3. Click a database in the sidebar to connect
4. Type anything: *"Show me all AP invoices over $10,000"*
5. Claude autonomously:
   - Calls `run-sql` to explore tables/schema
   - Builds and runs the actual SQL
   - Shows results in the chat with an export option
6. DML operations show a confirmation banner before executing

