import pyodbc 
# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
server = 'DESKTOP-AIJMJAR\\SQLEXPRESS'
# server = 'localhost'
# 'tcp:myserver.database.windows.net' 
database = 'nadc' 
username = 'osint' 
password = 'osint' 

conn_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password
print(conn_string)
# cnxn = pyodbc.connect(conn_string)
cnxn = pyodbc.connect(
    Trusted_Connection='No',
    Driver='{ODBC Driver 17 for SQL Server}',
    Server=server,
    Database='nadc',
    UID=username,
    PWD=password
)

# cnxn = pyodbc.connect(
#     Trusted_Connection='Yes',
#     Driver='{ODBC Driver 17 for SQL Server}',
#     Server=server,
#     Database=database
# )


cursor = cnxn.cursor()


#Sample select query
cursor.execute("SELECT @@version;") 
row = cursor.fetchone() 
while row: 
    print(row[0])
    row = cursor.fetchone()