import mysql.connector

# Establish connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",  # Replace with your MySQL username
    password="Apple@1234",  # Replace with your MySQL password
    database="securities_master"  # Connect to your existing database
)

# Create a cursor object
cursor = conn.cursor()

# Execute a simple query to check tables in your database
cursor.execute("SHOW TABLES")

# Fetch and print results
print("Tables in securities_master database:")
for table in cursor:
    print(table)

# Close the connection
cursor.close()
conn.close()
