import mysql.connector as mysql
import time


class DbConnector:
    """
    Connects to the MySQL server on the Ubuntu virtual machine.
    Connector needs HOST, DATABASE, USER and PASSWORD to connect,
    while PORT is optional and should be 3306.

    Example:
    HOST = "tdt4225-00.idi.ntnu.no" // Your server IP address/domain name
    DATABASE = "testdb" // Database name, if you just want to connect to MySQL server, leave it empty
    USER = "testuser" // This is the user you created and added privileges for
    PASSWORD = "test123" // The password you set for said user
    """

    def __init__(self,
                 HOST="tdt4225-37.idi.ntnu.no",
                 DATABASE="porto_taxi",
                 USER="jensc",
                 PASSWORD="gruppe37"):
        # Store connection parameters for reconnection
        self.HOST = HOST
        self.DATABASE = DATABASE
        self.USER = USER
        self.PASSWORD = PASSWORD
        
        # Connect to the database
        self.connect()

    def connect(self):
        """Establish database connection"""
        try:
            self.db_connection = mysql.connect(
                host=self.HOST, 
                database=self.DATABASE, 
                user=self.USER, 
                password=self.PASSWORD, 
                port=3306,
                autocommit=False
            )
            self.cursor = self.db_connection.cursor()
            
            print("Connected to:", self.db_connection.get_server_info())
            # get database information
            self.cursor.execute("select database();")
            database_name = self.cursor.fetchone()
            print("You are connected to the database:", database_name)
            print("-----------------------------------------------\n")
            
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)
            raise

    def reconnect(self, max_retries=3, delay=5):
        """Reconnect to database with retry logic"""
        for attempt in range(max_retries):
            try:
                print(f"Attempting to reconnect (attempt {attempt + 1}/{max_retries})...")
                if hasattr(self, 'cursor') and self.cursor:
                    try:
                        self.cursor.close()
                    except:
                        pass
                if hasattr(self, 'db_connection') and self.db_connection:
                    try:
                        self.db_connection.close()
                    except:
                        pass
                
                self.connect()
                print("Reconnection successful!")
                return True
                
            except Exception as e:
                print(f"Reconnection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    print(f"Waiting {delay} seconds before retry...")
                    time.sleep(delay)
        
        print("Failed to reconnect after all attempts")
        return False

    def close_connection(self):
        # close the cursor
        try:
            if hasattr(self, 'cursor') and self.cursor:
                self.cursor.close()
        except:
            pass
            
        # close the DB connection
        try:
            if hasattr(self, 'db_connection') and self.db_connection:
                server_info = self.db_connection.get_server_info()
                self.db_connection.close()
                print("\n-----------------------------------------------")
                print("Connection to %s is closed" % server_info)
        except:
            print("\n-----------------------------------------------")
            print("Connection to None is closed")
