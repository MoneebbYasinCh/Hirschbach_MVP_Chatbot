#!/usr/bin/env python3
"""
Diagnose Azure SQL Database connection issues
"""

import os
import sys
import pyodbc
from dotenv import load_dotenv

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables
load_dotenv()

def test_sql_connection_string():
    """Test using SQL_CONNECTION_STRING from environment"""
    print("Testing SQL_CONNECTION_STRING from environment...")
    print("=" * 60)
    
    # Get the single connection string from environment
    connection_string = os.getenv("SQL_CONNECTION_STRING")
    
    if not connection_string:
        print("❌ SQL_CONNECTION_STRING not found in environment variables")
        print("Add this to your .env file:")
        print('SQL_CONNECTION_STRING="Driver={ODBC Driver 18 for SQL Server};Server=hmldatastore.database.windows.net;Database=Warehouse;Encrypt=yes;TrustServerCertificate=no;UID=BI_Reader;PWD=hepqir-3hybgrmu-wAvrhef"')
        return None
    
    print(f"Using connection string from environment:")
    print(f"  {connection_string[:80]}...")
    
    # Test different driver variations
    connection_strings = [
        # Format 1: Use the exact string from environment
        connection_string,
        
        # Format 2: Try with ODBC Driver 18 (if different)
        connection_string.replace("{SQL Server}", "{ODBC Driver 18 for SQL Server}"),
        
        # Format 3: Try with SQL Server driver (if different)
        connection_string.replace("{ODBC Driver 18 for SQL Server}", "{SQL Server}"),
        
        # Format 4: Add connection timeout if not present
        connection_string + ";Connection Timeout=30;" if "Connection Timeout" not in connection_string else connection_string,
    ]
    
    for i, conn_str in enumerate(connection_strings, 1):
        print(f"\nTest {i}: {conn_str[:50]}...")
        try:
            with pyodbc.connect(conn_str, timeout=10) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    if result:
                        print(f"✅ SUCCESS! Use this connection string format")
                        print(f"   Full string: {conn_str}")
                        return conn_str
        except Exception as e:
            print(f"❌ Failed: {str(e)[:100]}...")
    
    return None

def test_server_connectivity():
    """Test basic server connectivity"""
    print("\nTesting server connectivity...")
    print("=" * 60)
    
    import socket
    
    # Try to get server from SQL_CONNECTION_STRING first
    connection_string = os.getenv("SQL_CONNECTION_STRING")
    server = None
    
    if connection_string:
        # Extract server from connection string
        import re
        match = re.search(r'Server=([^;]+)', connection_string)
        if match:
            server = match.group(1)
    
    # Fallback to individual environment variable
    if not server:
        server = os.getenv("AZURE_SQL_SERVER")
    
    if not server:
        print("❌ No server found in SQL_CONNECTION_STRING or AZURE_SQL_SERVER")
        return False
    
    port = 1433
    
    try:
        print(f"Testing connection to {server}:{port}...")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex((server, port))
        sock.close()
        
        if result == 0:
            print("✅ Server is reachable on port 1433")
            return True
        else:
            print("❌ Cannot reach server on port 1433")
            print("   This might be a firewall issue")
            return False
    except Exception as e:
        print(f"❌ Network error: {str(e)}")
        return False

def test_alternative_drivers():
    """Test if we can find a better driver"""
    print("\nChecking for alternative drivers...")
    print("=" * 60)
    
    drivers = pyodbc.drivers()
    print(f"Available drivers: {drivers}")
    
    # Look for any driver that might work with Azure
    azure_compatible = []
    for driver in drivers:
        if any(keyword in driver.lower() for keyword in ['odbc', 'sql', 'server']):
            azure_compatible.append(driver)
    
    print(f"SQL-related drivers: {azure_compatible}")
    
    if not azure_compatible:
        print("❌ No suitable drivers found")
        print("   Consider installing Microsoft ODBC Driver 18 for SQL Server")
        return None
    
    return azure_compatible[0]

def test_with_different_driver(driver_name):
    """Test connection with a different driver"""
    print(f"\nTesting with driver: {driver_name}")
    print("=" * 60)
    
    server = os.getenv("AZURE_SQL_SERVER", "Hgrmldatastore.datafrgbase.windows.net")
    database = os.getenv("AZURE_SQL_DATABASE", "Warehouse")
    username = os.getenv("AZURE_SQL_USERNAME", "BI_Reader")
    password = os.getenv("AZURE_SQL_PASSWORD", "hepqir-3hybgrmu-wAvrhef")
    
    conn_str = f"DRIVER={{{driver_name}}};SERVER={server},1433;DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    
    try:
        with pyodbc.connect(conn_str, timeout=10) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result:
                    print(f"✅ SUCCESS with {driver_name}!")
                    return True
    except Exception as e:
        print(f"❌ Failed with {driver_name}: {str(e)[:100]}...")
    
    return False

def main():
    """Main diagnostic function"""
    print("Azure SQL Database Connection Diagnostics")
    print("=" * 60)
    
    # Test 1: Server connectivity
    if not test_server_connectivity():
        print("\n🔧 SOLUTION: Check Azure SQL Database firewall settings")
        print("   1. Go to Azure Portal")
        print("   2. Navigate to your SQL Database")
        print("   3. Go to 'Networking' or 'Firewalls and virtual networks'")
        print("   4. Add your current IP address to the firewall rules")
        print("   5. Or temporarily enable 'Allow Azure services and resources'")
        return
    
    # Test 2: SQL connection string
    working_conn_str = test_sql_connection_string()
    if working_conn_str:
        print(f"\n🎉 FOUND WORKING CONNECTION!")
        print(f"Use this connection string: {working_conn_str}")
        return
    
    # Test 3: Alternative drivers
    alt_driver = test_alternative_drivers()
    if alt_driver and test_with_different_driver(alt_driver):
        print(f"\n🎉 FOUND WORKING DRIVER: {alt_driver}")
        return
    
   
if __name__ == "__main__":
    main()
