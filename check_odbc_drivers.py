#!/usr/bin/env python3
"""
Check available ODBC drivers on your system
"""

import pyodbc

def check_drivers():
    """Check what ODBC drivers are available"""
    print("Checking available ODBC drivers on your system...")
    print("=" * 50)
    
    try:
        drivers = pyodbc.drivers()
        
        if not drivers:
            print("❌ No ODBC drivers found!")
            print("You may need to install the Microsoft ODBC Driver for SQL Server")
            return False
        
        print(f"✅ Found {len(drivers)} ODBC drivers:")
        print()
        
        sql_drivers = []
        for i, driver in enumerate(drivers, 1):
            print(f"{i:2d}. {driver}")
            if 'sql server' in driver.lower() or 'odbc driver' in driver.lower():
                sql_drivers.append(driver)
        
        print()
        print("SQL Server related drivers:")
        if sql_drivers:
            for driver in sql_drivers:
                print(f"  ✅ {driver}")
        else:
            print("  ❌ No SQL Server drivers found")
            print("  You may need to install: Microsoft ODBC Driver 18 for SQL Server")
        
        print()
        print("Recommended driver for your setup:")
        if any('18' in driver for driver in sql_drivers):
            recommended = next((driver for driver in sql_drivers if '18' in driver), None)
            print(f"  🎯 {recommended}")
        elif any('17' in driver for driver in sql_drivers):
            recommended = next((driver for driver in sql_drivers if '17' in driver), None)
            print(f"  🎯 {recommended}")
        elif sql_drivers:
            print(f"  🎯 {sql_drivers[0]}")
        else:
            print("  ❌ No suitable SQL Server driver found")
            print("  Install: Microsoft ODBC Driver 18 for SQL Server")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking drivers: {str(e)}")
        return False

if __name__ == "__main__":
    check_drivers()
