#!/usr/bin/env python3
"""
Fix the server name in .env file
"""

import os
import shutil
from datetime import datetime

def fix_server_name():
    """Fix the server name in .env file"""
    print("Fixing server name in .env file...")
    
    # Create backup
    if os.path.exists(".env"):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f".env.backup_{timestamp}"
        shutil.copy2(".env", backup_name)
        print(f"✅ Created backup: {backup_name}")
    
    # Read current .env file
    with open(".env", "r") as f:
        content = f.read()
    
    # Fix the server name
    old_server = "hmldatastore.database.windows.net"
    new_server = "Hgrmldatastore.datafrgbase.windows.net"
    
    if old_server in content:
        content = content.replace(old_server, new_server)
        print(f"✅ Fixed server name: {old_server} → {new_server}")
    else:
        print("⚠️  Server name not found in expected format")
        print(f"Current content contains: {content[:200]}...")
    
    # Write the fixed content
    with open(".env", "w") as f:
        f.write(content)
    
    print("✅ .env file updated")
    
    # Verify the fix
    from dotenv import load_dotenv
    load_dotenv()
    
    server = os.getenv("AZURE_SQL_SERVER", "")
    print(f"Server name now: {server}")
    
    if server == new_server:
        print("✅ Server name is correct!")
        return True
    else:
        print("❌ Server name is still incorrect")
        return False

if __name__ == "__main__":
    print("Fixing Azure SQL Database server name")
    print("=" * 50)
    
    if fix_server_name():
        print("\n🎉 Server name fixed successfully!")
        print("Now run: python diagnose_azure_connection.py")
    else:
        print("\n❌ Failed to fix server name")
        print("Please manually edit .env file and change:")
        print("AZURE_SQL_SERVER=hmldatastore.database.windows.net")
        print("to:")
        print("AZURE_SQL_SERVER=Hgrmldatastore.datafrgbase.windows.net")
