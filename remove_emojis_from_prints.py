#!/usr/bin/env python3
"""
Script to remove emojis from print statements in Python files
Uses regular expressions to identify and remove Unicode emojis from print statements
"""

import os
import re
import glob
import sys
from typing import List, Tuple

# Comprehensive emoji regex pattern covering most Unicode emoji ranges
EMOJI_PATTERN = re.compile(
    r'[\U0001F600-\U0001F64F'  # Emoticons
    r'\U0001F300-\U0001F5FF'  # Misc Symbols and Pictographs
    r'\U0001F680-\U0001F6FF'  # Transport and Map
    r'\U0001F1E0-\U0001F1FF'  # Flags (iOS)
    r'\U00002500-\U00002BEF'  # Chinese/Japanese/Korean
    r'\U00002702-\U000027B0'  # Dingbats
    r'\U000024C2-\U0001F251'  # Enclosed characters
    r'\U0001f926-\U0001f937'  # Gestures
    r'\U00010000-\U0010ffff'  # Other unicode ranges that might contain emojis
    r'\u2640-\u2642'  # Gender symbols
    r'\u2600-\u2B55'  # Misc symbols
    r'\u200d'  # Zero width joiner
    r'\u23cf'  # Eject symbol
    r'\u23e9'  # Fast forward
    r'\u231a'  # Watch
    r'\ufe0f'  # Variation selector
    r'\u3030'  # Wavy dash
    r']+',
    flags=re.UNICODE
)

# Additional pattern for common emoji-like characters that might not be in the main ranges
ADDITIONAL_EMOJI_CHARS = re.compile(r'[🎯🎉✅❌⚠️🔧📊📋🚀💡🧪🎮📡👋🔍🔄✅📝📋🔍📊🎯✅📊📋🚀💡🧪🎮📡👋🔍🔄📝✅📋🔍📊🎯]', flags=re.UNICODE)

def find_python_files(directory: str) -> List[str]:
    """Find all Python files in the given directory recursively"""
    python_files = []
    for root, dirs, files in os.walk(directory):
        # Skip venv and __pycache__ directories
        dirs[:] = [d for d in dirs if d not in ['venv', '__pycache__', '.git', 'node_modules']]

        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))

    return python_files

def extract_print_statements(content: str) -> List[Tuple[str, int, str]]:
    """Extract all print statements from Python code with their line numbers and context"""
    print_statements = []

    lines = content.split('\n')
    for line_num, line in enumerate(lines, 1):
        # Find all print statements in the line
        print_matches = re.finditer(r'print\s*\([^)]*\)', line)
        for match in print_matches:
            print_statements.append((match.group(), line_num, line.strip()))

    return print_statements

def remove_emojis_from_print_statement(print_statement: str) -> str:
    """Remove emojis from a print statement while preserving the rest"""
    # Remove emojis from the entire print statement
    cleaned = EMOJI_PATTERN.sub('', print_statement)
    cleaned = ADDITIONAL_EMOJI_CHARS.sub('', cleaned)

    # Clean up extra whitespace that might result from emoji removal
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = cleaned.strip()

    return cleaned

def has_emojis(text: str) -> bool:
    """Check if text contains any emojis"""
    return bool(EMOJI_PATTERN.search(text) or ADDITIONAL_EMOJI_CHARS.search(text))

def process_file(file_path: str, dry_run: bool = True) -> Tuple[int, int]:
    """Process a single file to remove emojis from print statements"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()

        lines = original_content.split('\n')
        modified_lines = []
        changes_made = 0
        lines_changed = 0

        for line_num, line in enumerate(lines, 1):
            original_line = line
            # Find all print statements in the line
            print_matches = list(re.finditer(r'(print\s*\([^)]*\))', line))

            for match in print_matches:
                original_print = match.group(1)
                if has_emojis(original_print):
                    cleaned_print = remove_emojis_from_print_statement(original_print)
                    if cleaned_print != original_print:
                        # Replace the print statement in the line
                        line = line.replace(original_print, cleaned_print)
                        changes_made += 1

            if line != original_line:
                lines_changed += 1

            modified_lines.append(line)

        if changes_made > 0:
            new_content = '\n'.join(modified_lines)
            if not dry_run:
                # Create backup
                backup_path = file_path + '.bak'
                with open(backup_path, 'w', encoding='utf-8') as f:
                    f.write(original_content)

                # Write the cleaned content
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(new_content)

                print(f"SUCCESS: {file_path}: Removed emojis from {changes_made} print statements ({lines_changed} lines changed)")
            else:
                print(f"PREVIEW: {file_path}: Would remove emojis from {changes_made} print statements ({lines_changed} lines changed)")
                # Show a preview of changes
                preview_lines = []
                for i, line in enumerate(modified_lines):
                    if line != lines[i]:
                        preview_lines.append(f"Line {i+1}: {lines[i]} → {line}")
                        if len(preview_lines) >= 3:  # Show max 3 preview lines
                            break

                if preview_lines:
                    print("  Preview of changes:")
                    for preview in preview_lines:
                        try:
                            print(f"    {preview}")
                        except UnicodeEncodeError:
                            # If we can't print the preview due to encoding issues, skip it
                            print("    [Preview contains Unicode characters that cannot be displayed]")
        else:
            print(f"SUCCESS: {file_path}: No emojis found in print statements")

        return changes_made, lines_changed

    except Exception as e:
        try:
            print(f"ERROR: Error processing {file_path}: {str(e)}")
        except UnicodeEncodeError:
            print(f"ERROR: Error processing {file_path}: [Unicode encoding error in error message]")
        return 0, 0

def main():
    """Main function to process all Python files"""
    if len(sys.argv) > 1 and sys.argv[1] == '--apply':
        dry_run = False
        print("APPLYING CHANGES - Emojis will be removed from print statements")
    else:
        dry_run = True
        print("DRY RUN - Showing what would be changed (use --apply to actually modify files)")

    print("=" * 80)

    # Find all Python files
    project_root = os.path.dirname(os.path.abspath(__file__))
    python_files = find_python_files(project_root)

    print(f"Found {len(python_files)} Python files to process")

    total_changes = 0
    total_lines = 0
    files_with_changes = 0

    for file_path in python_files:
        changes, lines_changed = process_file(file_path, dry_run)
        if changes > 0:
            files_with_changes += 1
            total_changes += changes
            total_lines += lines_changed

    print("=" * 80)
    print("SUMMARY:")
    print(f"  Files processed: {len(python_files)}")
    print(f"  Files with emoji prints: {files_with_changes}")
    print(f"  Total emoji prints cleaned: {total_changes}")
    print(f"  Total lines modified: {total_lines}")

    if dry_run:
        print("\nTo actually apply these changes, run: python remove_emojis_from_prints.py --apply")
        print("   (This will create .bak backup files for all modified files)")
    else:
        print("\nChanges applied successfully!")
        print("   Backup files (.bak) have been created for all modified files")

if __name__ == "__main__":
    main()
