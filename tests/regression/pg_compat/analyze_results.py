#!/usr/bin/env python3

import json
import re
from pathlib import Path

def extract_errors_from_output(output):
    """Extract actual errors from psql output"""
    if 'STDERR' not in output:
        return [], 0
    
    # Find the stderr section
    stderr_match = re.search(r'STDERR \(\d+ chars\):\n(.+?)(?:\nSTDOUT|\Z)', output, re.DOTALL)
    if not stderr_match:
        return [], 0
    
    stderr_content = stderr_match.group(1)
    
    # Find all ERROR lines
    error_lines = []
    for line in stderr_content.split('\n'):
        if 'ERROR:' in line:
            error_lines.append(line.strip())
    
    return error_lines, len(error_lines)

def categorize_error(error_line):
    """Categorize an error into parser gaps, missing features, etc."""
    if 'parse error:' in error_line:
        if 'unexpected token' in error_line or 'expected' in error_line:
            return 'parser_syntax'
        elif 'unsupported type' in error_line:
            return 'unsupported_type'
        else:
            return 'parser_other'
    elif 'does not exist' in error_line:
        return 'missing_relation'
    elif 'unknown column' in error_line:
        return 'column_resolution'
    elif 'ambiguous' in error_line:
        return 'column_ambiguity'
    elif 'division by zero' in error_line:
        return 'runtime_error'
    elif 'COPY FROM requires STDIN' in error_line:
        return 'copy_limitation'
    elif 'operator does not exist' in error_line:
        return 'missing_operator'
    elif 'function' in error_line and ('does not exist' in error_line or 'unknown' in error_line):
        return 'missing_function'
    else:
        return 'other'

def analyze_compatibility():
    results_file = Path("~/code/rosssaunders/postrust/tests/regression/pg_compat/results/pg18_compatibility_results.json").expanduser()
    
    with open(results_file, 'r') as f:
        data = json.load(f)
    
    print("PostgreSQL 18 Compatibility Analysis")
    print("="*50)
    
    # Analyze each test file
    error_categories = {}
    total_errors = 0
    files_with_errors = 0
    
    test_results = []
    
    for test_name, test_data in data['detailed_output'].items():
        if test_name == 'setup':
            continue
            
        errors, error_count = extract_errors_from_output(test_data['output'])
        
        if error_count > 0:
            files_with_errors += 1
            total_errors += error_count
            
            # Categorize errors
            for error in errors:
                category = categorize_error(error)
                if category not in error_categories:
                    error_categories[category] = []
                error_categories[category].append((test_name, error))
        
        # Calculate real pass rate (very rough estimate)
        estimated_statements = test_data['total']
        failed_statements = min(error_count, estimated_statements)  # Cap at total statements
        passed_statements = estimated_statements - failed_statements
        pass_rate = (passed_statements / estimated_statements * 100) if estimated_statements > 0 else 0
        
        test_results.append({
            'name': test_name,
            'total_statements': estimated_statements,
            'errors': error_count,
            'estimated_passed': passed_statements,
            'pass_rate': pass_rate
        })
    
    # Sort by pass rate
    test_results.sort(key=lambda x: x['pass_rate'])
    
    print(f"\nOverall Statistics:")
    print(f"- Total test files: {len(test_results)}")
    print(f"- Files with errors: {files_with_errors}")
    print(f"- Total errors found: {total_errors}")
    
    print(f"\nWorst Performing Tests (by error count):")
    print("-" * 40)
    worst_tests = sorted(test_results, key=lambda x: x['errors'], reverse=True)[:10]
    for test in worst_tests:
        print(f"{test['name']:<25} {test['errors']:>4} errors ({test['pass_rate']:>5.1f}% est. pass)")
    
    print(f"\nError Categories (prioritized by frequency):")
    print("-" * 50)
    
    category_counts = {cat: len(errors) for cat, errors in error_categories.items()}
    sorted_categories = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)
    
    category_descriptions = {
        'parser_syntax': 'Parser gaps - syntax not supported',
        'missing_relation': 'Missing test fixture tables (setup issues)',
        'unsupported_type': 'Unsupported data types',
        'column_resolution': 'Column resolution issues',
        'column_ambiguity': 'Ambiguous column references',
        'missing_function': 'Missing functions/procedures',
        'missing_operator': 'Missing operators',
        'copy_limitation': 'COPY command limitations',
        'runtime_error': 'Runtime execution errors',
        'parser_other': 'Other parser issues',
        'other': 'Miscellaneous errors'
    }
    
    total_categorized_errors = sum(category_counts.values())
    
    for category, count in sorted_categories:
        description = category_descriptions.get(category, category)
        percentage = (count / total_categorized_errors * 100) if total_categorized_errors > 0 else 0
        print(f"{description:<35} {count:>4} errors ({percentage:>5.1f}%)")
    
    print(f"\nTop Parser Gaps to Fix (by impact):")
    print("-" * 40)
    
    # Focus on parser syntax errors as these block the most functionality
    if 'parser_syntax' in error_categories:
        syntax_patterns = {}
        for test_name, error in error_categories['parser_syntax']:
            # Extract the specific syntax issue
            if 'expected' in error and 'at byte' in error:
                pattern_match = re.search(r'expected ([^"]+)(?: after ([^"]+))? at byte', error)
                if pattern_match:
                    expected = pattern_match.group(1)
                    pattern = f"Missing: {expected}"
                    if pattern not in syntax_patterns:
                        syntax_patterns[pattern] = []
                    syntax_patterns[pattern].append(test_name)
            elif 'unexpected token' in error:
                token_match = re.search(r'unexpected token (.+?) at byte', error)
                if token_match:
                    pattern = f"Unexpected: {token_match.group(1)}"
                    if pattern not in syntax_patterns:
                        syntax_patterns[pattern] = []
                    syntax_patterns[pattern].append(test_name)
        
        sorted_patterns = sorted(syntax_patterns.items(), key=lambda x: len(x[1]), reverse=True)
        for pattern, affected_tests in sorted_patterns[:10]:
            unique_tests = len(set(affected_tests))
            print(f"{pattern:<40} affects {unique_tests:>2} test files")
    
    # Calculate a more realistic overall compatibility score
    total_estimated_statements = sum(t['total_statements'] for t in test_results)
    total_estimated_passed = sum(t['estimated_passed'] for t in test_results)
    realistic_score = (total_estimated_passed / total_estimated_statements * 100) if total_estimated_statements > 0 else 0
    
    print(f"\n" + "="*60)
    print(f"REALISTIC COMPATIBILITY ASSESSMENT")
    print(f"="*60)
    print(f"Estimated overall compatibility: {realistic_score:.1f}%")
    print(f"({total_estimated_passed}/{total_estimated_statements} statements estimated to pass)")
    print(f"")
    print(f"NOTE: This is a rough estimate based on error counting.")
    print(f"Actual compatibility may be higher or lower depending on:")
    print(f"- How many statements are in comments or test setup")
    print(f"- Whether errors cascade (one error causing many failures)")
    print(f"- Missing test fixtures affecting multiple tests")
    
if __name__ == "__main__":
    analyze_compatibility()