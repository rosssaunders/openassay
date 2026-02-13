#!/usr/bin/env python3

import subprocess
import time
import signal
import os
import sys
import json
from pathlib import Path
import re

class PostgresRegressionRunner:
    def __init__(self, project_root):
        self.project_root = Path(project_root)
        self.test_dir = self.project_root / "tests" / "regression" / "pg_compat"
        self.server_proc = None
        self.results = {}
        self.total_statements = 0
        self.passed_statements = 0
        
        # List of test files in the order we want to run them
        self.test_files = [
            'select.sql', 'select_distinct.sql', 'select_having.sql', 'select_implicit.sql',
            'join.sql', 'subselect.sql', 'aggregates.sql', 'window.sql', 'union.sql',
            'case.sql', 'with.sql', 'boolean.sql', 'strings.sql', 'text.sql',
            'int2.sql', 'int4.sql', 'int8.sql', 'float4.sql', 'float8.sql', 'numeric.sql',
            'json.sql', 'jsonb.sql', 'insert.sql', 'update.sql', 'delete.sql',
            'insert_conflict.sql', 'create_table.sql', 'create_view.sql', 'create_index.sql',
            'sequence.sql', 'groupingsets.sql', 'merge.sql', 'explain.sql',
            'date.sql', 'time.sql', 'timestamp.sql', 'interval.sql', 'arrays.sql', 'matview.sql'
        ]
    
    def count_sql_statements(self, sql_content):
        """Count SQL statements in content (rough heuristic)"""
        # Remove comments and count semicolons not in string literals
        lines = sql_content.split('\n')
        cleaned_lines = []
        
        for line in lines:
            # Remove SQL comments
            line = re.sub(r'--.*$', '', line)
            cleaned_lines.append(line)
        
        cleaned_content = '\n'.join(cleaned_lines)
        # Count semicolons (rough estimate)
        statements = len([s for s in cleaned_content.split(';') if s.strip()])
        return statements

    def start_server(self):
        """Start pg_server in background"""
        print("Starting pg_server...")
        pg_server_path = self.project_root / "target" / "release" / "pg_server"
        
        if not pg_server_path.exists():
            raise RuntimeError(f"pg_server not found at {pg_server_path}. Run 'cargo build --release --bin pg_server'")
        
        # Start server on default port 55432
        self.server_proc = subprocess.Popen(
            [str(pg_server_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=str(self.project_root)
        )
        
        # Give server time to start
        time.sleep(2)
        
        # Check if server started successfully
        if self.server_proc.poll() is not None:
            stdout, stderr = self.server_proc.communicate()
            raise RuntimeError(f"pg_server failed to start: {stderr.decode()}")
        
        print("pg_server started successfully")
    
    def stop_server(self):
        """Stop pg_server"""
        if self.server_proc:
            print("Stopping pg_server...")
            self.server_proc.terminate()
            try:
                self.server_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.server_proc.kill()
                self.server_proc.wait()
            self.server_proc = None
    
    def run_sql_file(self, sql_file, description=""):
        """Run a SQL file using psql and return success/failure counts"""
        sql_path = self.test_dir / "sql" / sql_file
        
        if not sql_path.exists():
            print(f"SQL file not found: {sql_path}")
            return 0, 0, f"File not found: {sql_file}"
        
        # Read the SQL content to count statements
        with open(sql_path, 'r') as f:
            sql_content = f.read()
        
        total_statements = self.count_sql_statements(sql_content)
        
        print(f"Running {sql_file} ({description})...")
        
        psql_cmd = [
            "/home/linuxbrew/.linuxbrew/opt/libpq/bin/psql",
            "-h", "localhost",
            "-p", "55432",
            "-U", "postgres",
            "-d", "postgres",
            "-f", str(sql_path),
            "--set", "ON_ERROR_STOP=off",  # Continue on errors
            "--quiet"  # Reduce noise
        ]
        
        # Set environment variables
        env = os.environ.copy()
        env["PGPASSWORD"] = ""  # No password
        
        try:
            result = subprocess.run(
                psql_cmd,
                capture_output=True,
                text=True,
                env=env,
                timeout=300  # 60 second timeout per file
            )
            
            # Analyze output to count errors
            stderr_output = result.stderr
            stdout_output = result.stdout
            
            # Count error lines (lines starting with ERROR:)
            error_lines = [line for line in stderr_output.split('\n') if line.strip().startswith('ERROR:')]
            error_count = len(error_lines)
            
            passed_statements = max(0, total_statements - error_count)
            
            output_summary = f"Return code: {result.returncode}\n"
            output_summary += f"STDOUT ({len(stdout_output)} chars):\n{stdout_output[:500]}{'...' if len(stdout_output) > 500 else ''}\n"
            output_summary += f"STDERR ({len(stderr_output)} chars):\n{stderr_output[:1000]}{'...' if len(stderr_output) > 1000 else ''}"
            
            return passed_statements, total_statements, output_summary
            
        except subprocess.TimeoutExpired:
            return 0, total_statements, "TIMEOUT: Test took longer than 60 seconds"
        except Exception as e:
            return 0, total_statements, f"EXCEPTION: {str(e)}"
    
    def run_setup(self):
        """Run the test setup SQL"""
        print("\n=== RUNNING TEST SETUP ===")
        
        # First modify test_setup.sql to work with our environment
        setup_path = self.test_dir / "sql" / "test_setup.sql"
        data_path = self.test_dir / "data"
        
        with open(setup_path, 'r') as f:
            setup_content = f.read()
        
        # Replace the postgres-specific paths with our data path
        # Replace \getenv and \set commands with direct paths
        modified_setup = setup_content.replace(
            r"\set filename :abs_srcdir '/data/", 
            f"\\set filename '{data_path}/"
        )
        
        # Remove the dynamic variable setting lines
        lines = modified_setup.split('\n')
        cleaned_lines = []
        for line in lines:
            if not (line.strip().startswith('\\getenv') or 
                   line.strip().startswith('\\set regresslib') or
                   line.strip().startswith('AS :regresslib')):
                cleaned_lines.append(line)
        
        modified_setup = '\n'.join(cleaned_lines)
        
        # Write modified setup to temp file
        temp_setup = self.test_dir / "sql" / "test_setup_modified.sql"
        with open(temp_setup, 'w') as f:
            f.write(modified_setup)
        
        passed, total, output = self.run_sql_file("test_setup_modified.sql", "test fixture setup")
        
        self.results['setup'] = {
            'passed': passed,
            'total': total,
            'output': output
        }
        
        print(f"Setup: {passed}/{total} statements passed")
        return passed, total
    
    def run_tests(self):
        """Run all test files"""
        print("\n=== RUNNING REGRESSION TESTS ===")
        
        for test_file in self.test_files:
            passed, total, output = self.run_sql_file(test_file, f"PostgreSQL regression test")
            
            self.results[test_file] = {
                'passed': passed,
                'total': total,
                'output': output
            }
            
            self.total_statements += total
            self.passed_statements += passed
            
            print(f"{test_file}: {passed}/{total} statements passed ({100*passed/total if total > 0 else 0:.1f}%)")
    
    def generate_report(self):
        """Generate detailed report"""
        results_file = self.test_dir / "results" / "pg18_compatibility_results.json"
        results_file.parent.mkdir(exist_ok=True)
        
        summary = {
            'overall_score': {
                'passed': self.passed_statements,
                'total': self.total_statements,
                'percentage': 100 * self.passed_statements / self.total_statements if self.total_statements > 0 else 0
            },
            'per_file_results': {},
            'detailed_output': self.results
        }
        
        for test_file in ['setup'] + self.test_files:
            if test_file in self.results:
                result = self.results[test_file]
                summary['per_file_results'][test_file] = {
                    'passed': result['passed'],
                    'total': result['total'],
                    'percentage': 100 * result['passed'] / result['total'] if result['total'] > 0 else 0
                }
        
        with open(results_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        # Also create a human-readable summary
        summary_file = self.test_dir / "results" / "compatibility_summary.txt"
        with open(summary_file, 'w') as f:
            f.write("PostgreSQL 18 Compatibility Test Results\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Overall Score: {self.passed_statements}/{self.total_statements} statements passed ")
            f.write(f"({summary['overall_score']['percentage']:.1f}%)\n\n")
            
            f.write("Per-file Results:\n")
            f.write("-" * 20 + "\n")
            
            for test_file, result in summary['per_file_results'].items():
                f.write(f"{test_file:<25} {result['passed']:>4}/{result['total']:<4} ({result['percentage']:>5.1f}%)\n")
        
        print(f"\nResults saved to:")
        print(f"  - {results_file}")
        print(f"  - {summary_file}")
        
        return summary
    
    def run(self):
        """Run the complete test suite"""
        try:
            self.start_server()
            
            # Run setup first
            self.run_setup()
            
            # Run all tests
            self.run_tests()
            
            # Generate report
            summary = self.generate_report()
            
            print(f"\n" + "="*60)
            print(f"FINAL RESULTS:")
            print(f"Overall Score: {self.passed_statements}/{self.total_statements} statements passed ({summary['overall_score']['percentage']:.1f}%)")
            print(f"="*60)
            
        finally:
            self.stop_server()

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 run_tests.py <project_root>")
        sys.exit(1)
    
    project_root = sys.argv[1]
    runner = PostgresRegressionRunner(project_root)
    runner.run()

if __name__ == "__main__":
    main()