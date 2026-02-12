# Phase 11: Audit Report - PostgreSQL C Source Alignment

## Overview
This document tracks the audit of Postrust implementation against PostgreSQL C source code to identify divergences and ensure compatibility.

## Module 1: Expression Evaluation

### Files Compared
- **Rust**: `src/executor/exec_expr.rs` (2,525 lines)
- **PostgreSQL**: `src/backend/executor/execExprInterp.c` (5,920 lines), `execExpr.c` (5,066 lines)

### Architecture Comparison

#### PostgreSQL Approach
- **Bytecode interpreter**: Pre-compiles expressions into linear `ExprEvalStep` array
- **Computed gotos**: Direct-threaded dispatch for performance (when available)
- **Fast paths**: Specialized `ExecJust*` functions bypass main loop for simple expressions
- **Opcode specialization**: Multiple variants (e.g., `FUNCEXPR_STRICT_1`, `FUNCEXPR_STRICT_2`) to optimize hot paths
- **JIT-friendly**: Helper functions enable JIT compilation

#### Postrust Approach
- **Recursive evaluator**: Match-based tree walking with async support
- **Switch dispatch**: Traditional Rust match statements
- **Uniform handling**: Single code path for each expression type
- **Async-first**: Boxed futures for subquery evaluation

### Divergences Found

#### ✅ CORRECT: Integer Arithmetic
- [x] Division by zero handling matches PG
- [x] INT_MIN / -1 overflow detection matches PG (lines 98-102, 173-177, 235-239 in int_arithmetic.rs)
- [x] All checked arithmetic functions use PG-compatible error messages

#### ✅ FIXED: LIKE ESCAPE Clause
**Impact**: Medium - affects LIKE pattern matching with custom escape characters

**PostgreSQL**:
```sql
SELECT 'abc%def' LIKE 'abc!%def' ESCAPE '!';  -- true
```

**Previous State**:
- AST `Expr::Like` had no `escape` field
- Parser `parse_like_expr()` didn't check for ESCAPE keyword
- Evaluator `eval_like_predicate()` hardcoded backslash escape

**Fixed** (commit b525fdd):
1. Added `escape: Option<Box<Expr>>` to `Expr::Like` in ast.rs
2. Added `Escape` keyword to lexer
3. Modified `parse_like_expr()` to check for ESCAPE keyword after pattern
4. Updated `eval_like_predicate()` to accept and validate custom escape character
5. Updated `like_match_recursive()` to use custom escape parameter (default: backslash)
6. Added comprehensive tests for LIKE ESCAPE functionality

**Test Results**: ✅ All tests pass

#### ✅ FIXED: chr() Function Unicode Support
**Impact**: Medium - affects character generation beyond ASCII range

**PostgreSQL** (`src/backend/utils/adt/varlena.c:chr()`):
- Supports full Unicode range (0 to U+10FFFF)
- Handles multibyte encodings
- Returns proper character encoding per database locale

**Previous State** (`src/utils/adt/string_functions.rs:chr_from_code()`):
- Limited to 0-255 range
- Returned ASCII/Latin-1 only

**Fixed** (commit 7753e1f):
1. Removed 0-255 limitation in `chr_from_code()`
2. Now supports full Unicode range (0 to U+10FFFF)
3. Proper error handling for negative and invalid code points
4. Added tests for:
   - ASCII characters (chr(65) = 'A')
   - Control characters (chr(10) = '\n')
   - Unicode beyond ASCII (chr(8364) = '€')
   - Emojis (chr(128512) = '😀')
   - Invalid inputs (negative numbers, out-of-range values)

**Test Results**: ✅ All tests pass

#### ❌ MISSING: LIKE ESCAPE Clause
**Impact**: Medium - affects LIKE pattern matching with custom escape characters

**PostgreSQL**:
```sql
SELECT 'abc%def' LIKE 'abc!%def' ESCAPE '!';  -- true
```

**Current State**:
- AST `Expr::Like` has no `escape` field
- Parser `parse_like_expr()` doesn't check for ESCAPE keyword
- Evaluator `eval_like_predicate()` hardcodes backslash escape

**Fix Required**:
1. Add `escape: Option<Box<Expr>>` to `Expr::Like` in ast.rs
2. Modify `parse_like_expr()` to check for ESCAPE keyword
3. Update `eval_like_predicate()` to use custom escape character
4. Update `like_match_recursive()` to accept escape parameter

**Test Cases Needed**:
```sql
SELECT 'abc%' LIKE 'abc!%' ESCAPE '!';
SELECT 'abc\def' LIKE 'abc\\def' ESCAPE '\';
```

#### ⚠️ SIMPLIFIED: LIKE Pattern Matching
**Impact**: Low-Medium - affects multibyte characters and locale-specific behavior

**PostgreSQL**:
- Separate implementations: `SB_MatchText` (single-byte), `MB_MatchText` (multibyte), `UTF8_MatchText`
- Locale-aware case folding for ILIKE
- Multibyte character support via `pg_mblen_with_len()`

**Current State**:
- Single implementation using Rust char iterators
- ASCII-only case folding: `to_ascii_lowercase()` (line 1539-1540)
- No locale support

**Recommendation**: Document limitation; fix only if regression tests fail

#### ⚠️ SIMPLIFIED: chr() Function
**Impact**: Low - was limited to 0-255 but now FIXED

**Status**: ✅ FIXED - Now supports full Unicode range

#### ⚠️ INCOMPLETE: format() Keywords
**Impact**: Low - affects format() identifier quoting

**PostgreSQL** (`src/backend/utils/adt/varlena.c`):
- Validates ~150+ reserved keywords for %I (identifier) format
- Uses keyword lookup table

**Current State** (`src/utils/adt/string_functions.rs:is_keyword()`):
- Only 24 keywords checked (lines 527-534)

**Recommendation**: Generate keyword list from parser's keyword enum

### NULL Handling - ✅ CORRECT
Both implementations handle NULL correctly:
- Binary operators return NULL if either operand is NULL
- Logical AND/OR implement 3-valued logic
- Comparison operators return NULL for NULL operands

### Error Messages - ✅ MOSTLY ALIGNED
Sample comparison:
| Operation | PostgreSQL | Postrust | Status |
|-----------|-----------|----------|--------|
| Division by zero | "division by zero" | "division by zero" | ✅ Match |
| INT overflow | "integer out of range" | "integer out of range" | ✅ Match |
| Unknown column | "column \"%s\" does not exist" | "unknown column \"%s\"" | ⚠️ Different wording |
| Ambiguous column | "column reference \"%s\" is ambiguous" | "column reference \"%s\" is ambiguous" | ✅ Match |

**Recommendation**: Standardize "unknown column" → "column does not exist" for consistency

---

## Module 2: String Functions

### Files Compared
- **Rust**: `src/utils/adt/string_functions.rs` (556 lines)
- **PostgreSQL**: `src/backend/utils/adt/varlena.c` (5,886 lines), `oracle_compat.c` (1,157 lines)

### Functions Implemented
| Function | Status | Notes |
|----------|--------|-------|
| initcap | ✅ | Basic implementation |
| lpad/rpad | ✅ | Via pad_string() |
| substring | ✅ | Character-based |
| overlay | ✅ | Basic implementation |
| trim | ✅ | Supports custom char set |
| left/right | ✅ | Character-based |
| md5 | ✅ | Custom implementation |
| sha256 | ✅ | Via sha2 crate |
| encode/decode | ✅ | hex, base64, escape |
| ascii/chr | ⚠️ | chr limited to 0-255 |
| format | ⚠️ | Limited keyword check |
| position | ✅ | Via find_substring_position |

### Missing PostgreSQL Functions
- `length()` vs `octet_length()` distinction
- `bit_length()`, `char_length()`
- `convert()`, `convert_from()`, `convert_to()` (encoding conversion)
- `quote_ident()`, `quote_literal()`, `quote_nullable()` (SQL quoting)
- `regexp_*` functions (regexp_matches, regexp_replace, etc.)
- `split_part()`, `string_to_array()`, `array_to_string()`
- `translate()` (character translation)

**Recommendation**: Add based on regression test failures

---

## Module 3: JSON/JSONB Operations - TODO

### Files to Compare
- **Rust**: `src/utils/adt/json.rs`
- **PostgreSQL**: `src/backend/utils/adt/json.c`, `jsonb.c`, `jsonfuncs.c`

---

## Module 4: Aggregate Execution - TODO

### Files to Compare
- **Rust**: `src/executor/node_agg.rs`
- **PostgreSQL**: `src/backend/executor/nodeAgg.c`

---

## Module 5: Date/Time/Interval - TODO

### Files to Compare
- **Rust**: `src/utils/adt/datetime.rs`
- **PostgreSQL**: `src/backend/utils/adt/date.c`, `timestamp.c`, `datetime.c`, `formatting.c`

---

## Module 6: Type Coercion/Casting - TODO

### Files to Compare
- **Rust**: Inline coercion in exec_expr.rs
- **PostgreSQL**: `src/backend/parser/parse_coerce.c`, type I/O functions

---

## Module 7: Array Operations - TODO

### Files to Compare
- **Rust**: TBD
- **PostgreSQL**: `src/backend/utils/adt/arrayfuncs.c`, `array_userfuncs.c`

---

## Module 8: Math Functions - TODO

### Files to Compare
- **Rust**: `src/utils/adt/math_functions.rs`
- **PostgreSQL**: `src/backend/utils/adt/float.c`, `numeric.c`

---

## Module 9: Subquery Execution - TODO

### Files to Compare
- **Rust**: `src/executor/node_subquery.rs`
- **PostgreSQL**: `src/backend/executor/nodeSubplan.c`

---

## Module 10: Sort/Comparison - TODO

### Files to Compare
- **Rust**: TBD
- **PostgreSQL**: `src/backend/utils/sort/tuplesort.c`

---

## Priority Fixes

### Completed ✅
1. **LIKE ESCAPE clause** ✅ - Implemented with full test coverage
2. **chr() Unicode support** ✅ - Extended to support full Unicode range

### High Priority (Block Regression Tests)
### Medium Priority (Improve Compatibility)
3. **format() keyword list** - Complete keyword validation (24 vs ~150+ in PG)
### Low Priority (Nice to Have)
4. **Standardize error messages** - "unknown column" → "column does not exist"
4. **Standardize error messages** - "unknown column" → "column does not exist"
5. **LIKE multibyte/locale support** - Only if regression tests fail

## Testing Strategy

1. **Unit tests**: Add tests for each fix in isolation
2. **Regression tests**: Run `tests/regression/pg_compat/sql/strings.sql`
3. **Differential testing**: Compare output with real PostgreSQL

---

## Progress Tracking

- [x] Module 1: Expression Evaluation - Initial audit complete, 2 fixes implemented
- [x] Module 2: String Functions - Initial audit complete, 1 fix implemented
- [ ] Module 3: JSON/JSONB - Not started
- [ ] Module 4: Aggregates - Not started
- [ ] Module 5: Date/Time - Not started
- [ ] Module 6: Type Coercion - Not started
- [ ] Module 7: Arrays - Not started
- [ ] Module 8: Math - Not started
- [ ] Module 9: Subqueries - Not started
- [ ] Module 10: Sort - Not started

---

## Next Steps

1. Implement LIKE ESCAPE support
2. Fix chr() to support full Unicode
3. Continue audit of remaining modules
4. Run regression tests to validate fixes
