"""
FA Rule Converter
=================

Converts actuarial Fine Adjustment rule files (FA_RULE_FINAL) into a flat-table
format (FineAdjustments Data + FineAdjustment Values CSVs), with optional audit
validation and adjacent-bin grouping for interaction terms.

Two modes:
  LTA  – Single FA rule file  → M1/M2 coefficients
  FTA  – Two FA rule files (INC + MAT) → M1/M2 from INC, M3/M4 from MAT

Key concepts:
  - "Single-factor sheet": one variable per sheet (e.g. consideration_band)
  - "Interaction sheet":   two variables (e.g. consideration_band & age_band),
                           stored as a matrix where rows = factor1, cols = factor2
  - "SetNumber":           a unique condition combination; each gets its own
                           coefficient values (M1/M2 for LTA, M1-M4 for FTA)
  - "Bin":                 a range like [0, 10000) or a categorical value like 3.0

Workflow (three-stage pipeline):
  1. Convert  → reads Key mapping + FA source(s), emits Data + Values CSVs
  2. Audit    → validates every generated row against the original source
  3. Group    → merges adjacent interaction columns with similar coefficients

Module organisation:
  Constants           – TABLE_VERSION_ID, OPERANDS_INC/MAT, tolerances, etc.
  Result dataclasses  – ConversionResult, AuditResult, GroupingResult
  Data containers     – SingleSheetData, InteractionSheetData, ExtractedData
  Bin / Key parsing   – is_range_bin, get_bin_operators, merge_bin_ranges,
                        read_key_mapping, resolve_sheet_name
  Data extraction     – extract_fa_data (reads Excel once, structures for reuse)
  Row builders        – _make_data_row, _make_value_row, _emit_conditions,
                        _emit_values, _emit_coeff_values
  Shared helpers      – _ordered_union, _all_sheet_names, _classify_set,
                        _build_single_bin_items, _prepare_interaction_context,
                        _lookup_interaction_cell
  Conversion engine   – _convert_singles, _convert_interactions, run_conversion
  Audit engine        – _audit_singles, _audit_interactions, run_audit
  Grouping engine     – _group_row_columns, _group_singles, _group_interactions,
                        run_grouping
  Workflow helpers    – workflow_convert, workflow_convert_audit,
                        workflow_convert_audit_group
"""

from dataclasses import dataclass, field
import os
import re

import pandas as pd


# ============================================================
# Constants
# ============================================================

TABLE_VERSION_ID = 999   # Placeholder version; all rows share this value
FINE_ADJ_BLOCK_KEY = 1   # All sets belong to block 1
DEFAULT_PRIORITY = 0     # Priority column; 0 = no special priority
DEFAULT_THRESHOLD = 0.01 # Default grouping threshold (max coeff diff to merge)

# Operand names used in output CSVs.
OPERANDS_INC = ['M1', 'M2']   # INC file → M1/M2 (used by both LTA and FTA)
OPERANDS_MAT = ['M3', 'M4']   # MAT file → M3/M4 (FTA only)

# Tolerance for floating-point coefficient comparisons in the audit.
# Tighter for individual values, looser for sums over many values.
_COEFF_TOL = 1e-10
_SUM_TOL_TIGHT = 1e-8
_SUM_TOL_LOOSE = 1e-6

# Decimal places used when rounding averaged grouping coefficients.
_COEFF_ROUND_DIGITS = 10

# Column indices in single-factor sheets (FA_RULE_FINAL Excel layout):
#   col 0 = index, col 1 = factor_name, col 2 = coefficient,
#   col 3 = bins (range string), col 4 = bin_pred (same as coefficient)
_SF_COL_BINS = 3
_SF_COL_COEFF = 4

# Sheet name skipped during extraction (always present but not converted).

# Bracket characters that appear in the Key file as operators rather than sheet names.
_BRACKET_SYMBOLS = {'[', ']', '(', ')'}


# ============================================================
# Result dataclasses  (replace raw tuples)
# ============================================================

@dataclass
class ConversionResult:
    """Returned by run_conversion (both LTA and FTA)."""
    data_csv_path: str
    values_csv_path: str
    total_sets: int
    data_row_count: int
    value_row_count: int
    sheets_processed: int
    sheet_summary: str
    skip_summary: str
    # FTA-only fields (None for LTA)
    diff_csv_path: str = None
    combined_sets: int = 0
    inc_only_sets: int = 0
    mat_only_sets: int = 0
    diff_count: int = 0


@dataclass
class AuditResult:
    """Returned by run_audit (both LTA and FTA)."""
    total_sheets: int
    sheets_pass: int
    detail_total: int
    detail_pass: int
    detail_csv_path: str
    summary_csv_path: str
    # LTA uses sum_pass; FTA uses inc_sum_pass / mat_sum_pass
    sum_pass: int = 0
    inc_sum_pass: int = 0
    mat_sum_pass: int = 0


@dataclass
class GroupingResult:
    """Returned by run_grouping (both LTA and FTA)."""
    data_csv_path: str
    values_csv_path: str
    summary_csv_path: str
    total_sets: int
    data_row_count: int
    value_row_count: int
    original_cells: int
    grouped_cells: int
    reduction_pct: float
    sheets_grouped: int


# ============================================================
# Structured data containers (internal)
# ============================================================

@dataclass
class SingleSheetData:
    """Extracted data from one single-factor sheet."""
    did: str                          # DataItemId (e.g. TOTAL_CONSID)
    bins: list                        # [(bin_str, coefficient), ...]


@dataclass
class InteractionSheetData:
    """Extracted data from one interaction sheet."""
    did1: str                         # DataItemId for row factor
    did2: str                         # DataItemId for column factor
    row_bins: list                    # [bin_str, ...] in original order
    col_bins: list                    # [bin_str, ...] in original order
    cells: dict                       # {(row_bin, col_bin): coefficient}
    grid: list = field(default_factory=list)  # [[coeff_or_None, ...], ...] positional 2D


@dataclass
class ExtractedData:
    """All data extracted from one FA rule file."""
    singles: dict = field(default_factory=dict)      # {sheet_name: SingleSheetData}
    interactions: dict = field(default_factory=dict)  # {sheet_name: InteractionSheetData}
    skipped: list = field(default_factory=list)       # [sheet_name, ...]


# ============================================================
# Bin / Key parsing utilities
# ============================================================

def is_range_bin(bin_str):
    """Check if a bin string is a range (has brackets) vs categorical (plain value).

    Examples:
        '[0.0, 10000.0)' → True   (range bin)
        '3.0'            → False  (categorical)
    """
    return bool(re.match(r'[\[\(]', str(bin_str).strip()))


def get_bin_operators(bin_str, bracket_map):
    """Convert a bin string into a list of (operator, value) condition pairs.

    Uses bracket_map to translate bracket characters into comparison operators:
        { '[' : '>=', ')' : '<', '(' : '>', ']' : '<=' }

    Examples:
        '[0.0, 10000.0)' → [('>=', 0.0), ('<', 10000.0)]
        '[1400000.0, inf)' → [('>=', 1400000.0)]   # inf → no upper bound
        '3.0'             → [('=', 3.0)]             # categorical
    """
    bin_str = str(bin_str).strip()
    if is_range_bin(bin_str):
        left_bracket = bin_str[0]
        right_bracket = bin_str[-1]
        inner = bin_str[1:-1]
        parts = [p.strip() for p in inner.split(',')]

        left_op = bracket_map.get(left_bracket, '>=')
        conditions = [(left_op, float(parts[0]))]

        if parts[1].lower() != 'inf':
            right_op = bracket_map.get(right_bracket, '<')
            conditions.append((right_op, float(parts[1])))
        return conditions
    else:
        # Categorical value: single equality condition
        try:
            return [('=', float(bin_str))]
        except ValueError:
            return [('=', bin_str)]


def merge_bin_ranges(bin_list, bracket_map):
    """Merge a list of adjacent range-bin strings into one wider range.

    Takes the lower bound from the first bin and the upper bound from the
    last bin, producing a single condition pair that spans the entire range.

    Returns None if any bin is categorical (can't merge non-ranges).

    Example:
        ['[0, 10000)', '[10000, 20000)', '[20000, 30000)']
        → [('>=', 0.0), ('<', 30000.0)]
    """
    if len(bin_list) == 1:
        return get_bin_operators(bin_list[0], bracket_map)
    if not all(is_range_bin(b) for b in bin_list):
        return None

    first = str(bin_list[0]).strip()
    last = str(bin_list[-1]).strip()

    # Lower bound from first bin
    left_op = bracket_map.get(first[0], '>=')
    lo = first[1:-1].split(',')[0].strip()
    conditions = [(left_op, float(lo))]

    # Upper bound from last bin (skip if infinity)
    hi_str = last[1:-1].split(',')[1].strip()
    if hi_str.lower() != 'inf':
        right_op = bracket_map.get(last[-1], '<')
        conditions.append((right_op, float(hi_str)))

    return conditions


def read_key_mapping(key_file):
    """Read the Key sheet (first sheet in key_file) to build two mappings.

    The Key sheet has rows where:
      - Column B = DataItemId (e.g. TOTAL_CONSID) or bracket symbol ([, ], (, ))
      - Column D = Sheet name  (e.g. consideration_band) or operator (>=, <, etc.)

    Returns:
        sheet_to_did:  {'consideration_band': 'TOTAL_CONSID', ...}
        bracket_map:   {'[': '>=', ')': '<', ...}
    """
    df = pd.read_excel(key_file, sheet_name=0, header=None)

    sheet_to_did = {}
    bracket_map = {}

    for idx in range(1, len(df)):
        col_b = df.iloc[idx, 1]
        col_d = df.iloc[idx, 3]
        if pd.isna(col_b) or pd.isna(col_d):
            continue
        col_b = str(col_b).strip()
        col_d = str(col_d).strip()

        if col_b in _BRACKET_SYMBOLS:
            bracket_map[col_b] = col_d
        else:
            sheet_to_did[col_d] = col_b   # sheet name → DataItemId

    return sheet_to_did, bracket_map


def resolve_sheet_name(factor_name, sheet_to_did, fa_sheet_names):
    """Resolve a (possibly truncated) factor name to its DataItemId.

    Excel truncates sheet names to 31 characters, so 'consideration_band & age_band'
    might appear as 'consideration_band & age_ba'. This function tries:
      1. Exact match in sheet_to_did
      2. Prefix match against FA file sheet names
      3. Prefix match against Key mapping sheet names

    Returns (resolved_sheet_name, data_item_id) or (None, None).
    """
    if factor_name in sheet_to_did:
        return factor_name, sheet_to_did[factor_name]
    for fa_sn in fa_sheet_names:
        if fa_sn.startswith(factor_name) and fa_sn in sheet_to_did:
            return fa_sn, sheet_to_did[fa_sn]
    for key_sn, did in sheet_to_did.items():
        if key_sn.startswith(factor_name):
            return key_sn, did
    return None, None


# ============================================================
# Data extraction  (reads Excel once, structures for reuse)
# ============================================================

def extract_fa_data(fa_file, sheet_to_did, bracket_map):
    """Read an FA rule file and extract all single-factor and interaction data.

    This is called once per file; the returned ExtractedData is then reused by
    conversion, audit, and grouping without re-reading the Excel file.

    Returns an ExtractedData containing:
      - singles:      {sheet_name: SingleSheetData}
      - interactions:  {sheet_name: InteractionSheetData}
      - skipped:       [sheet names without a Key mapping]
    """
    fa_xls = pd.ExcelFile(fa_file)
    result = ExtractedData()

    for sheet in fa_xls.sheet_names:
        if ' & ' not in sheet:
            # --- Single-factor sheet ---
            _, did = resolve_sheet_name(sheet, sheet_to_did, fa_xls.sheet_names)
            if not did:
                result.skipped.append(sheet)
                continue
            df = pd.read_excel(fa_file, sheet_name=sheet, header=None)
            bins_raw = df.iloc[1:, _SF_COL_BINS].dropna().tolist()
            vals_raw = df.iloc[1:, _SF_COL_COEFF].dropna().tolist()
            entries = [(str(b).strip(), float(v)) for b, v in zip(bins_raw, vals_raw)]
            result.singles[sheet] = SingleSheetData(did=did, bins=entries)
        else:
            # --- Interaction sheet ---
            parts = sheet.split(' & ')
            _, did1 = resolve_sheet_name(parts[0].strip(), sheet_to_did, fa_xls.sheet_names)
            _, did2 = resolve_sheet_name(parts[1].strip(), sheet_to_did, fa_xls.sheet_names)
            if not did1 or not did2:
                result.skipped.append(sheet)
                continue
            df = pd.read_excel(fa_file, sheet_name=sheet, header=None)
            col_bins = [str(b).strip() for b in df.iloc[0, 1:].tolist()]
            row_bins = [str(b).strip() for b in df.iloc[1:, 0].tolist()]
            cells = {}
            grid = []
            for r_idx, rb in enumerate(row_bins):
                row = []
                for c_idx, cb in enumerate(col_bins):
                    v = df.iloc[r_idx + 1, c_idx + 1]
                    if not pd.isna(v):
                        fv = float(v)
                        cells[(rb, cb)] = fv
                        row.append(fv)
                    else:
                        row.append(None)
                grid.append(row)
            result.interactions[sheet] = InteractionSheetData(
                did1=did1, did2=did2,
                row_bins=row_bins, col_bins=col_bins, cells=cells, grid=grid)

    return result


# ============================================================
# Row builders  (shared by LTA and FTA)
# ============================================================

def _make_data_row(set_number, did, op, val):
    """Build one FineAdjustments Data row dict."""
    return {
        'TableVersionId': TABLE_VERSION_ID,
        'FineAdjBlockKey': FINE_ADJ_BLOCK_KEY,
        'SetNumber': set_number,
        'Priority': DEFAULT_PRIORITY,
        'DataItemId': did,
        'Operand': op,
        'Value': val,
    }


def _make_value_row(set_number, operand, value):
    """Build one FineAdjustment Values row dict."""
    return {
        'TableVersionId': TABLE_VERSION_ID,
        'FineAdjBlockKey': FINE_ADJ_BLOCK_KEY,
        'SetNumber': set_number,
        'Operand': operand,
        'Value': value,
    }


def _emit_conditions(set_number, did, bin_str, bracket_map):
    """Generate Data rows for one bin's conditions under a given SetNumber.

    Example: bin '[0, 10000)' with did='TOTAL_CONSID' produces two rows:
        (set_number, TOTAL_CONSID, '>=', 0.0)
        (set_number, TOTAL_CONSID, '<', 10000.0)
    """
    return [_make_data_row(set_number, did, op, val)
            for op, val in get_bin_operators(bin_str, bracket_map)]


def _emit_values(set_number, coeff, operands):
    """Generate Value rows for one coefficient under the given operand names.

    Example: coeff=0.5, operands=['M1','M2'] produces two rows:
        (set_number, M1, 0.5)  and  (set_number, M2, 0.5)
    """
    return [_make_value_row(set_number, op, coeff) for op in operands]


# ============================================================
# Ordered union helper
# ============================================================

def _ordered_union(list_a, list_b):
    """Return items from list_a followed by items in list_b not already seen.

    Example:
        _ordered_union(['a', 'b', 'c'], ['b', 'c', 'd']) → ['a', 'b', 'c', 'd']
    """
    seen = set(list_a)
    result = list(list_a)
    for item in list_b:
        if item not in seen:
            result.append(item)
            seen.add(item)
    return result


def _all_sheet_names(extracted, extracted_mat, kind, is_fta):
    """Return the ordered list of sheet names to process for singles or interactions.

    Args:
        extracted:     ExtractedData from INC (or sole LTA) file.
        extracted_mat: ExtractedData from MAT file (None for LTA).
        kind:          'singles' or 'interactions'.
        is_fta:        True for FTA mode.

    In FTA mode the result is the ordered union of INC and MAT sheet names;
    in LTA mode it is just the INC sheet names in original order.
    """
    inc_dict = getattr(extracted, kind)
    if is_fta and extracted_mat is not None:
        mat_dict = getattr(extracted_mat, kind)
        return _ordered_union(list(inc_dict.keys()), list(mat_dict.keys()))
    return list(inc_dict.keys())


def _emit_coeff_values(set_number, inc_coeff, mat_coeff, is_fta, operands):
    """Build Value rows for one set, handling both LTA (INC only) and FTA (INC + MAT).

    Returns a list of value-row dicts ready for CSV output.
    """
    rows = []
    if not is_fta:
        rows.extend(_emit_values(set_number, inc_coeff, operands))
    else:
        if inc_coeff is not None:
            rows.extend(_emit_values(set_number, inc_coeff, OPERANDS_INC))
        if mat_coeff is not None:
            rows.extend(_emit_values(set_number, mat_coeff, OPERANDS_MAT))
    return rows


def _classify_set(inc_coeff, mat_coeff, is_fta):
    """Classify a set as 'Combined', 'INC-only', 'MAT-only', or 'LTA'."""
    if not is_fta:
        return 'LTA'
    has_inc = inc_coeff is not None
    has_mat = mat_coeff is not None
    if has_inc and has_mat:
        return 'Combined'
    return 'INC-only' if has_inc else 'MAT-only'


# ============================================================
# Shared matching helpers  (used by conversion, audit, grouping)
# ============================================================
#
# These three helpers encapsulate the LTA/FTA matching logic that
# would otherwise be copy-pasted in every pipeline stage.  Each
# stage (convert, audit, group) iterates sheets and bins in the
# same order — only the per-row *action* differs.  These helpers
# ensure that order is computed once, consistently.

def _build_single_bin_items(inc_entry, mat_entry, is_fta):
    """Build an ordered list of (bin_str, inc_coeff, mat_coeff) for a single-factor sheet.

    Matching strategy (must be the same everywhere):
      - LTA:  iterate the INC bins list directly (positional, preserves duplicates).
      - FTA with identical labels:  pair INC and MAT by position.
      - FTA with different labels:  dict-based matching (duplicates collapse).

    Returns:  list of (bin_str, inc_coeff_or_None, mat_coeff_or_None)
    """
    ref = inc_entry or mat_entry

    if not is_fta:
        return [(b, c, None) for b, c in ref.bins]

    inc_list = inc_entry.bins if inc_entry else []
    mat_list = mat_entry.bins if mat_entry else []
    inc_labels = [b for b, _ in inc_list]
    mat_labels = [b for b, _ in mat_list]

    if inc_labels == mat_labels:
        # Positional match — handles duplicate bin labels correctly
        return [(b, ic, mc) for (b, ic), (_, mc) in zip(inc_list, mat_list)]
    else:
        # Dict fallback — duplicates collapse to last value
        inc_bins = {b: c for b, c in inc_list}
        mat_bins = {b: c for b, c in mat_list}
        order = _ordered_union(list(inc_bins.keys()), list(mat_bins.keys()))
        return [(b, inc_bins.get(b), mat_bins.get(b)) for b in order]


def _prepare_interaction_context(inc_entry, mat_entry, is_fta):
    """Decide matching strategy and build iteration context for an interaction sheet.

    Returns (fta_positional, row_order, col_order, inc_cells_or_None, mat_cells_or_None).
      - fta_positional=True  → caller should use grid[r][c] positional lookups
      - fta_positional=False → caller should use inc_cells/mat_cells dict lookups

    For LTA, fta_positional is False and the caller uses ref.grid directly.
    """
    ref = inc_entry or mat_entry

    if not is_fta:
        return False, ref.row_bins, ref.col_bins, None, None

    positional = (inc_entry is not None and mat_entry is not None
                  and inc_entry.row_bins == mat_entry.row_bins
                  and inc_entry.col_bins == mat_entry.col_bins)

    if positional:
        return True, inc_entry.row_bins, inc_entry.col_bins, None, None
    else:
        inc_cells = inc_entry.cells if inc_entry else {}
        mat_cells = mat_entry.cells if mat_entry else {}
        row_order = _ordered_union(
            inc_entry.row_bins if inc_entry else [],
            mat_entry.row_bins if mat_entry else [])
        col_order = _ordered_union(
            inc_entry.col_bins if inc_entry else [],
            mat_entry.col_bins if mat_entry else [])
        return False, row_order, col_order, inc_cells, mat_cells


def _lookup_interaction_cell(r_idx, c_idx, rb, cb, inc_entry, mat_entry, ref,
                             is_fta, fta_positional, inc_cells, mat_cells):
    """Get (inc_coeff, mat_coeff) for one interaction cell.

    Returns (None, None) if the cell is empty in both files.

    Uses positional grid access when fta_positional is True (or for LTA),
    dict access when fta_positional is False (bins differ between INC/MAT).
    """
    if is_fta and fta_positional:
        inc_c = inc_entry.grid[r_idx][c_idx]
        mat_c = mat_entry.grid[r_idx][c_idx]
    elif is_fta:
        inc_c = inc_cells.get((rb, cb))
        mat_c = mat_cells.get((rb, cb))
    else:
        inc_c = ref.grid[r_idx][c_idx]
        mat_c = None
    return inc_c, mat_c


# ============================================================
# Core conversion engine
# ============================================================

def _convert_singles(extracted, bracket_map, operands, set_number,
                     extracted_mat=None):
    """Convert all single-factor sheets into Data + Value rows.

    For FTA mode (extracted_mat provided), bins are matched by label:
      - Matching bins → emit both INC and MAT operands
      - INC-only → emit INC operands
      - MAT-only → emit MAT operands

    Returns (data_rows, value_rows, next_set_number, processed_list,
             combined_count, inc_only_count, mat_only_count, diff_rows).
    """
    data_rows = []
    value_rows = []
    processed = []
    diff_rows = []
    counts = {'combined': 0, 'inc_only': 0, 'mat_only': 0}
    is_fta = extracted_mat is not None

    all_sheets = _all_sheet_names(extracted, extracted_mat, 'singles', is_fta)

    for sheet in all_sheets:
        inc_entry = extracted.singles.get(sheet)
        mat_entry = extracted_mat.singles.get(sheet) if is_fta else None
        ref = inc_entry or mat_entry
        did = ref.did

        start_set = set_number
        bin_items = _build_single_bin_items(inc_entry, mat_entry, is_fta)

        # FTA: flag sheet-level and bin-level differences
        if is_fta:
            if inc_entry and not mat_entry:
                diff_rows.append({'Sheet': sheet, 'SheetType': 'Single',
                                  'DifferenceType': 'Sheet in INC only',
                                  'Detail': f"DataItemId={did}"})
            elif mat_entry and not inc_entry:
                diff_rows.append({'Sheet': sheet, 'SheetType': 'Single',
                                  'DifferenceType': 'Sheet in MAT only',
                                  'Detail': f"DataItemId={did}"})
            elif inc_entry and mat_entry:
                inc_set = {b for b, _ in inc_entry.bins}
                mat_set = {b for b, _ in mat_entry.bins}
                for label, diff_bins in [('Bins in INC only', inc_set - mat_set),
                                         ('Bins in MAT only', mat_set - inc_set)]:
                    if diff_bins:
                        diff_rows.append({'Sheet': sheet, 'SheetType': 'Single',
                                          'DifferenceType': label,
                                          'Detail': "; ".join(sorted(diff_bins))})

        # Emit rows for each bin
        for bin_str, inc_coeff, mat_coeff in bin_items:
            data_rows.extend(_emit_conditions(set_number, did, bin_str, bracket_map))
            value_rows.extend(
                _emit_coeff_values(set_number, inc_coeff, mat_coeff, is_fta, operands))
            if is_fta:
                stype = _classify_set(inc_coeff, mat_coeff, is_fta)
                if stype == 'Combined':
                    counts['combined'] += 1
                elif stype == 'INC-only':
                    counts['inc_only'] += 1
                else:
                    counts['mat_only'] += 1
            set_number += 1

        processed.append(f"  {sheet} ({did}): sets {start_set}-{set_number - 1}")

    return (data_rows, value_rows, set_number, processed,
            counts['combined'], counts['inc_only'], counts['mat_only'], diff_rows)


def _convert_interactions(extracted, bracket_map, operands, set_number,
                          extracted_mat=None):
    """Convert all interaction sheets into Data + Value rows.

    Same matching logic as _convert_singles but for (row_bin, col_bin) cells.
    """
    data_rows = []
    value_rows = []
    processed = []
    diff_rows = []
    counts = {'combined': 0, 'inc_only': 0, 'mat_only': 0}
    is_fta = extracted_mat is not None

    all_sheets = _all_sheet_names(extracted, extracted_mat, 'interactions', is_fta)

    for sheet in all_sheets:
        inc_entry = extracted.interactions.get(sheet)
        mat_entry = extracted_mat.interactions.get(sheet) if is_fta else None
        ref = inc_entry or mat_entry
        did1, did2 = ref.did1, ref.did2

        # Flag FTA differences
        if is_fta:
            if inc_entry and not mat_entry:
                diff_rows.append({'Sheet': sheet, 'SheetType': 'Interaction',
                                  'DifferenceType': 'Sheet in INC only',
                                  'Detail': f"{did1} & {did2}"})
            elif mat_entry and not inc_entry:
                diff_rows.append({'Sheet': sheet, 'SheetType': 'Interaction',
                                  'DifferenceType': 'Sheet in MAT only',
                                  'Detail': f"{did1} & {did2}"})
            elif inc_entry and mat_entry:
                for label, a_bins, b_bins, direction in [
                    ('Row bins', set(inc_entry.row_bins), set(mat_entry.row_bins), 'in INC only'),
                    ('Row bins', set(mat_entry.row_bins), set(inc_entry.row_bins), 'in MAT only'),
                    ('Col bins', set(inc_entry.col_bins), set(mat_entry.col_bins), 'in INC only'),
                    ('Col bins', set(mat_entry.col_bins), set(inc_entry.col_bins), 'in MAT only'),
                ]:
                    diff = a_bins - b_bins
                    if diff:
                        diff_rows.append({'Sheet': sheet, 'SheetType': 'Interaction',
                                          'DifferenceType': f'{label} {direction}',
                                          'Detail': "; ".join(sorted(diff))})

        # Use shared helpers for matching strategy and iteration
        fta_pos, row_order, col_order, inc_cells, mat_cells = \
            _prepare_interaction_context(inc_entry, mat_entry, is_fta)

        start_set = set_number
        for r_idx, rb in enumerate(row_order):
            for c_idx, cb in enumerate(col_order):
                inc_c, mat_c = _lookup_interaction_cell(
                    r_idx, c_idx, rb, cb, inc_entry, mat_entry, ref,
                    is_fta, fta_pos, inc_cells, mat_cells)
                if inc_c is None and mat_c is None:
                    continue

                data_rows.extend(_emit_conditions(set_number, did1, rb, bracket_map))
                data_rows.extend(_emit_conditions(set_number, did2, cb, bracket_map))

                value_rows.extend(
                    _emit_coeff_values(set_number, inc_c, mat_c, is_fta, operands))
                if is_fta:
                    stype = _classify_set(inc_c, mat_c, is_fta)
                    if stype == 'Combined':
                        counts['combined'] += 1
                    elif stype == 'INC-only':
                        counts['inc_only'] += 1
                    else:
                        counts['mat_only'] += 1
                set_number += 1

        processed.append(f"  {sheet} ({did1} & {did2}): sets {start_set}-{set_number - 1}")

    return (data_rows, value_rows, set_number, processed,
            counts['combined'], counts['inc_only'], counts['mat_only'], diff_rows)


# ============================================================
# Public API: Conversion
# ============================================================

def run_conversion(key_file, fa_file, output_dir, mode='LTA',
                   fa_mat_file=None, extracted=None, extracted_mat=None):
    """Run conversion in LTA or FTA mode.

    Args:
        key_file:       Path to Key.xlsx
        fa_file:        Path to FA rule file (LTA) or FA INC file (FTA)
        output_dir:     Directory for output CSVs
        mode:           'LTA' or 'FTA'
        fa_mat_file:    Path to FA MAT file (FTA only)
        extracted:      Pre-extracted data (avoids re-reading Excel)
        extracted_mat:  Pre-extracted MAT data (FTA only)

    Returns: (ConversionResult, extracted_data, extracted_mat_data)
             The extracted data is returned so audit/grouping can reuse it.
    """
    sheet_to_did, bracket_map = read_key_mapping(key_file)
    prefix = f'{mode}_'
    operands = OPERANDS_INC  # LTA uses M1/M2; FTA handles its own via helpers

    # Extract data from Excel (once, then reuse)
    if extracted is None:
        extracted = extract_fa_data(fa_file, sheet_to_did, bracket_map)
    if mode == 'FTA' and extracted_mat is None:
        extracted_mat = extract_fa_data(fa_mat_file, sheet_to_did, bracket_map)

    set_number = 1
    mat_arg = extracted_mat if mode == 'FTA' else None

    # Process single-factor sheets
    s_data, s_vals, set_number, s_proc, s_comb, s_inc, s_mat, s_diffs = \
        _convert_singles(extracted, bracket_map, operands, set_number, mat_arg)

    # Process interaction sheets
    i_data, i_vals, set_number, i_proc, i_comb, i_inc, i_mat, i_diffs = \
        _convert_interactions(extracted, bracket_map, operands, set_number, mat_arg)

    # Combine and write
    all_data = s_data + i_data
    all_vals = s_vals + i_vals
    all_proc = s_proc + i_proc
    all_diffs = s_diffs + i_diffs

    out_data = os.path.join(output_dir, f'{prefix}FineAdjustments_Data.csv')
    out_vals = os.path.join(output_dir, f'{prefix}FineAdjustment_Values.csv')
    pd.DataFrame(all_data).to_csv(out_data, index=False)
    pd.DataFrame(all_vals).to_csv(out_vals, index=False)

    # Write merge differences (FTA only)
    diff_path = None
    if mode == 'FTA':
        diff_path = os.path.join(output_dir, f'{prefix}Merge_Differences.csv')
        pd.DataFrame(all_diffs).to_csv(diff_path, index=False)

    all_skipped = extracted.skipped[:]
    if extracted_mat:
        all_skipped.extend(s for s in extracted_mat.skipped if s not in all_skipped)

    result = ConversionResult(
        data_csv_path=out_data,
        values_csv_path=out_vals,
        total_sets=set_number - 1,
        data_row_count=len(all_data),
        value_row_count=len(all_vals),
        sheets_processed=len(all_proc),
        sheet_summary="\n".join(all_proc),
        skip_summary=", ".join(all_skipped) if all_skipped else "None",
        diff_csv_path=diff_path,
        combined_sets=s_comb + i_comb,
        inc_only_sets=s_inc + i_inc,
        mat_only_sets=s_mat + i_mat,
        diff_count=len(all_diffs),
    )
    return result, extracted, extracted_mat


# ============================================================
# Audit helpers
# ============================================================

def _get_value(gen_vals_set, operand):
    """Extract a single coefficient value for an operand from a filtered DataFrame."""
    rows = gen_vals_set[gen_vals_set['Operand'] == operand]
    return rows['Value'].iloc[0] if len(rows) > 0 else None


def _check_coeff(expected, actual, tol=_COEFF_TOL):
    """Check that an expected coefficient matches the generated one.

    Both-None is a PASS; one-None is a FAIL.  Otherwise checks |expected - actual| < tol.
    """
    if expected is None:
        return actual is None
    return actual is not None and abs(actual - expected) < tol


def _audit_singles(extracted, extracted_mat, is_fta, bracket_map,
                   data_by_set, vals_by_set, set_number, detail_rows, summary_rows):
    """Audit all single-factor sheets, appending to detail_rows and summary_rows.

    Returns the next available set_number after processing all singles.
    """
    all_singles = _all_sheet_names(extracted, extracted_mat, 'singles', is_fta)

    for sheet in all_singles:
        inc_entry = extracted.singles.get(sheet)
        mat_entry = extracted_mat.singles.get(sheet) if is_fta else None
        ref = inc_entry or mat_entry
        did = ref.did

        bin_items = _build_single_bin_items(inc_entry, mat_entry, is_fta)

        sheet_start = set_number
        all_match = True
        sums = {'inc_orig': 0, 'mat_orig': 0, 'm1': 0, 'm2': 0, 'm3': 0, 'm4': 0}

        for i, (bin_str, inc_coeff, mat_coeff) in enumerate(bin_items):
            expected_conds = get_bin_operators(bin_str, bracket_map)

            gd = data_by_set.get(set_number, pd.DataFrame())
            gv = vals_by_set.get(set_number, pd.DataFrame())

            m1 = _get_value(gv, 'M1')
            m2 = _get_value(gv, 'M2')

            if is_fta:
                m3 = _get_value(gv, 'M3')
                m4 = _get_value(gv, 'M4')
                if inc_coeff is not None: sums['inc_orig'] += inc_coeff
                if mat_coeff is not None: sums['mat_orig'] += mat_coeff
                if m1 is not None: sums['m1'] += m1
                if m3 is not None: sums['m3'] += m3

                inc_ok = _check_coeff(inc_coeff, m1) and _check_coeff(inc_coeff, m2)
                mat_ok = _check_coeff(mat_coeff, m3) and _check_coeff(mat_coeff, m4)
            else:
                m3 = m4 = None
                if inc_coeff is not None: sums['inc_orig'] += inc_coeff
                if m1 is not None: sums['m1'] += m1
                if m2 is not None: sums['m2'] += m2
                inc_ok = _check_coeff(inc_coeff, m1) and _check_coeff(inc_coeff, m2)
                mat_ok = True

            set_type = _classify_set(inc_coeff, mat_coeff, is_fta)

            gen_conds = list(zip(gd['Operand'].tolist(), gd['Value'].tolist())) if len(gd) > 0 else []
            cond_ok = gen_conds == expected_conds
            did_ok = set(gd['DataItemId'].unique()) == {did} if len(gd) > 0 else False

            row_pass = inc_ok and mat_ok and cond_ok and did_ok
            if not row_pass:
                all_match = False

            detail_rows.append({
                'Sheet': sheet, 'SheetType': 'Single', 'SetType': set_type,
                'BinIndex': i, 'OriginalBin': bin_str, 'SetNumber': set_number,
                'DataItemId': did,
                'INC_Coeff': inc_coeff, 'MAT_Coeff': mat_coeff,
                'M1': m1, 'M2': m2, 'M3': m3, 'M4': m4,
                'Check_INC': 'PASS' if inc_ok else 'FAIL',
                'Check_MAT': 'PASS' if mat_ok else 'FAIL',
                'Check_Conditions': 'PASS' if cond_ok else 'FAIL',
                'Check_DataItemId': 'PASS' if did_ok else 'FAIL',
                'RowResult': 'PASS' if row_pass else 'FAIL',
            })
            set_number += 1

        # Sheet-level sum checks
        if is_fta:
            inc_sum_ok = abs(sums['inc_orig'] - sums['m1']) < _SUM_TOL_TIGHT
            mat_sum_ok = abs(sums['mat_orig'] - sums['m3']) < _SUM_TOL_TIGHT
        else:
            inc_sum_ok = (abs(sums['inc_orig'] - sums['m1']) < _SUM_TOL_TIGHT and
                          abs(sums['inc_orig'] - sums['m2']) < _SUM_TOL_TIGHT)
            mat_sum_ok = True

        summary_rows.append({
            'Sheet': sheet, 'SheetType': 'Single', 'DataItemId': did,
            'BinCount': len(bin_items),
            'SetNumberRange': f"{sheet_start}-{set_number - 1}",
            'Check_INC_Sum': 'PASS' if inc_sum_ok else 'FAIL',
            'Check_MAT_Sum': 'PASS' if mat_sum_ok else 'FAIL',
            'Check_AllRowsPass': 'PASS' if all_match else 'FAIL',
        })

    return set_number


def _audit_interactions(extracted, extracted_mat, is_fta, bracket_map,
                        data_by_set, vals_by_set, set_number, detail_rows, summary_rows):
    """Audit all interaction sheets, appending to detail_rows and summary_rows.

    Returns the next available set_number after processing all interactions.
    """
    all_inters = _all_sheet_names(extracted, extracted_mat, 'interactions', is_fta)

    for sheet in all_inters:
        inc_entry = extracted.interactions.get(sheet)
        mat_entry = extracted_mat.interactions.get(sheet) if is_fta else None
        ref = inc_entry or mat_entry
        did1, did2 = ref.did1, ref.did2

        fta_pos, row_order, col_order, inc_cells, mat_cells = \
            _prepare_interaction_context(inc_entry, mat_entry, is_fta)

        sheet_start = set_number
        all_match = True
        sums = {'inc': 0, 'mat': 0, 'm1': 0, 'm3': 0}
        cell_count = 0

        for r_idx, rb in enumerate(row_order):
            for c_idx, cb in enumerate(col_order):
                inc_coeff, mat_coeff = _lookup_interaction_cell(
                    r_idx, c_idx, rb, cb, inc_entry, mat_entry, ref,
                    is_fta, fta_pos, inc_cells, mat_cells)
                if inc_coeff is None and mat_coeff is None:
                    continue

                cell_count += 1

                gd = data_by_set.get(set_number, pd.DataFrame())
                gv = vals_by_set.get(set_number, pd.DataFrame())

                m1 = _get_value(gv, 'M1')
                m2 = _get_value(gv, 'M2')

                if is_fta:
                    m3 = _get_value(gv, 'M3')
                    m4 = _get_value(gv, 'M4')
                    if inc_coeff is not None: sums['inc'] += inc_coeff
                    if mat_coeff is not None: sums['mat'] += mat_coeff
                    if m1 is not None: sums['m1'] += m1
                    if m3 is not None: sums['m3'] += m3
                    inc_ok = _check_coeff(inc_coeff, m1) and _check_coeff(inc_coeff, m2)
                    mat_ok = _check_coeff(mat_coeff, m3) and _check_coeff(mat_coeff, m4)
                else:
                    m3 = m4 = None
                    if inc_coeff is not None: sums['inc'] += inc_coeff
                    if m1 is not None: sums['m1'] += m1
                    inc_ok = _check_coeff(inc_coeff, m1) and _check_coeff(inc_coeff, m2)
                    mat_ok = True

                set_type = _classify_set(inc_coeff, mat_coeff, is_fta)

                conds1 = get_bin_operators(rb, bracket_map)
                conds2 = get_bin_operators(cb, bracket_map)
                did_ok = set(gd['DataItemId'].unique()) == {did1, did2} if len(gd) > 0 else False
                count_ok = len(gd) == len(conds1) + len(conds2)

                row_pass = inc_ok and mat_ok and did_ok and count_ok
                if not row_pass:
                    all_match = False

                detail_rows.append({
                    'Sheet': sheet, 'SheetType': 'Interaction', 'SetType': set_type,
                    'BinIndex': f"{rb} & {cb}",
                    'OriginalBin': f"{rb} & {cb}", 'SetNumber': set_number,
                    'DataItemId': f"{did1} & {did2}",
                    'INC_Coeff': inc_coeff, 'MAT_Coeff': mat_coeff,
                    'M1': m1, 'M2': m2, 'M3': m3, 'M4': m4,
                    'Check_INC': 'PASS' if inc_ok else 'FAIL',
                    'Check_MAT': 'PASS' if mat_ok else 'FAIL',
                    'Check_Conditions': 'PASS' if count_ok else 'FAIL',
                    'Check_DataItemId': 'PASS' if did_ok else 'FAIL',
                    'RowResult': 'PASS' if row_pass else 'FAIL',
                })
                set_number += 1

        tol = _SUM_TOL_LOOSE
        if is_fta:
            inc_sum_ok = abs(sums['inc'] - sums['m1']) < tol
            mat_sum_ok = abs(sums['mat'] - sums['m3']) < tol
        else:
            inc_sum_ok = abs(sums['inc'] - sums['m1']) < tol
            mat_sum_ok = True

        summary_rows.append({
            'Sheet': sheet, 'SheetType': 'Interaction',
            'DataItemId': f"{did1} & {did2}", 'BinCount': cell_count,
            'SetNumberRange': f"{sheet_start}-{set_number - 1}",
            'Check_INC_Sum': 'PASS' if inc_sum_ok else 'FAIL',
            'Check_MAT_Sum': 'PASS' if mat_sum_ok else 'FAIL',
            'Check_AllRowsPass': 'PASS' if all_match else 'FAIL',
        })

    return set_number


# ============================================================
# Public API: Audit
# ============================================================

def run_audit(key_file, fa_file, conv_result, output_dir, mode='LTA',
              fa_mat_file=None, extracted=None, extracted_mat=None):
    """Validate generated CSVs against the original FA source data.

    Checks per row: coefficient match, DataItemId, conditions, condition count.
    Checks per sheet: coefficient sum match.

    Returns: AuditResult
    """
    sheet_to_did, bracket_map = read_key_mapping(key_file)
    prefix = f'{mode}_'
    is_fta = mode == 'FTA'

    if extracted is None:
        extracted = extract_fa_data(fa_file, sheet_to_did, bracket_map)
    if is_fta and extracted_mat is None:
        extracted_mat = extract_fa_data(fa_mat_file, sheet_to_did, bracket_map)

    gen_data = pd.read_csv(conv_result.data_csv_path)
    gen_vals = pd.read_csv(conv_result.values_csv_path)

    # Pre-index by SetNumber for faster lookups
    data_by_set = dict(list(gen_data.groupby('SetNumber')))
    vals_by_set = dict(list(gen_vals.groupby('SetNumber')))

    detail_rows = []
    summary_rows = []
    set_number = 1

    set_number = _audit_singles(
        extracted, extracted_mat, is_fta, bracket_map, data_by_set, vals_by_set,
        set_number, detail_rows, summary_rows)

    _audit_interactions(
        extracted, extracted_mat, is_fta, bracket_map, data_by_set, vals_by_set,
        set_number, detail_rows, summary_rows)

    # Write audit CSVs
    detail_df = pd.DataFrame(detail_rows)
    summary_df = pd.DataFrame(summary_rows)
    detail_path = os.path.join(output_dir, f'{prefix}Audit_Detail.csv')
    summary_path = os.path.join(output_dir, f'{prefix}Audit_Summary.csv')
    detail_df.to_csv(detail_path, index=False)
    summary_df.to_csv(summary_path, index=False)

    total_sheets = len(summary_df)
    sheets_pass = (summary_df['Check_AllRowsPass'] == 'PASS').sum()
    detail_pass = (detail_df['RowResult'] == 'PASS').sum() if len(detail_df) > 0 else 0
    inc_sp = (summary_df['Check_INC_Sum'] == 'PASS').sum()
    mat_sp = (summary_df['Check_MAT_Sum'] == 'PASS').sum()

    return AuditResult(
        total_sheets=total_sheets, sheets_pass=int(sheets_pass),
        detail_total=len(detail_df), detail_pass=int(detail_pass),
        detail_csv_path=detail_path, summary_csv_path=summary_path,
        sum_pass=int(inc_sp),  # LTA compat
        inc_sum_pass=int(inc_sp), mat_sum_pass=int(mat_sp),
    )


# ============================================================
# Grouping engine
# ============================================================

def _group_row_columns(row_data, threshold, is_fta):
    """Group adjacent column entries within a single row where coefficients are similar.

    Adjacent columns are merged when:
      - They are consecutive in column order
      - They have the same type (combined / INC-only / MAT-only)
      - All available coefficient diffs are within threshold
        (for FTA combined: both INC AND MAT diffs must be within threshold)

    Args:
        row_data:  [(col_bin, inc_coeff_or_None, mat_coeff_or_None, col_idx), ...]
        threshold: Maximum absolute diff between consecutive coefficients
        is_fta:    Whether FTA mode (has mat coefficients)

    Returns: list of groups, each group a list of row_data entries
    """
    if not row_data:
        return []

    groups = []
    current = [row_data[0]]

    for i in range(1, len(row_data)):
        prev = row_data[i - 1]
        curr = row_data[i]

        # Must be adjacent columns
        if curr[3] != prev[3] + 1:
            groups.append(current)
            current = [curr]
            continue

        if is_fta:
            # Must be same type (combined, INC-only, or MAT-only) to merge
            def _entry_type(entry):
                return ('C' if entry[1] is not None and entry[2] is not None
                        else 'I' if entry[1] is not None else 'M')
            if _entry_type(prev) != _entry_type(curr):
                groups.append(current)
                current = [curr]
                continue

        # Check threshold for all available coefficients
        can_merge = True
        if prev[1] is not None and curr[1] is not None:
            if abs(curr[1] - prev[1]) > threshold:
                can_merge = False
        if is_fta and prev[2] is not None and curr[2] is not None:
            if abs(curr[2] - prev[2]) > threshold:
                can_merge = False

        if can_merge:
            current.append(curr)
        else:
            groups.append(current)
            current = [curr]

    groups.append(current)
    return groups


def _group_singles(extracted, extracted_mat, is_fta, bracket_map,
                   data_rows, value_rows, set_number):
    """Pass single-factor sheets through to grouping output unchanged.

    Returns the next available set_number.
    """
    all_singles = _all_sheet_names(extracted, extracted_mat, 'singles', is_fta)

    for sheet in all_singles:
        inc_entry = extracted.singles.get(sheet)
        mat_entry = extracted_mat.singles.get(sheet) if is_fta else None
        ref = inc_entry or mat_entry
        did = ref.did

        bin_items = _build_single_bin_items(inc_entry, mat_entry, is_fta)

        for bin_str, inc_c, mat_c in bin_items:
            data_rows.extend(
                _emit_conditions(set_number, did, bin_str, bracket_map))
            value_rows.extend(
                _emit_coeff_values(set_number, inc_c, mat_c, is_fta, OPERANDS_INC))
            set_number += 1

    return set_number


def _group_interactions(extracted, extracted_mat, is_fta, bracket_map, threshold,
                        data_rows, value_rows, set_number, stats):
    """Group adjacent interaction columns and append to data_rows/value_rows/stats.

    Adjacent columns within the same row are merged when their coefficients differ
    by at most *threshold*.  For FTA, both INC and MAT diffs must be within threshold.

    Returns the next available set_number.
    """
    all_inters = _all_sheet_names(extracted, extracted_mat, 'interactions', is_fta)

    for sheet in all_inters:
        inc_entry = extracted.interactions.get(sheet)
        mat_entry = extracted_mat.interactions.get(sheet) if is_fta else None
        ref = inc_entry or mat_entry
        did1, did2 = ref.did1, ref.did2

        fta_pos, row_order, col_order, inc_cells, mat_cells = \
            _prepare_interaction_context(inc_entry, mat_entry, is_fta)

        original_cells = 0
        grouped_cells = 0
        start_set = set_number

        for r_idx, rb in enumerate(row_order):
            row_data = []
            for c_idx, cb in enumerate(col_order):
                inc_c, mat_c = _lookup_interaction_cell(
                    r_idx, c_idx, rb, cb, inc_entry, mat_entry, ref,
                    is_fta, fta_pos, inc_cells, mat_cells)
                if inc_c is None and mat_c is None:
                    continue
                row_data.append((cb, inc_c, mat_c, c_idx))
            original_cells += len(row_data)

            if not row_data:
                continue

            groups = _group_row_columns(row_data, threshold, is_fta)
            grouped_cells += len(groups)

            for group in groups:
                col_bins_in_group = [g[0] for g in group]
                inc_coeffs = [g[1] for g in group if g[1] is not None]
                mat_coeffs = [g[2] for g in group if g[2] is not None]

                avg_inc = sum(inc_coeffs) / len(inc_coeffs) if inc_coeffs else None
                avg_mat = sum(mat_coeffs) / len(mat_coeffs) if mat_coeffs else None

                # Row factor conditions (unchanged)
                data_rows.extend(
                    _emit_conditions(set_number, did1, rb, bracket_map))

                # Column factor conditions (merged range)
                merged_conds = merge_bin_ranges(col_bins_in_group, bracket_map)
                if merged_conds is None:
                    for cb in col_bins_in_group:
                        data_rows.extend(
                            _emit_conditions(set_number, did2, cb, bracket_map))
                else:
                    data_rows.extend(
                        [_make_data_row(set_number, did2, op, val)
                         for op, val in merged_conds])

                rounded_inc = round(avg_inc, _COEFF_ROUND_DIGITS) if avg_inc is not None else None
                rounded_mat = round(avg_mat, _COEFF_ROUND_DIGITS) if avg_mat is not None else None
                value_rows.extend(
                    _emit_coeff_values(set_number, rounded_inc, rounded_mat,
                                       is_fta, OPERANDS_INC))
                set_number += 1

        reduction = ((original_cells - grouped_cells) / original_cells * 100
                     ) if original_cells > 0 else 0
        stats.append({
            'Sheet': sheet, 'OriginalCells': original_cells,
            'GroupedCells': grouped_cells, 'ReductionPct': round(reduction, 1),
            'SetRange': f"{start_set}-{set_number - 1}",
        })

    return set_number


def run_grouping(key_file, fa_file, conv_result, output_dir, mode='LTA',
                 fa_mat_file=None, extracted=None, extracted_mat=None,
                 threshold=DEFAULT_THRESHOLD):
    """Group adjacent interaction columns where coefficients differ by <= threshold.

    Single-factor sheets pass through unchanged.  For interaction sheets, each
    row is scanned left-to-right; adjacent columns that are within threshold
    (and, for FTA, both INC and MAT must be within threshold) are merged into
    one wider range with averaged coefficients.

    Returns: GroupingResult
    """
    sheet_to_did, bracket_map = read_key_mapping(key_file)
    prefix = f'{mode}_'
    is_fta = mode == 'FTA'

    if extracted is None:
        extracted = extract_fa_data(fa_file, sheet_to_did, bracket_map)
    if is_fta and extracted_mat is None:
        extracted_mat = extract_fa_data(fa_mat_file, sheet_to_did, bracket_map)

    grouped_data_rows = []
    grouped_value_rows = []
    set_number = 1
    stats = []

    set_number = _group_singles(
        extracted, extracted_mat, is_fta, bracket_map,
        grouped_data_rows, grouped_value_rows, set_number)

    _group_interactions(
        extracted, extracted_mat, is_fta, bracket_map, threshold,
        grouped_data_rows, grouped_value_rows, set_number, stats)

    # Write outputs
    out_data = os.path.join(output_dir, f'{prefix}FineAdjustments_Data_Grouped.csv')
    out_vals = os.path.join(output_dir, f'{prefix}FineAdjustment_Values_Grouped.csv')
    pd.DataFrame(grouped_data_rows).to_csv(out_data, index=False)
    pd.DataFrame(grouped_value_rows).to_csv(out_vals, index=False)

    stats_df = pd.DataFrame(stats)
    stats_path = os.path.join(output_dir, f'{prefix}Grouping_Summary.csv')
    stats_df.to_csv(stats_path, index=False)

    total_orig = sum(s['OriginalCells'] for s in stats)
    total_grouped = sum(s['GroupedCells'] for s in stats)
    overall_reduction = ((total_orig - total_grouped) / total_orig * 100
                         ) if total_orig > 0 else 0

    return GroupingResult(
        data_csv_path=out_data, values_csv_path=out_vals,
        summary_csv_path=stats_path, total_sets=set_number - 1,
        data_row_count=len(grouped_data_rows),
        value_row_count=len(grouped_value_rows),
        original_cells=total_orig, grouped_cells=total_grouped,
        reduction_pct=round(overall_reduction, 1), sheets_grouped=len(stats),
    )


# ============================================================
# Workflow orchestration  (called by GUI, testable standalone)
# ============================================================

def _noop_progress(stage, step, total):
    """Default no-op progress callback used when no GUI is attached."""
    pass


def workflow_convert(key_file, fa_file, output_dir, mode='LTA',
                     fa_mat_file=None, progress=None):
    """Run conversion only. Returns (ConversionResult, extracted, extracted_mat).

    Args:
        progress: Optional callback(stage_label, current_step, total_steps).
                  Called at each stage transition so the GUI can update.
    """
    cb = progress or _noop_progress
    cb("Reading Excel files...", 0, 2)
    cb("Converting sheets...", 1, 2)
    result = run_conversion(key_file, fa_file, output_dir, mode, fa_mat_file)
    cb("Conversion complete", 2, 2)
    return result


def workflow_convert_audit(key_file, fa_file, output_dir, mode='LTA',
                           fa_mat_file=None, progress=None):
    """Run conversion then audit. Returns (ConversionResult, AuditResult).

    Args:
        progress: Optional callback(stage_label, current_step, total_steps).
    """
    cb = progress or _noop_progress
    cb("Reading Excel files...", 0, 4)
    cb("Converting sheets...", 1, 4)
    conv, ext, ext_mat = run_conversion(key_file, fa_file, output_dir, mode,
                                        fa_mat_file)
    cb("Auditing output...", 2, 4)
    audit = run_audit(key_file, fa_file, conv, output_dir, mode, fa_mat_file,
                      ext, ext_mat)
    cb("Audit complete", 4, 4)
    return conv, audit


def workflow_convert_audit_group(key_file, fa_file, output_dir, mode='LTA',
                                 fa_mat_file=None, threshold=DEFAULT_THRESHOLD,
                                 progress=None):
    """Run conversion, audit, then grouping (only if audit passes).

    Returns (ConversionResult, AuditResult, GroupingResult_or_None).
    GroupingResult is None if audit had failures.

    Args:
        progress: Optional callback(stage_label, current_step, total_steps).
    """
    cb = progress or _noop_progress
    cb("Reading Excel files...", 0, 6)
    cb("Converting sheets...", 1, 6)
    conv, ext, ext_mat = run_conversion(key_file, fa_file, output_dir, mode,
                                        fa_mat_file)
    cb("Auditing output...", 2, 6)
    audit = run_audit(key_file, fa_file, conv, output_dir, mode, fa_mat_file,
                      ext, ext_mat)

    if audit.sheets_pass < audit.total_sheets:
        cb("Audit failed — grouping skipped", 6, 6)
        return conv, audit, None

    cb("Grouping adjacent bins...", 4, 6)
    grp = run_grouping(key_file, fa_file, conv, output_dir, mode, fa_mat_file,
                       ext, ext_mat, threshold)
    cb("Grouping complete", 6, 6)
    return conv, audit, grp

