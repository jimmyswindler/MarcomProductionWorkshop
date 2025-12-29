# 30_DataBundler.py
import pandas as pd
import numpy as np
import os
import re
import yaml
import sys
from itertools import combinations
import time
import traceback
import json
import argparse
import utils_ui 

# =========================================================
# THE BUNDLING CONSTITUTION (IRON LAWS)
# =========================================================
CONSTITUTION = {
    "FUEL_GAUGE": {
        "ENABLED": True,
        "YELLOW_ZONE_START": 18750, 
        "RED_ZONE_START": 5750      
    },
    "INTEGRITY_CHECKS": {
        "VALIDATE_WHOLE_JOBS": True 
    }
}

# --- CONFIGURATION ---
def load_config_from_path(config_path="config.yaml"):
    if not os.path.exists(config_path):
        utils_ui.print_error(f"Configuration file not found at '{config_path}'")
        sys.exit(1)
    try:
        with open(config_path, 'r') as f: config = yaml.safe_load(f)
        return config
    except Exception as e:
        utils_ui.print_error(f"Could not parse YAML file: {e}")
        sys.exit(1)

# ======================
# RUN HISTORY FUNCTIONS
# ======================
def load_run_history(history_path="run_history.yaml"):
    if not os.path.exists(history_path):
        default_history = {'monthly_pace_job_number': 100000, 'last_used_gang_run_suffix': 0}
        with open(history_path, 'w') as f: yaml.dump(default_history, f)
        return default_history
    try:
        with open(history_path, 'r') as f: return yaml.safe_load(f)
    except Exception as e:
        utils_ui.print_error(f"History file error: {e}"); sys.exit(1)

def save_run_history(pace_number, last_suffix, history_path="run_history.yaml"):
    try:
        history_data = {'monthly_pace_job_number': pace_number, 'last_used_gang_run_suffix': last_suffix}
        with open(history_path, 'w') as f: yaml.dump(history_data, f)
    except Exception as e: utils_ui.print_warning(f"Could not save run history file: {e}")

def safe_get_list(config_dict, key_path):
    keys = key_path.split('.')
    value = config_dict
    try:
        for key in keys:
            value = value.get(key)
            if value is None: return []
        return value if isinstance(value, list) else []
    except: return []

# =========================================================
# BUNDLING HELPERS
# =========================================================
def rebuild_pools(line_item_indices, df, primary_entity_col, col_qty, col_order, col_base_job):
    """
    Groups line items by Entity (Store) and provides hierarchy access.
    """
    entity_pool = {}
    if not line_item_indices: return entity_pool
    
    pool_df = df.loc[list(line_item_indices)]
    
    if primary_entity_col in pool_df.columns:
        for entity_id, group in pool_df.groupby(primary_entity_col):
            # Pre-calculate subgroups for "Giant Slayer" logic
            orders = {}
            if col_order:
                for oid, ogroup in group.groupby(col_order):
                    jobs = {}
                    if col_base_job:
                        for jid, jgroup in ogroup.groupby(col_base_job):
                            jobs[jid] = {'qty': jgroup[col_qty].sum(), 'indices': jgroup.index.tolist()}
                    orders[oid] = {'qty': ogroup[col_qty].sum(), 'indices': ogroup.index.tolist(), 'jobs': jobs}

            entity_pool[entity_id] = {
                'Total_Qty': int(round(group[col_qty].sum())),
                'Line_Indices': group.index.tolist(),
                'Orders': orders
            }
    return entity_pool

def _create_filler_rows(gap_qty, config):
    filler_rows = []
    col_names = config.get('column_names', {})
    col_job_ticket_num = col_names.get('job_ticket_number')
    col_qty = col_names.get('quantity_ordered')
    col_url = col_names.get('one_up_output_file_url') 
    filler_defs = config.get('bundling_rules', {}).get('filler_definitions', {})

    if not all([col_job_ticket_num, col_qty, col_url, filler_defs]): return pd.DataFrame()

    def get_filler(blank_id):
        if blank_id in filler_defs:
            return {col_job_ticket_num: blank_id, col_qty: filler_defs[blank_id].get('qty', 0), col_url: filler_defs[blank_id].get('path', '')}
        return None

    if gap_qty > 0:
        if gap_qty == 250:
            row1 = get_filler('BLANK-01')
            if row1: filler_rows.append(row1)
        elif gap_qty == 500:
            row1 = get_filler('BLANK-01'); row2 = get_filler('BLANK-02')
            if row1: filler_rows.append(row1)
            if row2: filler_rows.append(row2)
    return pd.DataFrame(filler_rows)

def _create_and_finalize_bundle(line_indices, bundle_name, df, target_qty, config, filler_map, final_bundles_dict):
    if not line_indices: return
    line_indices = list(dict.fromkeys(line_indices))
    
    bundle_df = df.loc[line_indices].copy()
    actual_qty = bundle_df[config.get('column_names', {}).get('quantity_ordered')].sum()
    preferred_bundle_qty = config.get('bundling_rules', {}).get('preferred_bundle_quantity', 6250)

    gap_qty = 0
    if actual_qty > preferred_bundle_qty: gap_qty = 0
    elif actual_qty == target_qty: gap_qty = filler_map.get(target_qty, 0) 
    else: gap_qty = preferred_bundle_qty - actual_qty
    if gap_qty < 0: gap_qty = 0
    
    if gap_qty > 0:
        filler_df = _create_filler_rows(gap_qty, config)
        if not filler_df.empty: bundle_df = pd.concat([bundle_df, filler_df], ignore_index=True, sort=False)
            
    final_bundles_dict[bundle_name] = bundle_df

def _find_exact_match_subset(candidates, target_qty, max_items=25):
    """
    Finds a combination of entities that sum EXACTLY to the target_qty.
    Optimized by grouping entities by quantity (Subset Sum Problem).
    """
    # 1. Filter and Group by Quantity
    pool = [c for c in candidates if c['Total_Qty'] <= target_qty]
    if not pool: return None
    
    qty_map = {}
    for c in pool:
        q = c['Total_Qty']
        if q not in qty_map: qty_map[q] = []
        qty_map[q].append(c)
        
    unique_qtys = sorted(qty_map.keys(), reverse=True)
    final_solution = None

    # 2. DFS on Counts
    def solve_counts(idx, remain_target, solution_counts, item_count):
        nonlocal final_solution
        if final_solution: return
        
        if remain_target == 0:
            final_solution = solution_counts
            return
        
        if idx >= len(unique_qtys) or item_count >= max_items:
            return
            
        qty = unique_qtys[idx]
        available_count = len(qty_map[qty])
        
        # Max copies we can theoretically take
        max_theoretical = remain_target // qty
        # Actual max we can take
        max_use = min(available_count, max_theoretical)
        
        # Optimization: Prune if even taking all remaining largest items can't reach target?
        # (Simple greedy pruning is risky in subset sum, sticking to basic DFS on counts is safe for "small" N)

        # Try from max_use down to 0
        for count in range(max_use, -1, -1):
            if item_count + count <= max_items:
                solve_counts(idx + 1, remain_target - (count * qty), solution_counts + [(qty, count)], item_count + count)
                if final_solution: return

    solve_counts(0, target_qty, [], 0)
    
    # 3. Reconstruct
    if final_solution:
        selected_entities = []
        for qty, count in final_solution:
            # Take the first 'count' entities from the list for this quantity
            selected_entities.extend(qty_map[qty][:count])
        return selected_entities

    return None

def _attempt_top_up_with_real_work(current_indices, current_qty, entity_pool, preferred_qty):
    """
    Scans the remaining pool for WHOLE stores (Sand) to fill a gap 
    Using EXACT MATCH logic first to avoid partial fills.
    """
    gap = preferred_qty - current_qty
    if gap <= 0: return current_indices, current_qty

    candidates = [e for e in entity_pool.values()]
    existing_set = set(current_indices)
    valid_candidates = [c for c in candidates if not any(idx in existing_set for idx in c['Line_Indices'])]

    # Try exact match first
    match = _find_exact_match_subset(valid_candidates, gap)
    if match:
        top_up_indices = []
        fill_qty = 0
        for m in match:
            top_up_indices.extend(m['Line_Indices'])
            fill_qty += m['Total_Qty']
        return current_indices + top_up_indices, current_qty + fill_qty

    # Fallback to greedy largest-fill (Best Effort) if exact not found
    # (Though typically we prefer exact or nothing, but top-up implies "get as close as possible"?)
    # Original logic was greedy best effort. 
    # Let's keep greedy best effort as fallback if exact fails, 
    # OR should we stick to "only exact" for top up? 
    # "While a bundle is at partial capacity, smaller whole store(s) should be consumed to fill that space."
    # Implicitly, we want to fill it completely.
    # If we can't fill it completely, finding the largest chunk is better than nothing?
    # Let's keep the greedy fallback for maximizing fill if exact fails.
    
    valid_candidates.sort(key=lambda x: x['Total_Qty'], reverse=True)
    
    top_up_indices = []
    fill_qty = 0
    
    for cand in valid_candidates:
        if fill_qty + cand['Total_Qty'] <= gap:
            top_up_indices.extend(cand['Line_Indices'])
            fill_qty += cand['Total_Qty']
            
        if fill_qty == gap: break
            
    return current_indices + top_up_indices, current_qty + fill_qty

# =========================================================
# STRATEGIES (Hierarchy Based)
# =========================================================

def _strategy_0_lockdown(fragment_df, entity_pool, df, col_qty, bundle_search_thresholds, preferred_bundle_qty, min_threshold):
    """
    PHASE 0: LOCKDOWN (Consecutive Consumption).
    If we have a fragment from the queue, we MUST use it as the seed.
    If the fragment is larger than a bundle (Giant Remnant), we slice it.
    """
    seed_qty = fragment_df[col_qty].sum()
    seed_indices = fragment_df.index.tolist()
    
    # A. Handle Oversized Fragments (e.g. 8000 remaining from a 14250 store)
    if seed_qty > preferred_bundle_qty:
        target = preferred_bundle_qty
        slice_indices = []
        current_sum = 0
        
        # Greedy sequential slice of the fragment
        for idx in seed_indices:
            val = df.loc[idx, col_qty]
            if current_sum + val <= target:
                slice_indices.append(idx)
                current_sum += val
            else: break
            
        if slice_indices:
            # We return the slice as the bundle.
            # The remainder becomes the 'new' fragment to push back to queue.
            rem_indices = [x for x in seed_indices if x not in slice_indices]
            new_frag = df.loc[rem_indices].copy()
            return slice_indices, target, new_frag

    # B. Standard Fragment (<= 6250)
    # We treat this fragment as the ANCHOR.
    # We try to fill the rest using Exact Match Subset Sum first.
    
    current_indices = list(seed_indices)
    current_qty = seed_qty
    gap = preferred_bundle_qty - current_qty
    
    candidates = [e for e in entity_pool.values()]
    # _find_exact_match_subset expects a list of dicts with 'Total_Qty'
    
    match = _find_exact_match_subset(candidates, gap)
    
    if match:
         for m in match:
             current_indices.extend(m['Line_Indices'])
             current_qty += m['Total_Qty']
             
         return current_indices, current_qty, None

    # Fallback to Greedy Bucket Sweep if exact match fails
    candidates.sort(key=lambda x: x['Total_Qty'], reverse=True)
    
    for partner in candidates:
        if gap == 0: break
        if partner['Total_Qty'] <= gap:
            current_indices.extend(partner['Line_Indices'])
            current_qty += partner['Total_Qty']
            gap -= partner['Total_Qty']
            
    return current_indices, current_qty, None


def _strategy_giant_slayer(entity_pool, df, col_qty, bundle_search_thresholds):
    """
    Phase 1: Handle Stores > 6250.
    Logic: Fragment them down to valid bundles.
    Priority: Whole Orders -> Whole Jobs -> Lines.
    Returns the REMAINDER as new_fragment_df to enforce consecutive consumption.
    """
    giants = [e for e in entity_pool.values() if e['Total_Qty'] > 6250]
    if not giants: return None, None, None
    
    giants.sort(key=lambda x: x['Total_Qty'], reverse=True)
    giant = giants[0]
    
    for target in sorted(bundle_search_thresholds, reverse=True):
        
        # A. Try Whole Orders
        current_indices = []
        current_qty = 0
        sorted_orders = sorted(giant['Orders'].values(), key=lambda x: x['qty'], reverse=True)
        
        for order in sorted_orders:
            if order['qty'] > target:
                if current_qty == 0:
                    sorted_jobs = sorted(order['jobs'].values(), key=lambda x: x['qty'], reverse=True)
                    job_indices = []
                    job_qty = 0
                    for job in sorted_jobs:
                        if job_qty + job['qty'] <= target:
                            job_indices.extend(job['indices'])
                            job_qty += job['qty']
                    
                    if job_qty == target:
                        all_giant_indices = set(giant['Line_Indices'])
                        bundle_set = set(job_indices)
                        rem_indices = list(all_giant_indices - bundle_set)
                        new_frag = df.loc[rem_indices].copy()
                        return job_indices, target, new_frag
                continue

            if current_qty + order['qty'] <= target:
                current_indices.extend(order['indices'])
                current_qty += order['qty']
                
        if current_qty == target:
             all_giant_indices = set(giant['Line_Indices'])
             bundle_set = set(current_indices)
             rem_indices = list(all_giant_indices - bundle_set)
             new_frag = df.loc[rem_indices].copy()
             return current_indices, target, new_frag

    # Fallback: Slice lines
    target = 6250
    slice_indices = []
    current_qty = 0
    all_lines = giant['Line_Indices']
    
    for idx in all_lines:
        val = df.loc[idx, col_qty]
        if current_qty + val <= target:
            slice_indices.append(idx)
            current_qty += val
        else: break
        
    if slice_indices:
        all_giant_indices = set(giant['Line_Indices'])
        bundle_set = set(slice_indices)
        rem_indices = list(all_giant_indices - bundle_set)
        new_frag = df.loc[rem_indices].copy()
        return slice_indices, current_qty, new_frag

    return None, None, None

def _strategy_combiner_no_fragmentation(entity_pool, bundle_search_thresholds):
    """
    Phase 2: Handle Stores <= 6250.
    Logic: Combine Whole Stores Only using Subset Sum to find EXACT matches.
    """
    candidates = [e for e in entity_pool.values() if e['Total_Qty'] <= 6250]
    # No need to sort upfront for logic, but helps deterministic behavior if we iterate (not used in _find_exact_match_subset logic directly but for falling back)
    
    # Check max threshold first (e.g. 6250), then 6000, etc.
    for target in sorted(bundle_search_thresholds, reverse=True):
        
        match = _find_exact_match_subset(candidates, target)
        if match:
            current_indices = []
            for m in match:
                current_indices.extend(m['Line_Indices'])
            return current_indices, target

    return None, None

# =========================================================
# ORCHESTRATOR LEVEL 1: PRIMARY ENTITY LOOP
# =========================================================
def bundle_primary_entity_sequential(df, start_bundle_num, base_bundle_name, config, category_name, bundle_rules, 
                                     initial_stats, primary_entity_col, preferred_bundle_qty, bundle_search_thresholds, filler_map, master_tracking_list, disqualified_indices):
    if df.empty: return {}, pd.DataFrame(), start_bundle_num, initial_stats.get(category_name, {}), {}
    
    bundle_name_suffix = bundle_rules.get('bundle_name_suffix')
    leftover_destination = bundle_rules.get('leftover_sheet_name')
    MIN_BUNDLE_THRESHOLD = min(bundle_search_thresholds) # Should be 5750
    
    col_names = config.get('column_names', {})
    col_qty = col_names.get('quantity_ordered')
    col_cost_center = col_names.get('cost_center')
    col_order = col_names.get('order_number')
    col_base_job = col_names.get('base_job_ticket_number')
    col_job_ticket_num = col_names.get('job_ticket_number')

    stats = initial_stats.get(category_name, {})
    final_bundles, row_destinations = {}, {}
    bundle_counter = start_bundle_num
    
    line_item_pool = set(df.index)
    fragment_lockdown_queue = [] # Stores DFs of splits that must be used next
    
    def get_next_bundle_name():
        nonlocal bundle_counter
        name = f"{bundle_name_suffix}{bundle_counter:03d}"; bundle_counter += 1
        return name
        
    def log_destination(indices, dest):
        subset = df.loc[indices].copy()
        subset['Destination'] = dest
        master_tracking_list.append(subset)
        [row_destinations.setdefault(idx, dest) for idx in indices]

    def get_pool():
         return rebuild_pools(line_item_pool, df, primary_entity_col, col_qty, col_order, col_base_job)

    MAX_PASSES = 3000
    outer_pass_num = 0
    
    with utils_ui.create_progress() as progress:
        task = progress.add_task(f"Bundling {category_name}...", total=None) 
        
        while outer_pass_num < MAX_PASSES:
            outer_pass_num += 1
            progress.update(task, description=f"Bundling {category_name} (Pass {outer_pass_num})")
            
            if not line_item_pool and not fragment_lockdown_queue: break
            
            # 1. Update Pool Total (Safety Check)
            current_pool_total = df.loc[list(line_item_pool), col_qty].sum() if line_item_pool else 0
            if fragment_lockdown_queue:
                current_pool_total += sum([f[col_qty].sum() for f in fragment_lockdown_queue])
                
            if not fragment_lockdown_queue and current_pool_total < 5750: 
                break
                
            # 2. Rebuild Pool
            entity_pool = get_pool()
            
            bundle_indices = None
            target_hit = 0
            new_frag_df = None
            
            # --- PHASE 0: LOCKDOWN (Queue Priority) ---
            if fragment_lockdown_queue:
                frag_df = fragment_lockdown_queue.pop(0)
                # Ensure fragment indices are not in entity pool (they shouldn't be)
                bundle_indices, target_hit, new_frag_df = _strategy_0_lockdown(
                    frag_df, entity_pool, df, col_qty, bundle_search_thresholds, preferred_bundle_qty, MIN_BUNDLE_THRESHOLD
                )
            else:
                # --- PHASE 1: GIANT SLAYER (Fragmentation Allowed) ---
                has_giants = any(e['Total_Qty'] > 6250 for e in entity_pool.values())
                
                if has_giants:
                     bundle_indices, target_hit, new_frag_df = _strategy_giant_slayer(entity_pool, df, col_qty, bundle_search_thresholds)
                
                # --- PHASE 2: COMBINER (No Fragmentation) ---
                if not bundle_indices:
                     bundle_indices, target_hit = _strategy_combiner_no_fragmentation(entity_pool, bundle_search_thresholds)
            
            # --- PHASE 3: TOP-UP (Priority Use of Real Work) ---
            if bundle_indices:
                if target_hit < preferred_bundle_qty:
                    # Scan remaining entity pool for small stores to fill gap
                    # Note: entity_pool is still valid because we haven't committed indices yet
                    bundle_indices, target_hit = _attempt_top_up_with_real_work(
                        bundle_indices, target_hit, entity_pool, preferred_bundle_qty
                    )
            
            # 4. Finalize
            if bundle_indices:
                bname = get_next_bundle_name()
                _create_and_finalize_bundle(bundle_indices, bname, df, target_hit, config, filler_map, final_bundles)
                log_destination(bundle_indices, bname)
                
                line_item_pool.difference_update(bundle_indices)
                
                # CRITICAL: If Giant Slayer or Lockdown returned a remainder, 
                # immediately queue it to force consecutive consumption.
                if new_frag_df is not None and not new_frag_df.empty:
                    # Remove fragment indices from general pool
                    line_item_pool.difference_update(new_frag_df.index)
                    # Push to front of queue
                    fragment_lockdown_queue.insert(0, new_frag_df)
            else:
                break

    # --- FINALIZE ---
    all_bundled_indices = set()
    for bundle_df in final_bundles.values(): all_bundled_indices.update(bundle_df.index)
            
    # Recover stranded fragments
    if fragment_lockdown_queue:
        for fdf in fragment_lockdown_queue:
            line_item_pool.update(fdf.index)

    leftovers = df.loc[list(line_item_pool)].copy()
    if not leftovers.empty:
        log_destination(leftovers.index.tolist(), leftover_destination)

    bundled_qty = sum(b.loc[~b[col_job_ticket_num].astype(str).str.startswith('BLANK-'), col_qty].sum() for b in final_bundles.values())
    leftover_qty = leftovers[col_qty].sum() if not leftovers.empty else 0
    
    utils_ui.print_info(f"Summary ({category_name}): {len(final_bundles)} bundles ({int(bundled_qty):,} qty) | Leftovers: {int(leftover_qty):,} qty")
    
    return final_bundles, leftovers, bundle_counter, stats

# =========================================================
# VALIDATION FUNCTIONS
# =========================================================
def validate_bundles(all_bundles, config):
    utils_ui.print_section("Mix Validation")
    bb = safe_get_list(config, 'product_ids.12ptBounceBack')
    bc = safe_get_list(config, 'categorization_rules.business_card_identifiers')
    bc_reg = '|'.join(re.escape(i) for i in bc) if bc else '(?!)'
    col_pid = config.get('column_names', {}).get('product_id')
    col_desc = config.get('column_names', {}).get('paper_description')
    col_job = config.get('column_names', {}).get('job_ticket_number')

    for n, b in all_bundles.items():
        val = b[~b[col_job].astype(str).str.startswith('BLANK-')] if col_job in b.columns else b
        if val.empty: continue
        has_bb = val[col_pid].astype(str).isin(bb).any() if col_pid in val.columns else False
        has_bc = val[col_desc].str.contains(bc_reg, case=False).any() if col_desc in val.columns and pd.api.types.is_string_dtype(val[col_desc]) else False
        if has_bb and has_bc: utils_ui.print_error(f"Mix Error in {n}"); return False
    
    utils_ui.print_success("Mix Validation Passed.")
    return True

def validate_constitution(all_bundles, output_sheets, config, immune_stores):
    utils_ui.print_section("Constitution Check")
    col_cost = config.get('column_names', {}).get('cost_center')
    col_qty = config.get('column_names', {}).get('quantity_ordered')
    col_job = config.get('column_names', {}).get('job_ticket_number')
    col_base = config.get('column_names', {}).get('base_job_ticket_number')
    
    EXEMPT_CATEGORIES = set(config.get('bundling_rules', {}).get('exempt_categories', []))
    if not EXEMPT_CATEGORIES: EXEMPT_CATEGORIES = {'PrintOnDemand', 'LargeFormat', 'Outsource', 'Apparel', 'Promo', 'Unknown'}

    dq_config = config.get('bundling_rules', {}).get('disqualify_jobs_over_quantity', {})
    dq_enabled = dq_config.get('enabled', False)
    dq_threshold = dq_config.get('threshold', 1000) 

    errors, violation_details = [], []

    if CONSTITUTION['INTEGRITY_CHECKS']['VALIDATE_WHOLE_JOBS'] and col_cost:
        bundled_job_map = {} 
        for b_name, b_df in all_bundles.items():
            if not b_df.empty and col_cost in b_df.columns and col_base in b_df.columns:
                val_df = b_df[~b_df[col_job].astype(str).str.startswith('BLANK-')]
                for _, row in val_df.iterrows():
                    key = (row[col_cost], row[col_base])
                    bundled_job_map[key] = b_name

        for sheet_name, l_df in output_sheets.items():
            if sheet_name in all_bundles or sheet_name == 'exceptions' or l_df.empty: continue
            is_sheet_exempt = sheet_name in EXEMPT_CATEGORIES
            if col_cost not in l_df.columns or col_base not in l_df.columns: continue
            
            for _, row in l_df.iterrows():
                if row.get('__IS_DISQUALIFIED') == True: continue 
                sid = row[col_cost]
                if sid in immune_stores: continue 
                row_cat = row.get('Category', '')
                if is_sheet_exempt or row_cat in EXEMPT_CATEGORIES: continue 
                item_qty = row.get(col_qty, 0)
                if dq_enabled and item_qty > dq_threshold: continue 
                jid = row[col_base]
                if (sid, jid) in bundled_job_map:
                    p_bundle = bundled_job_map[(sid, jid)]
                    violation_details.append(f"JOB SPLIT: Store '{sid}' Job '{jid}' (Found in {p_bundle}) also found in Leftover '{sheet_name}' (Qty {item_qty}).")

        if violation_details:
            errors.append(f"IRON LAW VIOLATION: Job Fragmentation detected.")
            for v in violation_details[:10]: errors.append(f"    - {v}")
            if len(violation_details) > 10: errors.append(f"    ... and {len(violation_details) - 10} more violations.")
        else:
             utils_ui.print_success("Whole Job Law Passed.")

    if errors:
        utils_ui.print_error("CONSTITUTIONAL FAILURE")
        for e in errors: utils_ui.print_error(f"{e}")
        fail_mode = config.get('bundling_rules', {}).get('fail_on_fragmentation_violation', True)
        if fail_mode: return False, violation_details
        else:
            utils_ui.print_warning("Fragmentation violations detected but ignored per configuration.")
            return True, violation_details
    
    utils_ui.print_success("Constitution Passed.")
    return True, []

def _build_hierarchical_frag_map(master_df, col_cost_center, col_order_num, col_base_job, immune_stores):
    store_report, unclaimed = {}, {"orders": {}, "jobs": {}}
    if master_df.empty or 'Destination' not in master_df.columns: 
        return {"store_report_map": store_report, "unclaimed_report_map": unclaimed}
    
    EXEMPT_DESTS = {'PrintOnDemand', 'LargeFormat', 'Outsource', 'Apparel', 'Promo', 'Unknown', '25up layout'}
    
    if '__IS_DISQUALIFIED' in master_df.columns: analysis_df = master_df[master_df['__IS_DISQUALIFIED'] != True].copy()
    else: analysis_df = master_df.copy()

    def _get_status(dests):
        valid = set(dests) - {'Unknown'}
        bundles = {d for d in valid if d not in EXEMPT_DESTS and not d.endswith('BounceBack') and not d.endswith('BusinessCard')}
        leftovers = {d for d in valid if d.endswith('BounceBack') or d.endswith('BusinessCard')}
        is_frag = False
        if len(bundles) > 1: is_frag = True
        if len(bundles) > 0 and len(leftovers) > 0: is_frag = True
        return is_frag, list(valid)

    entity_dests = {
        'store': analysis_df.groupby(col_cost_center)['Destination'].apply(set),
        'order': analysis_df.groupby(col_order_num)['Destination'].apply(set),
        'job': analysis_df.groupby(col_base_job)['Destination'].apply(set)
    }
    
    status = {k: {id: _get_status(d) for id, d in v.items()} for k, v in entity_dests.items()}
    frag_stores = {sid for sid, (is_frag, _) in status['store'].items() if is_frag and sid not in immune_stores}
    claimed_orders, claimed_jobs = set(), set()

    for sid, sgroup in master_df[master_df[col_cost_center].isin(frag_stores)].groupby(col_cost_center):
        _, s_dests = status['store'].get(sid); s_entry = {"is_fragmented": True, "destinations": s_dests, "fragmented_orders": {}}
        for oid, ogroup in sgroup.groupby(col_order_num):
            o_frag, o_dests = status['order'].get(oid, (False, [])); claimed_orders.add(oid); o_entry = {"is_fragmented": o_frag, "destinations": o_dests, "fragmented_jobs": {}}
            for jid, _ in ogroup.groupby(col_base_job):
                j_frag, j_dests = status['job'].get(jid, (False, [])); claimed_jobs.add(jid); o_entry["fragmented_jobs"][jid] = {"is_fragmented": j_frag, "destinations": j_dests}
            s_entry["fragmented_orders"][oid] = o_entry
        store_report[sid] = s_entry
    
    for oid, (is_frag, dests) in status['order'].items():
        if is_frag and oid not in claimed_orders:
             entry = {"is_fragmented": True, "destinations": dests, "fragmented_jobs": {}}
             for jid in master_df[master_df[col_order_num] == oid][col_base_job].unique():
                 jf, jd = status['job'].get(jid, (False, [])); entry["fragmented_jobs"][jid] = {"is_fragmented": jf, "destinations": jd}
             unclaimed["orders"][oid] = entry
    for jid, (is_frag, dests) in status['job'].items():
        if is_frag and jid not in claimed_jobs: unclaimed["jobs"][jid] = {"is_fragmented": True, "destinations": dests}

    return {"store_report_map": store_report, "unclaimed_report_map": unclaimed}

# =========================================================
# ORCHESTRATOR LEVEL 2: MAIN CONTROLLER
# =========================================================
def run_bundling_process(categorized_data_sheets, output_file, config):
    col_names = config.get('column_names', {})
    if not all(k in col_names for k in ['order_number', 'job_ticket_number', 'quantity_ordered', 'cost_center', 'base_job_ticket_number']):
        utils_ui.print_error("Missing required config columns."); return None, None

    bundling_rules = config.get('bundling_rules', {})
    cats_to_bundle = [k for k, v in bundling_rules.items() if isinstance(v, dict) and 'bundle_name_suffix' in v]
    
    history = load_run_history()
    base_name, bundle_ctr = history['monthly_pace_job_number'], history['last_used_gang_run_suffix'] + 1
    initial_ctr = bundle_ctr
    
    all_bundles, output_sheets, all_remainders = {}, {}, []
    master_tracking_list = []
    
    store_origins = {}
    for sheet_name, df in categorized_data_sheets.items():
        if col_names['cost_center'] in df.columns:
            unique_stores = df[col_names['cost_center']].unique()
            for sid in unique_stores:
                if sid not in store_origins: store_origins[sid] = set()
                store_origins[sid].add(sheet_name)
    
    immune_stores = {sid for sid, origins in store_origins.items() if len(origins) > 1}
    utils_ui.print_info(f"Identified {len(immune_stores)} stores with 'Arrival Immunity'.")

    if 'exceptions' in categorized_data_sheets: 
        df_exc = categorized_data_sheets.pop('exceptions')
        output_sheets['exceptions'] = df_exc

    for cat, df in categorized_data_sheets.items():
        if cat in cats_to_bundle:
            utils_ui.print_section(f"Processing Category: {cat}")
            rules = bundling_rules.get(cat)
            
            dq_rule = bundling_rules.get('disqualify_jobs_over_quantity', {})
            if dq_rule.get('enabled') and cat in dq_rule.get('categories', []):
                 mask = df[col_names['quantity_ordered']] > dq_rule.get('threshold', 1000)
                 if mask.any():
                     dq_jobs = df.loc[mask, col_names['base_job_ticket_number']].unique()
                     dq_rows = df[df[col_names['base_job_ticket_number']].isin(dq_jobs)]
                     df = df[~df[col_names['base_job_ticket_number']].isin(dq_jobs)]
                     dq_df = dq_rows.copy()
                     dq_df['__IS_DISQUALIFIED'] = True
                     all_remainders.append(dq_df)
                     utils_ui.print_info(f"Disqualified {len(dq_jobs)} jobs based on quantity threshold.")

            bundles, rem, bundle_ctr, _ = bundle_primary_entity_sequential(
                df, bundle_ctr, base_name, config, cat, rules, {}, col_names['cost_center'],
                bundling_rules.get('preferred_bundle_quantity', 6250),
                bundling_rules.get('bundle_search_thresholds', [6250]),
                {int(k): v for k,v in bundling_rules.get('filler_padding_map', {}).items()},
                master_tracking_list,
                set()
            )
            all_bundles.update(bundles)
            if not rem.empty: all_remainders.append(rem)
        else:
            output_sheets[cat] = df
            df_copy = df.copy()
            df_copy['Destination'] = cat
            master_tracking_list.append(df_copy)

    # Re-process remainders
    if all_remainders:
        left_df = pd.concat([r.reindex(columns=list(set().union(*(x.columns for x in all_remainders)))) for r in all_remainders], ignore_index=True)
        # Simplify fallback logic
        fallback = bundling_rules.get('leftover_category_fallback', 'PrintOnDemand')
        sheet_map = {cat: bundling_rules.get(cat, {}).get('leftover_sheet_name', fallback) for cat in cats_to_bundle}
        
        for cat in left_df['Category'].unique():
             dest_sheet = sheet_map.get(cat, fallback)
             subset = left_df[left_df['Category'] == cat].copy()
             output_sheets[dest_sheet] = pd.concat([output_sheets.get(dest_sheet, pd.DataFrame()), subset], ignore_index=True)
             if 'Destination' not in subset.columns or subset['Destination'].isna().all():
                 subset['Destination'] = dest_sheet
                 master_tracking_list.append(subset)

    if not validate_bundles(all_bundles, config): return None, None
    is_valid_constit, violations = validate_constitution(all_bundles, output_sheets, config, immune_stores)
    if not is_valid_constit: return None, None 

    utils_ui.print_section("Generating Fragmentation Map")
    master_frag_df = pd.DataFrame()
    if master_tracking_list:
        master_frag_df = pd.concat(master_tracking_list, ignore_index=True, sort=False)
    
    all_frag_maps = _build_hierarchical_frag_map(
        master_frag_df, col_names['cost_center'], col_names['order_number'], col_names['base_job_ticket_number'], immune_stores
    )

    utils_ui.print_info(f"Saving to {os.path.basename(output_file)}...")
    with pd.ExcelWriter(output_file) as writer:
        cols = set()
        for d in list(output_sheets.values()) + list(all_bundles.values()): cols.update(d.columns)
        final_cols = sorted(list(cols))
        if '__IS_DISQUALIFIED' in final_cols: final_cols.remove('__IS_DISQUALIFIED')
        
        col_job_key = col_names.get('job_ticket_number')
        col_store_key = col_names.get('cost_center')
        for n in sorted(all_bundles.keys()): 
            # Sort by Store then Job Ticket Number
            sort_cols = []
            if col_store_key and col_store_key in all_bundles[n].columns:
                sort_cols.append(col_store_key)
            if col_job_key and col_job_key in all_bundles[n].columns:
                sort_cols.append(col_job_key)
            
            if sort_cols:
                all_bundles[n] = all_bundles[n].sort_values(by=sort_cols)
                
            all_bundles[n].reindex(columns=final_cols).to_excel(writer, sheet_name=n, index=False)
        order = safe_get_list(config, 'sheet_output_order')
        written = set()
        for s in order:
             if s in output_sheets:
                 output_sheets[s].reindex(columns=final_cols).to_excel(writer, sheet_name=s, index=False)
                 written.add(s)
        for s in sorted(output_sheets.keys()):
             if s not in written: output_sheets[s].reindex(columns=final_cols).to_excel(writer, sheet_name=s, index=False)

    if bundle_ctr > initial_ctr: save_run_history(base_name, bundle_ctr - 1)
    return output_file, all_frag_maps

def main(input_path, output_dir, config_path):
    utils_ui.setup_logging(None)
    utils_ui.print_banner("20b - Auto Bundler")
    cfg = load_config_from_path(config_path)
    try:
        dfs = pd.read_excel(input_path, sheet_name=None)
        # Normalize cols logic similar to original
        for n, d in dfs.items(): 
            for c in ['order_number', 'job_ticket_number', 'product_id', 'sku']:
                if cfg['column_names'].get(c) in d.columns: 
                    d[cfg['column_names'][c]] = d[cfg['column_names'][c]].astype(str).replace('nan', '')

        out_path = os.path.join(output_dir, os.path.splitext(os.path.basename(input_path))[0].replace("_CATEGORIZED", "") + ".xlsx")
        res, fmap = run_bundling_process(dfs, out_path, cfg)
        if res and fmap:
            with open(out_path.replace(".xlsx", "_fragmap.json"), 'w') as f: json.dump(fmap, f, indent=4)
            utils_ui.print_success("Bundling Complete.")
        else: raise Exception("Bundling Failed")
    except Exception as e:
        utils_ui.print_error(f"Critical Error: {e}"); traceback.print_exc(); sys.exit(1)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])