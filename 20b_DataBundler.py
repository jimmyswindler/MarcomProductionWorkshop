# 20b_DataBundler.py
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

# =========================================================
# THE BUNDLING CONSTITUTION (IRON LAWS)
# =========================================================
# 1. WHOLE JOB LAW: A specific JOB cannot exist in both a Bundle and Leftovers.
#    - Splitting a STORE is allowed, provided the split happens between jobs.
#    - EXCEPTION: "Arrival Immunity" - If a store is fragmented in the input files,
#      it is exempt from strict fragmentation reporting.
# 2. CONSECUTIVE BUNDLE LAW: Fragments must appear on consecutive numbered bundles.
# 3. MINIMUM BUNDLE LAW: No bundle < 5,750 is allowed (except filler-padded fragments).
# 4. FUEL GAUGE LAW: Do not start new fragments when fuel is low.
# =========================================================

CONSTITUTION = {
    "FUEL_GAUGE": {
        "ENABLED": True,
        "YELLOW_ZONE_START": 18750, 
        "RED_ZONE_START": 5750      
    },
    "YIELD_PROTECTION": {
        "DISALLOW_RUNT_BUNDLES": True, 
        "MIN_BUNDLE_SIZE": 5750,
        "SAND_RATIO_REQUIRED": 1.1 
    },
    "INTEGRITY_CHECKS": {
        "VALIDATE_WHOLE_JOBS": True 
    }
}

# --- CONFIGURATION ---
def load_config_from_path(config_path="config.yaml"):
    if not os.path.exists(config_path):
        print(f"FATAL ERROR: Configuration file not found at '{config_path}'")
        sys.exit(1)
    try:
        with open(config_path, 'r') as f: config = yaml.safe_load(f)
        print("✓ Configuration loaded successfully from path.")
        return config
    except Exception as e:
        print(f"FATAL ERROR: Could not parse YAML file: {e}"); sys.exit(1)

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
        print(f"FATAL ERROR: History file error: {e}"); sys.exit(1)

def save_run_history(pace_number, last_suffix, history_path="run_history.yaml"):
    try:
        history_data = {'monthly_pace_job_number': pace_number, 'last_used_gang_run_suffix': last_suffix}
        with open(history_path, 'w') as f: yaml.dump(history_data, f)
    except Exception as e: print(f"⚠️ WARNING: Could not save run history file: {e}")

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
# BUNDLING ALGORITHM
# =========================================================
def rebuild_pools(line_item_indices, df, primary_entity_col, col_base_job, col_qty):
    entity_pool, job_pool = {}, {}
    if not line_item_indices: return entity_pool, job_pool
    pool_df = df.loc[list(line_item_indices)]
    if primary_entity_col in pool_df.columns:
        for entity_id, group in pool_df.groupby(primary_entity_col):
            entity_pool[entity_id] = {
                'Total_Qty': group[col_qty].sum(),
                'Line_Indices': group.index.tolist(),
                'Job_IDs': group[col_base_job].unique().tolist()
            }
    if col_base_job in pool_df.columns:
        for job_id, group in pool_df.groupby(col_base_job):
            job_pool[job_id] = {
                'Total_Qty': group[col_qty].sum(),
                'Line_Indices': group.index.tolist()
            }
    return entity_pool, job_pool

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
        elif gap_qty > 0 and gap_qty % 250 == 0:
            num_fillers = gap_qty // 250
            for i in range(num_fillers):
                filler_id = 'BLANK-01' if (i % 2) == 0 else 'BLANK-02'
                row = get_filler(filler_id)
                if row: filler_rows.append(row)
    return pd.DataFrame(filler_rows)

def _create_and_finalize_bundle(line_indices, bundle_name, df, target_qty, config, filler_map, final_bundles_dict):
    if not line_indices: return
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

def _get_sand_qty(job_pool, sand_threshold=2500):
    sand_qty = 0
    for jid, jinfo in job_pool.items():
        if jinfo['Total_Qty'] <= sand_threshold:
            sand_qty += jinfo['Total_Qty']
    return sand_qty

# --- STRATEGIES ---

def _find_combination_for_seed(seed_qty, bundle_search_thresholds, job_pool, line_item_pool_indices, df, col_qty, max_combo_d=25):
    for target_threshold in sorted(bundle_search_thresholds, reverse=True):
        target_needed = target_threshold - seed_qty
        if target_needed <= 0: continue
            
        eligible_jobs = {jid: jinfo for jid, jinfo in job_pool.items() 
                         if jinfo['Total_Qty'] <= target_needed and 
                            all(idx in line_item_pool_indices for idx in jinfo['Line_Indices'])}
        
        if eligible_jobs:
            sorted_job_items = sorted(eligible_jobs.items(), key=lambda item: item[0])
            found_combo_jobs = None
            for k in range(1, min(len(sorted_job_items) + 1, max_combo_d + 1)):
                for combo in combinations(sorted_job_items, k):
                    if sum(item[1]['Total_Qty'] for item in combo) == target_needed:
                        found_combo_jobs = [item[0] for item in combo]
                        break
                if found_combo_jobs: break
            
            if found_combo_jobs:
                combo_indices = [idx for jid in found_combo_jobs for idx in job_pool[jid]['Line_Indices']]
                return combo_indices, target_threshold
                
        available_lines_df = df.loc[list(line_item_pool_indices)].sort_index(ascending=True)
        available_lines_df = available_lines_df[available_lines_df[col_qty] <= target_needed]

        if not available_lines_df.empty and available_lines_df[col_qty].sum() >= target_needed:
            current_bundle_lines = []
            current_bundle_qty = 0
            for line_idx, line_row in available_lines_df.iterrows():
                line_qty = line_row[col_qty]
                if current_bundle_qty + line_qty <= target_needed:
                    current_bundle_lines.append(line_idx)
                    current_bundle_qty += line_qty
            if current_bundle_qty == target_needed:
                return current_bundle_lines, target_threshold
    return None, None

def _find_perfect_sequential_pack(available_lines_df, col_qty, bundle_search_thresholds):
    for target_qty in sorted(bundle_search_thresholds, reverse=True):
        current_bundle_lines = []
        current_bundle_qty = 0
        for line_idx, line_row in available_lines_df.iterrows():
             line_qty = line_row[col_qty]
             if current_bundle_qty + line_qty <= target_qty:
                 current_bundle_lines.append(line_idx)
                 current_bundle_qty += line_qty
        if current_bundle_qty == target_qty:
            return current_bundle_lines, target_qty
    return None, None

def _strategy_p0_handle_fragment_lockdown(fragment_df, line_item_pool, job_pool, df, col_qty,
                                          bundle_search_thresholds, preferred_bundle_qty):
    print("  - Running Priority 0 (TERMINATOR MODE)...")
    seed_qty = fragment_df[col_qty].sum()
    seed_indices = fragment_df.index.tolist()
    
    # Case A: Slice oversized fragment
    if seed_qty > preferred_bundle_qty:
        print(f"    - P0: Slicing oversized fragment (Qty: {seed_qty}).")
        current_bundle_lines = []
        current_bundle_qty = 0
        lines_df = fragment_df.sort_index(ascending=True)
        for line_idx, line_row in lines_df.iterrows():
             line_qty = line_row[col_qty]
             if current_bundle_qty + line_qty <= preferred_bundle_qty:
                 current_bundle_lines.append(line_idx); current_bundle_qty += line_qty
             else: break 
        if not current_bundle_lines:
             first_line_idx = lines_df.index[0]
             current_bundle_lines = [first_line_idx]
             current_bundle_qty = lines_df.loc[first_line_idx, col_qty]
        remainder_indices = [idx for idx in seed_indices if idx not in current_bundle_lines]
        new_fragment_df = df.loc[remainder_indices].copy() if remainder_indices else None
        final_target_qty = current_bundle_qty if current_bundle_qty in bundle_search_thresholds else preferred_bundle_qty
        return current_bundle_lines, final_target_qty, new_fragment_df

    # Case B: Perfect Fit
    elif seed_qty in bundle_search_thresholds:
        print(f"    - P0: Fragment perfect match ({seed_qty}).")
        return seed_indices, seed_qty, None

    # Case C: Top Up
    else:
        print(f"    - P0: Fragment needs top-up (Seed: {seed_qty}).")
        combo_indices, target_qty_met = _find_combination_for_seed(
            seed_qty, bundle_search_thresholds, job_pool, line_item_pool, df, col_qty, max_combo_d=25
        )
        
        if combo_indices:
             print(f"    - P0 (Case C): Found perfect sequential combination. Creating {target_qty_met}-qty bundle.")
             all_bundle_indices = seed_indices + combo_indices
             return all_bundle_indices, target_qty_met, None

        # Forced Top-Up
        print(f"    - P0 (Case C): Could not find perfect top-up. Initiating NUCLEAR FILLER OPTION.")
        target_needed = preferred_bundle_qty - seed_qty
        
        available_lines_df = df.loc[list(line_item_pool)].sort_index(ascending=True)
        top_up_indices = []
        top_up_qty = 0
        for line_idx, line_row in available_lines_df.iterrows():
            top_up_indices.append(line_idx)
            top_up_qty += line_row[col_qty]
            if top_up_qty >= target_needed: break 
        
        all_blob_indices = seed_indices + top_up_indices
        all_blob_df = df.loc[all_blob_indices].sort_index(ascending=True)
        
        current_bundle_lines = []
        current_bundle_qty = 0
        for line_idx, line_row in all_blob_df.iterrows():
             line_qty = line_row[col_qty]
             if current_bundle_qty + line_qty <= preferred_bundle_qty:
                 current_bundle_lines.append(line_idx); current_bundle_qty += line_qty
             else: break 
        
        if not current_bundle_lines and not all_blob_df.empty:
             first_line_idx = all_blob_df.index[0]
             current_bundle_lines = [first_line_idx]
             current_bundle_qty = all_blob_df.loc[first_line_idx, col_qty]

        remainder_indices = [idx for idx in all_blob_indices if idx not in current_bundle_lines]
        new_fragment_df = df.loc[remainder_indices].copy() if remainder_indices else None
        final_target_qty = preferred_bundle_qty 
        
        return current_bundle_lines, final_target_qty, new_fragment_df

def _strategy_pa_perfect_entities(entity_pool, bundle_search_thresholds):
    """ Priority A: Largest Perfect Entities First. """
    print("  - Running Priority A (Perfect Entities)...")
    for target_qty in sorted(bundle_search_thresholds, reverse=True):
        candidates = [eid for eid, einfo in entity_pool.items() if einfo['Total_Qty'] == target_qty]
        if candidates:
            candidates.sort()
            found_entity_id = candidates[0]
            print(f"    - A: Found Perfect Entity '{found_entity_id}' ({target_qty}).")
            line_indices = entity_pool[found_entity_id]['Line_Indices']
            return line_indices, target_qty
    return None, None

def _strategy_pb1_split_oversized_entities(entity_pool, job_pool, fragment_lockdown_queue, df, col_qty,
                                           min_bundle_threshold, preferred_bundle_qty, bundle_search_thresholds,
                                           total_pool_qty, red_zone_threshold):
    """ Priority B1: Split oversized entities with Sand Check. """
    print("  - Running Priority B1 (Split Oversized Entities)...")
    
    # 1. Look-Ahead Check (Eclipse Protection)
    future_pool = total_pool_qty - preferred_bundle_qty
    if future_pool < red_zone_threshold:
        print(f"    - B1: Aborting split. Future pool ({future_pool}) would be < Red Zone ({red_zone_threshold}).")
        return None, None

    # 2. Candidates
    eligible_oversized_entities = sorted([
        (eid, einfo['Total_Qty']) for eid, einfo in entity_pool.items() 
        if einfo['Total_Qty'] > preferred_bundle_qty and 
           all(jid in job_pool and job_pool[jid]['Total_Qty'] <= preferred_bundle_qty for jid in einfo['Job_IDs'])
    ], key=lambda x: x[1], reverse=True)
    
    if eligible_oversized_entities:
        entity_id_to_split, entity_qty = eligible_oversized_entities[0]
        
        # 3. Sand Check
        remainder_est = entity_qty - preferred_bundle_qty
        while remainder_est > preferred_bundle_qty: remainder_est -= preferred_bundle_qty
        
        sand_needed = 0
        if remainder_est < min_bundle_threshold:
            sand_needed = min_bundle_threshold - remainder_est
        
        available_sand = _get_sand_qty(job_pool)
        sand_ratio = CONSTITUTION['YIELD_PROTECTION']['SAND_RATIO_REQUIRED']
        
        if sand_needed > 0 and available_sand < (sand_needed * sand_ratio):
            print(f"    - B1: SKIPPING split of '{entity_id_to_split}'. Insufficient Sand.")
            return None, None

        print(f"    - B1: Splitting Entity '{entity_id_to_split}' (Qty: {entity_qty}).")
        
        entity_indices = entity_pool[entity_id_to_split]['Line_Indices']
        entity_lines_df = df.loc[entity_indices].sort_index(ascending=True)
        current_bundle_lines, current_bundle_qty = _find_perfect_sequential_pack(entity_lines_df, col_qty, bundle_search_thresholds)

        if not current_bundle_lines: return None, None
        
        remainder_indices = [idx for idx in entity_indices if idx not in current_bundle_lines]
        if remainder_indices:
            remainder_df = df.loc[remainder_indices].copy()
            fragment_lockdown_queue.append(remainder_df)
        
        return current_bundle_lines, current_bundle_qty
                
    return None, None

def _strategy_pb2_split_oversized_jobs(line_item_pool, job_pool, fragment_lockdown_queue, df, col_qty, 
                                       min_bundle_threshold, preferred_bundle_qty, bundle_search_thresholds,
                                       total_pool_qty, red_zone_threshold):
    """ Priority B2: Split oversized jobs with Sand Check and Eclipse Protection. """
    print("  - Running Priority B2 (Split Oversized Jobs)...")
    
    # 1. Look-Ahead Check
    future_pool = total_pool_qty - preferred_bundle_qty
    if future_pool < red_zone_threshold:
        print(f"    - B2: Aborting split. Future pool ({future_pool}) would be < Red Zone.")
        return None, None

    oversized_job_items = sorted([
        (jid, jinfo) for jid, jinfo in job_pool.items() 
        if jinfo['Total_Qty'] > preferred_bundle_qty
    ], key=lambda item: item[1]['Total_Qty'], reverse=True)
    
    if oversized_job_items:
        job_id_to_split, job_info_to_split = oversized_job_items[0]
        
        # Sand Check
        job_qty = job_info_to_split['Total_Qty']
        remainder_est = job_qty - preferred_bundle_qty
        while remainder_est > preferred_bundle_qty: remainder_est -= preferred_bundle_qty
        
        sand_needed = 0
        if remainder_est < min_bundle_threshold:
            sand_needed = min_bundle_threshold - remainder_est
            
        available_sand = _get_sand_qty(job_pool)
        sand_ratio = CONSTITUTION['YIELD_PROTECTION']['SAND_RATIO_REQUIRED']

        if sand_needed > 0 and available_sand < (sand_needed * sand_ratio):
            print(f"    - B2: SKIPPING split of Job '{job_id_to_split}'. Insufficient Sand.")
            return None, None
        
        print(f"    - B2: Splitting Job '{job_id_to_split}' (Qty: {job_qty}).")
        
        line_indices_to_process = [idx for idx in job_info_to_split['Line_Indices'] if idx in line_item_pool]
        lines_df = df.loc[line_indices_to_process].sort_index(ascending=True)
        simulated_bundle_lines, simulated_bundle_qty = _find_perfect_sequential_pack(lines_df, col_qty, bundle_search_thresholds)
        
        if not simulated_bundle_lines: return None, None

        simulated_remainder_indices = [idx for idx in line_indices_to_process if idx not in simulated_bundle_lines]
        if simulated_remainder_indices:
            remainder_df = df.loc[simulated_remainder_indices].copy()
            fragment_lockdown_queue.append(remainder_df)

        return simulated_bundle_lines, simulated_bundle_qty
            
    return None, None

def _strategy_pb3_perfect_jobs(job_pool, bundle_search_thresholds):
    print("  - Running Priority B3 (Perfect Jobs)...")
    for target_qty in bundle_search_thresholds:
        perfect_job_ids = sorted([jid for jid, jinfo in job_pool.items() if jinfo['Total_Qty'] == target_qty])
        if perfect_job_ids:
            job_id_to_bundle = perfect_job_ids[0]
            print(f"    - B3: Found Perfect Job '{job_id_to_bundle}'.")
            return job_pool[job_id_to_bundle]['Line_Indices'], target_qty
    return None, None

def _strategy_pc_combine_entities(entity_pool, bundle_search_thresholds, min_bundle_threshold):
    print("  - Running Priority C (Combine Entities)...")
    sub_threshold_entities = {eid: einfo for eid, einfo in entity_pool.items() if einfo['Total_Qty'] < min_bundle_threshold}
    if sub_threshold_entities:
        sorted_items = sorted(sub_threshold_entities.items(), key=lambda item: item[0])
        # Increased max combination from 8 to 26 (checking up to 25 entities)
        for target_qty in bundle_search_thresholds:
            for k in range(1, min(len(sorted_items) + 1, 26)):
                for combo in combinations(sorted_items, k):
                    if sum(item[1]['Total_Qty'] for item in combo) == target_qty:
                        print(f"    - C: Found {k}-Entity combo for {target_qty}.")
                        line_indices = [idx for eid in [x[0] for x in combo] for idx in entity_pool[eid]['Line_Indices']]
                        return line_indices, target_qty
    return None, None

def _strategy_pd1_combine_jobs(job_pool, bundle_search_thresholds, min_bundle_threshold):
    print("    - D1: Combining Jobs...")
    sub_threshold_jobs = {jid: jinfo for jid, jinfo in job_pool.items() if jinfo['Total_Qty'] < min_bundle_threshold}
    if sub_threshold_jobs:
        sorted_items = sorted(sub_threshold_jobs.items(), key=lambda item: item[0])
        # Increased max combination from 8 to 26 (checking up to 25 jobs)
        for target_qty in bundle_search_thresholds:
            for k in range(1, min(len(sorted_items) + 1, 26)):
                for combo in combinations(sorted_items, k):
                    if sum(item[1]['Total_Qty'] for item in combo) == target_qty:
                        print(f"    - D1: Found {k}-Job combo for {target_qty}.")
                        line_indices = [idx for jid in [x[0] for x in combo] for idx in job_pool[jid]['Line_Indices']]
                        return line_indices, target_qty
    return None, None

def _strategy_pd2_greedy_lines(line_item_pool, df, col_qty, bundle_search_thresholds, preferred_bundle_qty, 
                               fragment_lockdown_queue, col_cost_center):
    """ 
    Priority D2: Greedy Line Packing (Smart Version).
    If this strategy splits a store, it MUST lock down the remainder to guarantee integrity.
    """
    print("    - D2: Greedy Line Packing...")
    
    available_lines_df = df.loc[list(line_item_pool)].sort_index(ascending=True)
    
    current_bundle_lines, target_qty = _find_perfect_sequential_pack(
        available_lines_df, col_qty, bundle_search_thresholds
    )
    
    if current_bundle_lines:
        print(f"      - D2: Found greedy line pack for {target_qty}.")
        
        # --- INTEGRITY CHECK: Did we split the last store? ---
        if col_cost_center in df.columns:
            last_idx = current_bundle_lines[-1]
            last_store_id = df.loc[last_idx, col_cost_center]
            
            pool_indices = set(available_lines_df.index)
            picked_indices = set(current_bundle_lines)
            remaining_indices = pool_indices - picked_indices
            
            if remaining_indices:
                remainder_mask = available_lines_df.loc[list(remaining_indices), col_cost_center] == last_store_id
                orphaned_indices = remainder_mask[remainder_mask].index.tolist()
                
                if orphaned_indices:
                    print(f"      - D2 ALERT: Split Store '{last_store_id}' (Parent in current bundle). Locking {len(orphaned_indices)} remainder items.")
                    remainder_df = df.loc[orphaned_indices].copy()
                    fragment_lockdown_queue.append(remainder_df)
        
        return current_bundle_lines, target_qty
        
    return None, None

# =========================================================
# ORCHESTRATOR
# =========================================================
def bundle_primary_entity_sequential(df, start_bundle_num, base_bundle_name, config, category_name, bundle_rules, 
                                     initial_stats, primary_entity_col, preferred_bundle_qty, bundle_search_thresholds, filler_map, master_tracking_list, disqualified_indices):
    if df.empty: return {}, pd.DataFrame(), start_bundle_num, initial_stats.get(category_name, {}), {}
    
    bundle_name_suffix = bundle_rules.get('bundle_name_suffix')
    leftover_destination = bundle_rules.get('leftover_sheet_name')
    MIN_BUNDLE_THRESHOLD = min(bundle_search_thresholds)
    SORTED_THRESHOLDS = sorted(bundle_search_thresholds, reverse=True)
    
    col_names = config.get('column_names', {})
    col_qty = col_names.get('quantity_ordered')
    col_cost_center = col_names.get('cost_center')
    col_base_job = col_names.get('base_job_ticket_number')
    col_job_ticket_num = col_names.get('job_ticket_number')

    stats = initial_stats.get(category_name, {})
    final_bundles, row_destinations = {}, {}
    bundle_counter = start_bundle_num
    original_indices = df.index.tolist(); line_item_pool = set(df.index)
    fragment_lockdown_queue = []
    
    entity_pool, job_pool = rebuild_pools(line_item_pool, df, primary_entity_col, col_base_job, col_qty)
    
    RED_ZONE = CONSTITUTION['FUEL_GAUGE']['RED_ZONE_START']
    YELLOW_ZONE = CONSTITUTION['FUEL_GAUGE']['YELLOW_ZONE_START']

    def get_next_bundle_name():
        nonlocal bundle_counter
        name = f"{bundle_name_suffix}{bundle_counter:03d}"; bundle_counter += 1
        return name
        
    def log_destination(indices, dest):
        subset = df.loc[indices].copy()
        subset['Destination'] = dest
        master_tracking_list.append(subset)
        [row_destinations.setdefault(idx, dest) for idx in indices]

    MAX_PASSES = 1000; outer_pass_num = 0
    while outer_pass_num < MAX_PASSES:
        outer_pass_num += 1
        bundle_made = False
        
        if not line_item_pool and not fragment_lockdown_queue: break
        
        run_only_p0 = bool(fragment_lockdown_queue)
        
        entity_pool, job_pool = rebuild_pools(line_item_pool, df, primary_entity_col, col_base_job, col_qty)
        current_pool_qty = df.loc[list(line_item_pool), col_qty].sum() if line_item_pool else 0
        
        allow_splitting = True
        if not run_only_p0:
            print(f"\n--- Pass {outer_pass_num} | Pool: {int(current_pool_qty)} (Y:{YELLOW_ZONE}, R:{RED_ZONE}) ---")
            
            if current_pool_qty < RED_ZONE:
                print("  ! RED ZONE DETECTED.")
                
                bundled_stores = set()
                for b in final_bundles.values():
                    if not b.empty and col_cost_center in b.columns:
                        data_only = b[~b[col_job_ticket_num].astype(str).str.startswith('BLANK-')]
                        bundled_stores.update(data_only[col_cost_center].unique())
                
                pool_df = df.loc[list(line_item_pool)]
                pool_stores = set(pool_df[col_cost_center].unique())
                
                stranded_stores = bundled_stores.intersection(pool_stores)
                
                if stranded_stores:
                    print(f"  ! RESCUE MISSION: Found {len(stranded_stores)} stranded store fragments.")
                    rescue_indices = pool_df[pool_df[col_cost_center].isin(stranded_stores)].index.tolist()
                    
                    if rescue_indices:
                        print(f"  -> Locking {len(rescue_indices)} items for forced bundling.")
                        rescue_df = df.loc[rescue_indices].copy()
                        fragment_lockdown_queue.append(rescue_df)
                        line_item_pool.difference_update(rescue_indices) 
                        continue 
                
                print("  ! STOPPING BUNDLING.")
                break
                
            if current_pool_qty < YELLOW_ZONE:
                print("  ! YELLOW ZONE. SPLITTING DISABLED.")
                allow_splitting = False
        else:
            print(f"\n--- Pass {outer_pass_num} | FRAGMENT LOCKDOWN ---")

        # --- STRATEGIES ---
        if fragment_lockdown_queue:
            fragment_df = fragment_lockdown_queue.pop(0)
            b_indices, t_qty, new_frag = _strategy_p0_handle_fragment_lockdown(
                fragment_df, line_item_pool, job_pool, df, col_qty, SORTED_THRESHOLDS, preferred_bundle_qty
            )
            if b_indices:
                bname = get_next_bundle_name()
                _create_and_finalize_bundle(b_indices, bname, df, t_qty, config, filler_map, final_bundles)
                log_destination(b_indices, bname)
                line_item_pool.difference_update(b_indices)
                
                bundled_stores = df.loc[b_indices, col_cost_center].unique().tolist()
                print(f"      -> COMMITTED: Bundle {bname} | Stores: {bundled_stores}")
                
                if new_frag is not None and not new_frag.empty: fragment_lockdown_queue.insert(0, new_frag)
                bundle_made = True; continue
        
        if run_only_p0: continue

        pipeline = [lambda: _strategy_pa_perfect_entities(entity_pool, SORTED_THRESHOLDS)]
        if allow_splitting:
            pipeline.extend([
                lambda: _strategy_pb1_split_oversized_entities(entity_pool, job_pool, fragment_lockdown_queue, df, col_qty, MIN_BUNDLE_THRESHOLD, preferred_bundle_qty, SORTED_THRESHOLDS, current_pool_qty, RED_ZONE),
                lambda: _strategy_pb2_split_oversized_jobs(line_item_pool, job_pool, fragment_lockdown_queue, df, col_qty, MIN_BUNDLE_THRESHOLD, preferred_bundle_qty, SORTED_THRESHOLDS, current_pool_qty, RED_ZONE)
            ])
        pipeline.extend([
            lambda: _strategy_pb3_perfect_jobs(job_pool, SORTED_THRESHOLDS),
            lambda: _strategy_pc_combine_entities(entity_pool, SORTED_THRESHOLDS, MIN_BUNDLE_THRESHOLD),
            lambda: _strategy_pd1_combine_jobs(job_pool, SORTED_THRESHOLDS, MIN_BUNDLE_THRESHOLD),
            lambda: _strategy_pd2_greedy_lines(line_item_pool, df, col_qty, SORTED_THRESHOLDS, preferred_bundle_qty, fragment_lockdown_queue, col_cost_center)
        ])

        for strat in pipeline:
            b_indices, t_qty = strat()
            if b_indices:
                bname = get_next_bundle_name()
                _create_and_finalize_bundle(b_indices, bname, df, t_qty, config, filler_map, final_bundles)
                log_destination(b_indices, bname)
                line_item_pool.difference_update(b_indices)
                
                bundled_stores = df.loc[b_indices, col_cost_center].unique().tolist()
                print(f"      -> COMMITTED: Bundle {bname} | Stores: {bundled_stores}")
                
                bundle_made = True; break
        
        if not bundle_made: break

    # --- FINALIZE ---
    if fragment_lockdown_queue:
        for fdf in fragment_lockdown_queue: line_item_pool.update(fdf.index)
    
    leftovers = df.loc[list(line_item_pool)].copy()
    if not leftovers.empty:
        log_destination(leftovers.index.tolist(), leftover_destination)

    bundled_rows = sum(len(b) for b in final_bundles.values())
    bundled_qty = sum(b.loc[~b[col_job_ticket_num].astype(str).str.startswith('BLANK-'), col_qty].sum() for b in final_bundles.values())
    leftover_rows = len(leftovers); leftover_qty = leftovers[col_qty].sum() if not leftovers.empty else 0
    
    print(f"\n--- Report ({category_name}) ---")
    print(f"Bundled: {len(final_bundles)} bundles, {int(bundled_qty):,} qty.")
    print(f"Leftovers: {int(leftover_qty):,} qty.")
    
    return final_bundles, leftovers, bundle_counter, stats

def _build_hierarchical_frag_map(master_df, col_cost_center, col_order_num, col_base_job, immune_stores):
    store_report, unclaimed = {}, {"orders": {}, "jobs": {}}
    if master_df.empty or 'Destination' not in master_df.columns: 
        return {"store_report_map": store_report, "unclaimed_report_map": unclaimed}

    EXEMPT_DESTS = {'PrintOnDemand', 'LargeFormat', 'Outsource', 'Apparel', 'Promo', 'Unknown', '25up layout'}
    
    # Filter: Exclude disqualified items using the flag
    if '__IS_DISQUALIFIED' in master_df.columns:
        analysis_df = master_df[master_df['__IS_DISQUALIFIED'] != True].copy()
    else:
        analysis_df = master_df.copy()

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

# ======================
# VALIDATION
# ======================
def validate_constitution(all_bundles, output_sheets, config, immune_stores):
    print("\n" + "="*50)
    print("CONSTITUTION VALIDATION (Smart Integrity Check)")
    print("="*50)
    
    col_cost = config.get('column_names', {}).get('cost_center')
    col_qty = config.get('column_names', {}).get('quantity_ordered')
    col_job = config.get('column_names', {}).get('job_ticket_number')
    col_base = config.get('column_names', {}).get('base_job_ticket_number')
    
    EXEMPT_CATEGORIES = set(config.get('bundling_rules', {}).get('exempt_categories', []))
    if not EXEMPT_CATEGORIES:
        EXEMPT_CATEGORIES = {'PrintOnDemand', 'LargeFormat', 'Outsource', 'Apparel', 'Promo', 'Unknown'}

    dq_config = config.get('bundling_rules', {}).get('disqualify_jobs_over_quantity', {})
    dq_enabled = dq_config.get('enabled', False)
    dq_threshold = dq_config.get('threshold', 1000) 
    
    print(f"  - Integrity Mode: {'Strict' if not dq_enabled else 'Smart (Whale Exemption Active)'}")
    print(f"  - Exempt Categories: {EXEMPT_CATEGORIES}")

    errors = []
    violation_details = []

    if CONSTITUTION['INTEGRITY_CHECKS']['VALIDATE_WHOLE_JOBS'] and col_cost:
        bundled_job_map = {} 
        
        for b_name, b_df in all_bundles.items():
            if not b_df.empty and col_cost in b_df.columns and col_base in b_df.columns:
                val_df = b_df[~b_df[col_job].astype(str).str.startswith('BLANK-')]
                for _, row in val_df.iterrows():
                    key = (row[col_cost], row[col_base])
                    bundled_job_map[key] = b_name

        for sheet_name, l_df in output_sheets.items():
            if sheet_name in all_bundles or sheet_name == 'exceptions' or l_df.empty: 
                continue
            
            is_sheet_exempt = sheet_name in EXEMPT_CATEGORIES
            if col_cost not in l_df.columns or col_base not in l_df.columns: continue
            
            for _, row in l_df.iterrows():
                if row.get('__IS_DISQUALIFIED') == True: continue # Skip Disqualified

                sid = row[col_cost]
                if sid in immune_stores: continue 

                row_cat = row.get('Category', '')
                if is_sheet_exempt or row_cat in EXEMPT_CATEGORIES: continue 
                
                item_qty = row.get(col_qty, 0)
                # Whale Exemption redundant if __IS_DISQUALIFIED is used, but safe to keep
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
             print("✓ Whole Job Law Passed.")

    if CONSTITUTION['YIELD_PROTECTION']['DISALLOW_RUNT_BUNDLES']:
        min_sz = CONSTITUTION['YIELD_PROTECTION']['MIN_BUNDLE_SIZE']
        runt_errors = []
        for n, b in all_bundles.items():
            total_qty = b[col_qty].sum()
            if total_qty < min_sz: runt_errors.append(f"Bundle '{n}' ({total_qty})")
        if runt_errors:
            errors.append(f"IRON LAW VIOLATION: Runt Bundles detected: {', '.join(runt_errors)}")
        else:
            print("✓ Min Bundle Law Passed.")

    if errors:
        print("\n!!! CONSTITUTIONAL FAILURE !!!")
        for e in errors: print(f"  - {e}")
        
        fail_mode = config.get('bundling_rules', {}).get('fail_on_fragmentation_violation', True)
        if fail_mode:
            return False, violation_details
        else:
            print("⚠️ WARNING: Fragmentation violations detected but ignored per configuration.")
            return True, violation_details
    
    print("✓ Constitution Passed.")
    return True, []

def validate_bundles(all_bundles, config):
    print("--- Mix Validation ---")
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
        if has_bb and has_bc: print(f"FATAL: Mix Error in {n}"); return False
    return True

# ======================
# MAIN
# ======================
def run_bundling_process(categorized_data_sheets, output_file, config):
    print("\n=== AUTO-BUNDLER (Big Rock Strategy) ===")
    col_names = config.get('column_names', {})
    if not all(k in col_names for k in ['order_number', 'job_ticket_number', 'quantity_ordered', 'cost_center', 'base_job_ticket_number']):
        print("FATAL: Missing columns."); return None, None

    bundling_rules = config.get('bundling_rules', {})
    cats_to_bundle = [k for k, v in bundling_rules.items() if isinstance(v, dict) and 'bundle_name_suffix' in v]
    
    history = load_run_history()
    base_name, bundle_ctr = history['monthly_pace_job_number'], history['last_used_gang_run_suffix'] + 1
    initial_ctr = bundle_ctr
    
    all_bundles, output_sheets, all_remainders = {}, {}, []
    master_tracking_list = []
    disqualified_indices = set() # Deprecated in logic, kept for safety
    
    store_origins = {}
    for sheet_name, df in categorized_data_sheets.items():
        if col_names['cost_center'] in df.columns:
            unique_stores = df[col_names['cost_center']].unique()
            for sid in unique_stores:
                if sid not in store_origins: store_origins[sid] = set()
                store_origins[sid].add(sheet_name)
    
    immune_stores = {sid for sid, origins in store_origins.items() if len(origins) > 1}
    print(f"  - Identified {len(immune_stores)} stores with 'Arrival Immunity' (Pre-fragmented).")

    if 'exceptions' in categorized_data_sheets: 
        df_exc = categorized_data_sheets.pop('exceptions')
        output_sheets['exceptions'] = df_exc

    for cat, df in categorized_data_sheets.items():
        if cat in cats_to_bundle:
            print(f"\n--- Processing {cat} ---")
            rules = bundling_rules.get(cat)
            
            dq_rule = bundling_rules.get('disqualify_jobs_over_quantity', {})
            if dq_rule.get('enabled') and cat in dq_rule.get('categories', []):
                 mask = df[col_names['quantity_ordered']] > dq_rule.get('threshold', 1000)
                 if mask.any():
                     dq_jobs = df.loc[mask, col_names['base_job_ticket_number']].unique()
                     dq_rows = df[df[col_names['base_job_ticket_number']].isin(dq_jobs)]
                     df = df[~df[col_names['base_job_ticket_number']].isin(dq_jobs)]
                     
                     # FLAGGING DISQUALIFIED ROWS
                     dq_df = dq_rows.copy()
                     dq_df['__IS_DISQUALIFIED'] = True
                     all_remainders.append(dq_df)
                     
                     print(f"  - Disqualified {len(dq_jobs)} jobs.")

            bundles, rem, bundle_ctr, _ = bundle_primary_entity_sequential(
                df, bundle_ctr, base_name, config, cat, rules, {}, col_names['cost_center'],
                bundling_rules.get('preferred_bundle_quantity', 6250),
                bundling_rules.get('bundle_search_thresholds', [6250]),
                {int(k): v for k,v in bundling_rules.get('filler_padding_map', {}).items()},
                master_tracking_list,
                disqualified_indices
            )
            all_bundles.update(bundles)
            if not rem.empty: all_remainders.append(rem)
        else:
            output_sheets[cat] = df
            df_copy = df.copy()
            df_copy['Destination'] = cat
            master_tracking_list.append(df_copy)

    if all_remainders:
        left_df = pd.concat([r.reindex(columns=list(set().union(*(x.columns for x in all_remainders)))) for r in all_remainders], ignore_index=True)
        map_route = {cat: r.get('leftover_sheet_name') for cat, r in bundling_rules.items() if isinstance(r, dict)}
        routed = pd.Series(False, index=left_df.index)
        for cat, sheet in map_route.items():
            mask = left_df['Category'] == cat if 'Category' in left_df.columns else pd.Series(False, index=left_df.index)
            if mask.any():
                df_rout = left_df[mask].copy()
                output_sheets[sheet] = pd.concat([output_sheets.get(sheet, pd.DataFrame()), df_rout], ignore_index=True)
                
                df_rout['Destination'] = sheet
                master_tracking_list.append(df_rout)
                routed |= mask
                
        fallback = bundling_rules.get('leftover_category_fallback', 'PrintOnDemand')
        if (~routed).any(): 
            df_fall = left_df[~routed].copy()
            output_sheets[fallback] = pd.concat([output_sheets.get(fallback, pd.DataFrame()), df_fall], ignore_index=True)
            df_fall['Destination'] = fallback
            master_tracking_list.append(df_fall)

    if not validate_bundles(all_bundles, config): return None, None
    
    is_valid_constit, violations = validate_constitution(all_bundles, output_sheets, config, immune_stores)
    if not is_valid_constit: return None, None 

    print("\n--- Generating Global Fragmentation Map ---")
    master_frag_df = pd.DataFrame()
    if master_tracking_list:
        master_frag_df = pd.concat(master_tracking_list, ignore_index=True, sort=False)
    
    all_frag_maps = _build_hierarchical_frag_map(
        master_frag_df, 
        col_names['cost_center'], 
        col_names['order_number'], 
        col_names['base_job_ticket_number'],
        immune_stores
    )

    print(f"\nSaving to {output_file}")
    with pd.ExcelWriter(output_file) as writer:
        cols = set()
        for d in list(output_sheets.values()) + list(all_bundles.values()): cols.update(d.columns)
        final_cols = sorted(list(cols))
        
        # Drop the temp flag before saving
        if '__IS_DISQUALIFIED' in final_cols: final_cols.remove('__IS_DISQUALIFIED')
        
        for n in sorted(all_bundles.keys()):
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
    cfg = load_config_from_path(config_path)
    try:
        dfs = pd.read_excel(input_path, sheet_name=None)
        for n, d in dfs.items(): 
            for c in ['order_number', 'job_ticket_number', 'product_id', 'sku']:
                if cfg['column_names'].get(c) in d.columns: 
                    d[cfg['column_names'][c]] = d[cfg['column_names'][c]].astype(str).replace('nan', '')

        out_path = os.path.join(output_dir, os.path.splitext(os.path.basename(input_path))[0].replace("_CATEGORIZED", "") + ".xlsx")
        res, fmap = run_bundling_process(dfs, out_path, cfg)
        if res and fmap:
            with open(out_path.replace(".xlsx", "_fragmap.json"), 'w') as f: json.dump(fmap, f, indent=4)
            print("✓ Success")
        else: raise Exception("Bundling Failed")
    except Exception as e:
        print(f"CRITICAL: {e}"); traceback.print_exc(); sys.exit(1)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3])