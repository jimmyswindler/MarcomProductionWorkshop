import os
import sys
import time
import json
import logging
import traceback

# Add project root to path for shared libs
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

from shared_lib.database import get_db_connection

class PipelineObserver:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(PipelineObserver, cls).__new__(cls)
            cls._instance.initialized = False
        return cls._instance

    def __init__(self, run_name=None):
        if self.initialized:
            return
            
        self.run_name = run_name or "DB_INPUT_RUN"
        self.db_id = None
        self.last_db_write = 0
        self.debounce_seconds = 1.5
        
        # State tracking
        self.state = {
            "stage_1_runlist_status": {"total": 0, "completed": 0, "status": "PENDING", "start_time": None, "end_time": None},
            "stage_2_assets_status": {"total": 0, "completed": 0, "status": "PENDING", "start_time": None, "end_time": None},
            "stage_3_tickets_status": {"total": 0, "completed": 0, "status": "PENDING", "start_time": None, "end_time": None},
            "stage_4_press_files_status": {"total": 0, "completed": 0, "status": "PENDING", "start_time": None, "end_time": None},
            "stage_5_imposition_gang_status": {"total": 0, "completed": 0, "status": "PENDING", "start_time": None, "end_time": None},
            "stage_6_imposition_single_status": {"total": 0, "completed": 0, "status": "PENDING", "start_time": None, "end_time": None}
        }
        
        self.dirty_keys = set()
        self.initialized = True
        self._hydrate_from_db()
        
    def _hydrate_from_db(self):
        conn = self._get_conn()
        if not conn: return
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT id, stage_1_runlist_status, stage_2_assets_status, stage_3_tickets_status, stage_4_press_files_status, stage_5_imposition_gang_status, stage_6_imposition_single_status FROM pipeline_progress_state WHERE run_name = %s ORDER BY id DESC LIMIT 1", (self.run_name,))
                res = cur.fetchone()
                if res:
                    self.db_id = res[0]
                    keys = [
                        "stage_1_runlist_status", "stage_2_assets_status", "stage_3_tickets_status", 
                        "stage_4_press_files_status", "stage_5_imposition_gang_status", "stage_6_imposition_single_status"
                    ]
                    for idx, key in enumerate(keys):
                        if res[idx+1]:
                            val = res[idx+1]
                            if isinstance(val, str): val = json.loads(val)
                            self.state[key] = val
        except Exception as e: pass
        finally: conn.close()
        
    def _get_conn(self):
        return get_db_connection()

    def _flush_to_db(self, force=False):
        now = time.time()
        if not force and (now - self.last_db_write < self.debounce_seconds):
            return
            
        conn = self._get_conn()
        if not conn:
            return
            
        try:
            with conn.cursor() as cur:
                if not self.db_id:
                    cur.execute("""
                        INSERT INTO pipeline_progress_state 
                        (run_name, is_active, status, started_at) 
                        VALUES (%s, TRUE, 'RUNNING', NOW()) 
                        RETURNING id;
                    """, (self.run_name,))
                    res = cur.fetchone()
                    if res:
                        self.db_id = res[0]
                
                if self.db_id and self.dirty_keys:
                    col_updates = []
                    params = []
                    for k in self.dirty_keys:
                        col_updates.append(f"{k} = %s")
                        params.append(json.dumps(self.state[k]))
                    
                    if col_updates:
                        params.append(self.db_id)
                        query = f"UPDATE pipeline_progress_state SET {', '.join(col_updates)}, last_updated_at = NOW() WHERE id = %s"
                        cur.execute(query, tuple(params))
                        self.dirty_keys.clear()
            
            conn.commit()
            self.last_db_write = now
        except Exception as e:
            logging.error(f"PipelineObserver _flush_to_db error: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def start_run(self, run_name):
        self.run_name = run_name
        # Mark previous active runs as inactive
        conn = self._get_conn()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("UPDATE pipeline_progress_state SET is_active = FALSE WHERE is_active = TRUE AND run_name != %s", (run_name,))
                conn.commit()
            except Exception as e:
                logging.error(f"PipelineObserver start_run error: {e}")
                if conn:
                    conn.rollback()
            finally:
                if conn:
                    conn.close()
                    
        # Reset all counters unconditionally to 0 / PENDING
        for k, v in self.state.items():
            if isinstance(v, dict):
                v["total"] = 0
                v["completed"] = 0
                v["status"] = "PENDING"
                v["start_time"] = None
                v["end_time"] = None
                
                # Reset any polar batch fragments
                if "details" in v:
                    v["details"] = {}
                        
            self.dirty_keys.add(k)
        
        # Set db_id to None to FORCE an INSERT instead of an UPDATE
        self.db_id = None
        self._flush_to_db(force=True)

    def finish_run(self, status="COMPLETED"):
        conn = self._get_conn()
        if conn and self.db_id:
            try:
                with conn.cursor() as cur:
                    cur.execute("UPDATE pipeline_progress_state SET status = %s, ended_at = NOW(), is_active = FALSE WHERE id = %s", (status, self.db_id))
                conn.commit()
            except Exception as e:
                if conn:
                    conn.rollback()
            finally:
                if conn:
                    conn.close()
                    
        self._flush_to_db(force=True)

    def start_stage(self, stage_key, total=1, details=None):
        if stage_key in self.state:
            self.state[stage_key]["total"] = total
            self.state[stage_key]["completed"] = 0
            self.state[stage_key]["status"] = "RUNNING"
            self.state[stage_key]["start_time"] = time.time()
            if details is not None:
                self.state[stage_key]["details"] = details
            self.dirty_keys.add(stage_key)
            self._flush_to_db(force=True)

    def update_stage(self, stage_key, completed=None, increment=1, details=None):
        if stage_key in self.state:
            if completed is not None:
                self.state[stage_key]["completed"] = completed
            else:
                self.state[stage_key]["completed"] += increment
            if details is not None:
                if "details" not in self.state[stage_key]:
                    self.state[stage_key]["details"] = {}
                self.state[stage_key]["details"].update(details)
            self.dirty_keys.add(stage_key)
            self._flush_to_db(force=False)

    def reload_stage_from_db(self, stage_key):
        """ Reloads specific stage state from DB to avoid overwriting subprocess changes """
        conn = self._get_conn()
        if not conn: return
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT {stage_key} FROM pipeline_progress_state WHERE run_name = %s ORDER BY id DESC LIMIT 1", (self.run_name,))
                res = cur.fetchone()
                if res and res[0]:
                    val = res[0]
                    if isinstance(val, str): val = json.loads(val)
                    self.state[stage_key] = val
        except Exception: pass
        finally: conn.close()
        
    def update_batch_progress(self, stage_key, batch_name, pct=None, increment=None):
        if stage_key in self.state:
            stage = self.state[stage_key]
            if "details" in stage and "batches" in stage["details"] and batch_name in stage["details"]["batches"]:
                if pct is not None:
                    stage["details"]["batches"][batch_name]["pct"] = pct
                elif increment is not None:
                    stage["details"]["batches"][batch_name]["pct"] += increment
                self.dirty_keys.add(stage_key)
                # IMPORTANT: force=True to ensure subprocesses save without dropping
                self._flush_to_db(force=True)

    def finish_stage(self, stage_key):
        if stage_key in self.state:
            stage = self.state[stage_key]
            stage["completed"] = stage["total"]
            stage["status"] = "COMPLETED"
            stage["end_time"] = time.time()
            self.dirty_keys.add(stage_key)
            self._flush_to_db(force=True)

    def error_stage(self, stage_key):
        if stage_key in self.state:
            stage = self.state[stage_key]
            stage["status"] = "ERROR"
            stage["end_time"] = time.time()
            self.dirty_keys.add(stage_key)
            self._flush_to_db(force=True)
            self.finish_run(status="FAILED")

    def check_cancellation(self):
        conn = self._get_conn()
        if not conn or not self.db_id:
            return False
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT status FROM pipeline_progress_state WHERE id = %s", (self.db_id,))
                res = cur.fetchone()
                if res and res[0] == 'CANCELED':
                    return True
        except Exception as e:
            logging.error(f"PipelineObserver check_cancellation error: {e}")
        finally:
            if conn:
                conn.close()
        return False

# Global singleton accessor
def get_observer(run_name=None):
    return PipelineObserver(run_name)
