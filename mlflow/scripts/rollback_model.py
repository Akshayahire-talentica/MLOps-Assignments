#!/usr/bin/env python3
"""
Model Rollback Automation
Task: Rollback automation (Owner: Aksay)
Input: Alert trigger / manual command
Output: Auto rollback to previous stable version
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import mlflow
from mlflow.tracking import MlflowClient


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ModelRollback:
    """
    Automates model rollback to previous stable version
    Triggered by alerts or manual intervention
    """
    
    def __init__(self, mlflow_uri: str = "http://localhost:5000"):
        """Initialize rollback manager"""
        self.mlflow_uri = mlflow_uri
        mlflow.set_tracking_uri(mlflow_uri)
        self.client = MlflowClient(tracking_uri=mlflow_uri)
        self.audit_log_path = Path("reports/audit")
        self.audit_log_path.mkdir(parents=True, exist_ok=True)
    
    def get_production_model(self, model_name: str) -> Optional[Dict]:
        """Get currently deployed production model"""
        try:
            versions = self.client.search_model_versions(f"name='{model_name}'")
            
            for version in versions:
                if version.current_stage == "Production":
                    return {
                        'name': model_name,
                        'version': version.version,
                        'stage': version.current_stage,
                        'run_id': version.run_id,
                        'created_at': version.creation_timestamp
                    }
            
            logger.warning(f"No production model found for '{model_name}'")
            return None
            
        except Exception as e:
            logger.error(f"Error getting production model: {e}")
            return None
    
    def get_previous_production_model(self, model_name: str, 
                                     exclude_version: str) -> Optional[Dict]:
        """Get the previous production model (before current one)"""
        try:
            versions = self.client.search_model_versions(f"name='{model_name}'")
            
            # Sort by version (descending)
            sorted_versions = sorted(
                [v for v in versions if v.version != exclude_version],
                key=lambda x: int(x.version),
                reverse=True
            )
            
            # Find the most recent archived/staging version
            for version in sorted_versions:
                if version.current_stage in ["Staging", "Archived"]:
                    return {
                        'name': model_name,
                        'version': version.version,
                        'stage': version.current_stage,
                        'run_id': version.run_id,
                        'created_at': version.creation_timestamp
                    }
            
            logger.warning("No previous stable version found")
            return None
            
        except Exception as e:
            logger.error(f"Error finding previous model: {e}")
            return None
    
    def transition_model_stage(self, model_name: str, version: str, 
                              new_stage: str) -> bool:
        """Transition model to a new stage"""
        try:
            self.client.transition_model_version_stage(
                name=model_name,
                version=version,
                stage=new_stage,
                archive_existing_versions=True
            )
            logger.info(f"[OK] Model {model_name} v{version} → {new_stage}")
            return True
        except Exception as e:
            logger.error(f"[FAILED] Stage transition error: {e}")
            return False
    
    def rollback_model(self, model_name: str, reason: str = "manual", 
                      triggered_by: str = "operator") -> Dict:
        """
        Execute model rollback to previous stable version
        
        Args:
            model_name: Name of model to rollback
            reason: Reason for rollback (drift, performance, manual, etc.)
            triggered_by: Who/what triggered rollback
        
        Returns:
            Rollback result dictionary
        """
        logger.info("="*70)
        logger.info("MODEL ROLLBACK INITIATED")
        logger.info("="*70)
        
        timestamp = datetime.now().isoformat()
        
        rollback_result = {
            'timestamp': timestamp,
            'model_name': model_name,
            'reason': reason,
            'triggered_by': triggered_by,
            'success': False,
            'current_model': None,
            'rollback_model': None,
            'actions_taken': []
        }
        
        # Step 1: Get current production model
        logger.info(f"[1/5] Getting current production model: {model_name}")
        current_model = self.get_production_model(model_name)
        
        if not current_model:
            rollback_result['error'] = "No production model found"
            logger.error("[FAILED] No production model to rollback from")
            return rollback_result
        
        rollback_result['current_model'] = current_model
        logger.info(f"[OK] Current model: v{current_model['version']}")
        
        # Step 2: Get previous stable model
        logger.info("[2/5] Finding previous stable version...")
        previous_model = self.get_previous_production_model(
            model_name, 
            exclude_version=current_model['version']
        )
        
        if not previous_model:
            rollback_result['error'] = "No previous stable version found"
            logger.error("[FAILED] Cannot rollback - no previous version")
            return rollback_result
        
        rollback_result['rollback_model'] = previous_model
        logger.info(f"[OK] Previous stable model: v{previous_model['version']}")
        
        # Step 3: Archive current production model
        logger.info("[3/5] Archiving current model...")
        if self.transition_model_stage(model_name, current_model['version'], "Archived"):
            rollback_result['actions_taken'].append(
                f"Archived model v{current_model['version']}"
            )
        else:
            rollback_result['error'] = "Failed to archive current model"
            return rollback_result
        
        # Step 4: Promote previous model to production
        logger.info("[4/5] Promoting previous model to production...")
        if self.transition_model_stage(model_name, previous_model['version'], "Production"):
            rollback_result['actions_taken'].append(
                f"Promoted model v{previous_model['version']} to Production"
            )
        else:
            rollback_result['error'] = "Failed to promote previous model"
            # Try to restore current model
            self.transition_model_stage(model_name, current_model['version'], "Production")
            return rollback_result
        
        # Step 5: Restart services (Docker)
        logger.info("[5/5] Restarting services...")
        restart_success = self.restart_docker_services()
        
        if restart_success:
            rollback_result['actions_taken'].append("Restarted Docker services")
            rollback_result['success'] = True
            logger.info("[OK] Rollback completed successfully")
        else:
            rollback_result['warning'] = "Rollback completed but service restart failed"
            rollback_result['success'] = True
            logger.warning("[WARNING] Rollback done but services may need manual restart")
        
        # Save audit log
        self.log_rollback(rollback_result)
        
        logger.info("="*70)
        logger.info("ROLLBACK SUMMARY")
        logger.info("="*70)
        logger.info(f"Model: {model_name}")
        logger.info(f"Rolled back FROM: v{current_model['version']}")
        logger.info(f"Rolled back TO: v{previous_model['version']}")
        logger.info(f"Reason: {reason}")
        logger.info(f"Triggered by: {triggered_by}")
        logger.info("="*70)
        
        return rollback_result
    
    def restart_docker_services(self) -> bool:
        """Restart Docker services to load new model"""
        try:
            logger.info("Restarting Docker services...")
            
            # Check if docker-compose.yml exists
            if not Path("docker-compose.yml").exists():
                logger.warning("docker-compose.yml not found, skipping restart")
                return True
            
            # Restart services
            result = subprocess.run(
                ["docker", "compose", "restart", "api-v1", "model-v2", "canary-router"],
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                logger.info("[OK] Services restarted")
                return True
            else:
                logger.error(f"[FAILED] Restart failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("[FAILED] Service restart timed out")
            return False
        except Exception as e:
            logger.error(f"[FAILED] Restart error: {e}")
            return False
    
    def log_rollback(self, rollback_result: Dict) -> None:
        """Log rollback event to audit trail"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        audit_file = self.audit_log_path / f"rollback_{timestamp}.json"
        
        with open(audit_file, 'w') as f:
            json.dump(rollback_result, f, indent=2)
        
        logger.info(f"[OK] Audit log saved: {audit_file}")
        
        # Update latest rollback
        latest_file = self.audit_log_path / "rollback_latest.json"
        with open(latest_file, 'w') as f:
            json.dump(rollback_result, f, indent=2)
    
    def get_rollback_history(self, limit: int = 10) -> list:
        """Get history of rollback events"""
        audit_files = sorted(
            self.audit_log_path.glob("rollback_*.json"),
            reverse=True
        )
        
        history = []
        for audit_file in audit_files[:limit]:
            if audit_file.name == "rollback_latest.json":
                continue
            
            with open(audit_file, 'r') as f:
                history.append(json.load(f))
        
        return history


def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description="Model Rollback Automation")
    parser.add_argument(
        "--model", 
        type=str, 
        default="nmf_recommendation",
        help="Model name to rollback"
    )
    parser.add_argument(
        "--reason",
        type=str,
        default="manual",
        choices=["drift", "performance", "error", "manual"],
        help="Reason for rollback"
    )
    parser.add_argument(
        "--triggered-by",
        type=str,
        default="operator",
        help="Who/what triggered the rollback"
    )
    parser.add_argument(
        "--history",
        action="store_true",
        help="Show rollback history"
    )
    
    args = parser.parse_args()
    
    rollback_manager = ModelRollback()
    
    if args.history:
        # Show history
        history = rollback_manager.get_rollback_history()
        
        print("\n" + "="*70)
        print("ROLLBACK HISTORY")
        print("="*70)
        
        if not history:
            print("No rollback history found")
        else:
            for event in history:
                print(f"\nTimestamp: {event['timestamp']}")
                print(f"Model: {event['model_name']}")
                print(f"Reason: {event['reason']}")
                print(f"Success: {event['success']}")
                if event.get('current_model'):
                    print(f"From: v{event['current_model']['version']}")
                if event.get('rollback_model'):
                    print(f"To: v{event['rollback_model']['version']}")
                print("-"*70)
        
        return 0
    
    # Execute rollback
    result = rollback_manager.rollback_model(
        model_name=args.model,
        reason=args.reason,
        triggered_by=args.triggered_by
    )
    
    return 0 if result['success'] else 1


if __name__ == "__main__":
    sys.exit(main())
