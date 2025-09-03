import asyncio
import time
from typing import Dict, List, Optional

class ChannelOperationStatus:
    def __init__(self):
        self.operations: Dict[str, dict] = {}
        self.lock = asyncio.Lock()

    async def start_operation(self, user_id: int, channel_id: str, operation_type: str) -> str:
        """Start tracking a new operation"""
        async with self.lock:
            op_key = f"{user_id}:{channel_id}:{operation_type}:{int(time.time())}"
            self.operations[op_key] = {
                "user_id": user_id,
                "channel_id": channel_id,
                "type": operation_type,
                "start_time": time.time(),
                "processed": 0,
                "downloaded": 0,
                "skipped": 0,
                "errors": 0,
                "status": "running"
            }
            return op_key

    async def update_operation(self, op_key: str, **kwargs):
        """Update operation progress"""
        async with self.lock:
            if op_key in self.operations:
                self.operations[op_key].update(kwargs)

    async def stop_operation(self, op_key: str):
        """Mark operation as completed"""
        async with self.lock:
            if op_key in self.operations:
                self.operations[op_key]["status"] = "completed"

    async def get_user_operations(self, user_id: int) -> List[dict]:
        """Get all active operations for a user"""
        async with self.lock:
            return [
                op for op in self.operations.values() 
                if op["user_id"] == user_id and op["status"] == "running"
            ]

    async def cleanup_completed(self):
        """Remove completed operations older than 1 hour"""
        async with self.lock:
            current_time = time.time()
            to_remove = []
            for op_key, op_data in self.operations.items():
                if (op_data["status"] == "completed" and 
                    current_time - op_data["start_time"] > 3600):
                    to_remove.append(op_key)
            
            for key in to_remove:
                del self.operations[key]

# Global status tracker
channel_status = ChannelOperationStatus()
