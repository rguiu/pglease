"""Heartbeat management for keeping leases alive."""

import logging
import threading
import time
from typing import Dict, Optional

from .backend import Backend
from .exceptions import HeartbeatError

logger = logging.getLogger(__name__)


class HeartbeatManager:
    """
    Manages background heartbeat threads for active leases.
    
    Each task gets its own heartbeat thread that periodically renews
    the lease until stopped.
    """
    
    def __init__(self, backend: Backend, interval: int = 10):
        """
        Initialize heartbeat manager.
        
        Args:
            backend: Backend to send heartbeats through
            interval: Seconds between heartbeats (default: 10)
        """
        self.backend = backend
        self.interval = interval
        self._threads: Dict[str, threading.Thread] = {}
        self._stop_events: Dict[str, threading.Event] = {}
        self._lock = threading.Lock()
    
    def start(self, task_name: str, owner_id: str, ttl: int) -> None:
        """
        Start heartbeat thread for a task.
        
        Args:
            task_name: Task to send heartbeats for
            owner_id: Owner of the lease
            ttl: TTL to renew with each heartbeat
        """
        with self._lock:
            # Stop existing heartbeat if running
            if task_name in self._threads:
                self.stop(task_name)
            
            # Create stop event
            stop_event = threading.Event()
            self._stop_events[task_name] = stop_event
            
            # Create and start thread
            thread = threading.Thread(
                target=self._heartbeat_loop,
                args=(task_name, owner_id, ttl, stop_event),
                daemon=True,
                name=f"pglease-heartbeat-{task_name}",
            )
            self._threads[task_name] = thread
            thread.start()
            
            logger.debug(f"Started heartbeat for {task_name}")
    
    def stop(self, task_name: str) -> None:
        """
        Stop heartbeat thread for a task.
        
        Args:
            task_name: Task to stop heartbeat for
        """
        with self._lock:
            if task_name not in self._threads:
                return
            
            # Signal thread to stop
            stop_event = self._stop_events.get(task_name)
            if stop_event:
                stop_event.set()
            
            # Wait for thread to finish
            thread = self._threads.get(task_name)
            if thread and thread.is_alive():
                thread.join(timeout=1.0)
            
            # Clean up
            self._threads.pop(task_name, None)
            self._stop_events.pop(task_name, None)
            
            logger.debug(f"Stopped heartbeat for {task_name}")
    
    def stop_all(self) -> None:
        """Stop all heartbeat threads."""
        with self._lock:
            task_names = list(self._threads.keys())
        
        for task_name in task_names:
            self.stop(task_name)
    
    def _heartbeat_loop(
        self,
        task_name: str,
        owner_id: str,
        ttl: int,
        stop_event: threading.Event,
    ) -> None:
        """
        Background loop that sends periodic heartbeats.
        
        Runs until stop_event is set or heartbeat fails.
        """
        logger.info(f"Heartbeat loop started for {task_name} (interval={self.interval}s)")
        
        while not stop_event.is_set():
            # Wait for interval or until stopped
            if stop_event.wait(timeout=self.interval):
                break
            
            # Send heartbeat
            try:
                success = self.backend.heartbeat(task_name, owner_id, ttl)
                
                if not success:
                    logger.error(
                        f"Heartbeat failed for {task_name} - lease may have been lost"
                    )
                    break
            
            except Exception as e:
                logger.error(f"Heartbeat error for {task_name}: {e}")
                break
        
        logger.info(f"Heartbeat loop stopped for {task_name}")
