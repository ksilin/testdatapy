"""Reference pool for managing correlated IDs in test data generation."""
import random
import threading
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Callable


class ReferencePool:
    """Manages pools of IDs for generating correlated data.
    
    This class provides thread-safe access to reference IDs that can be used
    to create relationships between different entity types in test data.
    """
    
    def __init__(self):
        """Initialize an empty reference pool."""
        self._references: Dict[str, List[str]] = {}
        self._lock = threading.RLock()  # Use reentrant lock to prevent deadlock
        self._recent_items: Dict[str, deque] = {}
        self._recent_window_sizes: Dict[str, int] = {}
        self._stats_enabled = False
        self._stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "access_count": 0,
            "reference_count": 0,
            "last_access": None
        })
        
        self._indices: Dict[str, Dict[str, str]] = {}  # field_value -> reference_id
        self._record_cache: Dict[str, Dict[str, Any]] = {} 
    
    def is_empty(self) -> bool:
        """Check if the pool is empty."""
        with self._lock:
            return len(self._references) == 0
    
    def size(self) -> int:
        """Get total number of references across all types."""
        with self._lock:
            return sum(len(refs) for refs in self._references.values())
    
    def add_references(self, ref_type: str, references: List[str]) -> None:
        """Add references of a specific type to the pool.
        
        Args:
            ref_type: Type of references (e.g., 'customers', 'products')
            references: List of reference IDs to add
        """
        with self._lock:
            if ref_type not in self._references:
                self._references[ref_type] = []
            self._references[ref_type].extend(references)
            
            if self._stats_enabled:
                self._stats[ref_type]["reference_count"] = len(self._references[ref_type])
    
    def has_type(self, ref_type: str) -> bool:
        """Check if the pool has references of a specific type."""
        with self._lock:
            return ref_type in self._references and len(self._references[ref_type]) > 0
    
    def get_type_count(self, ref_type: str) -> int:
        """Get the number of references for a specific type."""
        with self._lock:
            return len(self._references.get(ref_type, []))
    
    def get_random(self, ref_type: str) -> str:
        """Get a random reference of a specific type.
        
        Args:
            ref_type: Type of reference to retrieve
            
        Returns:
            Random reference ID
            
        Raises:
            ValueError: If the reference type doesn't exist
        """
        with self._lock:
            if ref_type not in self._references or not self._references[ref_type]:
                raise ValueError(f"No references found for type: {ref_type}")
            
            if self._stats_enabled:
                self._stats[ref_type]["access_count"] += 1
                self._stats[ref_type]["last_access"] = threading.get_ident()
            
            return random.choice(self._references[ref_type])
    
    def get_random_multiple(self, ref_type: str, count: int) -> List[str]:
        """Get multiple unique random references.
        
        Args:
            ref_type: Type of references to retrieve
            count: Number of references to get
            
        Returns:
            List of unique random reference IDs
        """
        with self._lock:
            if ref_type not in self._references or not self._references[ref_type]:
                return []
            
            available = self._references[ref_type]
            sample_size = min(count, len(available))
            return random.sample(available, sample_size)
    
    def get_weighted_random(self, ref_type: str, weight_func: Callable[[str], float]) -> str:
        """Get a weighted random reference.
        
        Args:
            ref_type: Type of reference to retrieve
            weight_func: Function that returns weight for each reference ID
            
        Returns:
            Weighted random reference ID
        """
        with self._lock:
            if ref_type not in self._references or not self._references[ref_type]:
                raise ValueError(f"No references found for type: {ref_type}")
            
            if self._stats_enabled:
                self._stats[ref_type]["access_count"] += 1
                self._stats[ref_type]["last_access"] = threading.get_ident()
            
            references = self._references[ref_type]
            weights = [weight_func(ref) for ref in references]
            return random.choices(references, weights=weights)[0]
    
    def enable_recent_tracking(self, ref_type: str, window_size: int) -> None:
        """Enable tracking of recent items for a reference type.
        
        Args:
            ref_type: Type of reference to track
            window_size: Maximum number of recent items to keep
        """
        with self._lock:
            self._recent_items[ref_type] = deque(maxlen=window_size)
            self._recent_window_sizes[ref_type] = window_size
    
    def add_recent(self, ref_type: str, reference: str) -> None:
        """Add a reference to the recent items tracking.
        
        Args:
            ref_type: Type of reference
            reference: Reference ID to add
        """
        with self._lock:
            if ref_type in self._recent_items:
                self._recent_items[ref_type].append(reference)
    
    def get_recent(self, ref_type: str) -> List[str]:
        """Get list of recent items for a reference type.
        
        Args:
            ref_type: Type of reference
            
        Returns:
            List of recent reference IDs
        """
        with self._lock:
            if ref_type in self._recent_items:
                return list(self._recent_items[ref_type])
            return []
    
    def get_random_recent(self, ref_type: str, bias_recent: bool = True) -> str:
        """Get a random reference from recent items.
        
        Args:
            ref_type: Type of reference
            bias_recent: Whether to bias selection towards more recent items
            
        Returns:
            Random reference from recent items
        """
        with self._lock:
            recent = self.get_recent(ref_type)
            if not recent:
                raise ValueError(f"No recent items for type: {ref_type}")
            
            if self._stats_enabled:
                self._stats[ref_type]["access_count"] += 1
                self._stats[ref_type]["last_access"] = threading.get_ident()
            
            if bias_recent:
                # Weight more recent items higher
                weights = [i + 1 for i in range(len(recent))]
                return random.choices(recent, weights=weights)[0]
            else:
                return random.choice(recent)
    
    def clear_type(self, ref_type: str) -> None:
        """Clear all references of a specific type."""
        with self._lock:
            if ref_type in self._references:
                del self._references[ref_type]
            if ref_type in self._recent_items:
                del self._recent_items[ref_type]
            if ref_type in self._stats:
                del self._stats[ref_type]
    
    def clear_all(self) -> None:
        """Clear all references from the pool."""
        with self._lock:
            self._references.clear()
            self._recent_items.clear()
            self._stats.clear()
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize the pool state to a dictionary."""
        with self._lock:
            return {
                "references": dict(self._references),
                "recent_items": {k: list(v) for k, v in self._recent_items.items()},
                "recent_window_sizes": dict(self._recent_window_sizes),
                "stats_enabled": self._stats_enabled,
                "stats": dict(self._stats) if self._stats_enabled else None
            }
    
    @classmethod
    def from_dict(cls, state: Dict[str, Any]) -> 'ReferencePool':
        """Create a ReferencePool from a serialized state."""
        pool = cls()
        pool._references = state.get("references", {})
        pool._recent_window_sizes = state.get("recent_window_sizes", {})
        pool._stats_enabled = state.get("stats_enabled", False)
        
        # Restore recent items with proper deque
        for ref_type, items in state.get("recent_items", {}).items():
            window_size = pool._recent_window_sizes.get(ref_type, len(items))
            pool._recent_items[ref_type] = deque(items, maxlen=window_size)
        
        if pool._stats_enabled and state.get("stats"):
            pool._stats = defaultdict(lambda: {
                "access_count": 0,
                "reference_count": 0,
                "last_access": None
            }, state["stats"])
        
        return pool
    
    def validate_reference(self, ref_type: str, reference: str) -> bool:
        """Check if a reference exists in the pool."""
        with self._lock:
            return (ref_type in self._references and 
                    reference in self._references[ref_type])
    
    def enable_stats(self) -> None:
        """Enable statistics tracking."""
        self._stats_enabled = True
    
    def get_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get usage statistics."""
        with self._lock:
            return dict(self._stats)
    
    def add_field_index(self, ref_type: str, field_path: str, record_id: str, field_value: str) -> None:
        """Add field index for fast lookups by field value."""
        with self._lock:
            index_key = f"{ref_type}.{field_path}"
            if index_key not in self._indices:
                self._indices[index_key] = {}
            self._indices[index_key][str(field_value)] = record_id
    
    def find_by_field_value(self, ref_type: str, field_path: str, field_value: str) -> Optional[str]:
        """Find reference ID by field value using index."""
        with self._lock:
            index_key = f"{ref_type}.{field_path}"
            if index_key in self._indices:
                return self._indices[index_key].get(str(field_value))
            return None
    
    def get_random_batch(self, ref_type: str, count: int, avoid_recent: bool = False) -> List[str]:
        """Get batch of random references for better performance."""
        with self._lock:
            if ref_type not in self._references or not self._references[ref_type]:
                return []
            
            available = self._references[ref_type].copy()
            
            # Optionally avoid recent items to ensure variety
            if avoid_recent and ref_type in self._recent_items:
                recent = list(self._recent_items[ref_type])
                available = [ref for ref in available if ref not in recent]
            
            if not available:
                available = self._references[ref_type]
            
            sample_size = min(count, len(available))
            return random.sample(available, sample_size)
    
    def get_nested_field_value(self, ref_type: str, ref_id: str, field_path: str) -> Any:
        """Get nested field value from cached record."""
        with self._lock:
            if (ref_type in self._record_cache and 
                ref_id in self._record_cache[ref_type]):
                record = self._record_cache[ref_type][ref_id]
                return self._traverse_field_path(record, field_path)
            return None
    
    def _traverse_field_path(self, record: Dict[str, Any], field_path: str) -> Any:
        """Traverse nested field path in record."""
        parts = field_path.split(".")
        current = record
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        
        return current
    
    # TODO - this is far too specialized to be useful - remove or generalize
    def build_indices_for_entity(self, ref_type: str) -> None:
        """Build all indices for an entity type after bulk loading."""
        with self._lock:
            if ref_type not in self._record_cache:
                return
            
            # Common fields to index
            common_fields = [
                "full.Vehicle.cLicenseNr",
                "full.Vehicle.cLicenseNrCleaned", 
                "full.Customer.cKeyCustomer",
                "full.Job.cKeyJob",
                "jobid"
            ]
            
            for record_id, record in self._record_cache[ref_type].items():
                for field_path in common_fields:
                    field_value = self._traverse_field_path(record, field_path)
                    if field_value is not None:
                        self.add_field_index(ref_type, field_path, record_id, str(field_value))
    
    def get_memory_usage(self) -> Dict[str, int]:
        """Get memory usage statistics for optimization monitoring."""
        with self._lock:
            import sys
            
            usage = {
                "total_references": sum(len(refs) for refs in self._references.values()),
                "total_cached_records": sum(len(cache) for cache in self._record_cache.values()),
                "total_indices": sum(len(index) for index in self._indices.values()),
                "memory_estimate_mb": 0
            }
            
            # Rough memory estimation
            try:
                refs_size = sys.getsizeof(self._references)
                cache_size = sys.getsizeof(self._record_cache) 
                indices_size = sys.getsizeof(self._indices)
                usage["memory_estimate_mb"] = (refs_size + cache_size + indices_size) // (1024 * 1024)
            except:
                usage["memory_estimate_mb"] = -1  # Could not estimate
            
            return usage
