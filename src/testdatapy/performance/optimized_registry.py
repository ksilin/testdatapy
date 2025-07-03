"""Optimized function registry with improved performance characteristics.

This module provides an optimized version of the function registry
with better search performance and memory efficiency.
"""

import threading
from collections import defaultdict
from typing import Dict, Any, List, Optional, Set, Callable, Type
import re
import time

from ..transformers.function_registry import (
    FunctionRegistry, FunctionMetadata, FunctionCategory, 
    global_registry
)
from ..logging_config import get_schema_logger


class OptimizedFunctionRegistry(FunctionRegistry):
    """Optimized function registry with improved performance."""
    
    def __init__(self):
        """Initialize the optimized registry."""
        super().__init__()
        self._logger = get_schema_logger(__name__)
        
        # Performance optimizations
        self._search_cache: Dict[str, List[str]] = {}
        self._search_cache_lock = threading.RLock()
        self._search_cache_ttl = 300  # 5 minutes
        self._search_cache_timestamps: Dict[str, float] = {}
        
        # Indexing for fast lookups
        self._category_index: Dict[FunctionCategory, Set[str]] = defaultdict(set)
        self._tag_index: Dict[str, Set[str]] = defaultdict(set)
        self._name_fragments: Dict[str, Set[str]] = defaultdict(set)
        self._index_lock = threading.RLock()
        
        # Performance statistics
        self._stats = {
            "search_cache_hits": 0,
            "search_cache_misses": 0,
            "index_rebuilds": 0,
            "registrations": 0
        }
        
        self._rebuild_indexes()
        self._logger.info("OptimizedFunctionRegistry initialized")
    
    def register(self, name: str, func: Callable, description: str = "",
                category: FunctionCategory = FunctionCategory.CUSTOM,
                input_types: Optional[List[Type]] = None,
                output_type: Optional[Type] = None,
                tags: Optional[Set[str]] = None,
                is_safe: bool = True,
                overwrite: bool = False,
                **metadata_kwargs) -> bool:
        """Register a function with optimized indexing."""
        success = super().register(
            name, func, description, category, input_types, output_type,
            tags, is_safe, overwrite, **metadata_kwargs
        )
        
        if success:
            self._stats["registrations"] += 1
            self._update_indexes_for_function(name)
            self._invalidate_search_cache()
        
        return success
    
    def unregister(self, name: str) -> bool:
        """Unregister a function with index cleanup."""
        success = super().unregister(name)
        
        if success:
            self._remove_from_indexes(name)
            self._invalidate_search_cache()
        
        return success
    
    def _update_indexes_for_function(self, name: str) -> None:
        """Update indexes for a specific function."""
        with self._index_lock:
            if name not in self._functions:
                return
            
            metadata = self._functions[name]
            
            # Category index
            self._category_index[metadata.category].add(name)
            
            # Tag index
            for tag in metadata.tags:
                self._tag_index[tag].add(name)
            
            # Name fragment index for fast searching
            self._index_name_fragments(name)
    
    def _remove_from_indexes(self, name: str) -> None:
        """Remove function from all indexes."""
        with self._index_lock:
            # Remove from category index
            for category_set in self._category_index.values():
                category_set.discard(name)
            
            # Remove from tag index
            for tag_set in self._tag_index.values():
                tag_set.discard(name)
            
            # Remove from name fragment index
            for fragment_set in self._name_fragments.values():
                fragment_set.discard(name)
    
    def _rebuild_indexes(self) -> None:
        """Rebuild all indexes from current function data."""
        with self._index_lock:
            self._category_index.clear()
            self._tag_index.clear() 
            self._name_fragments.clear()
            
            for name in self._functions:
                self._update_indexes_for_function(name)
            
            self._stats["index_rebuilds"] += 1
            self._logger.debug("Indexes rebuilt", function_count=len(self._functions))
    
    def _index_name_fragments(self, name: str) -> None:
        """Index name fragments for fast searching."""
        # Split name into fragments
        fragments = set()
        
        # Split by common delimiters
        parts = re.split(r'[._\-]', name.lower())
        fragments.update(parts)
        
        # Add substrings of significant length
        for i in range(len(name)):
            for j in range(i + 2, min(i + 10, len(name) + 1)):
                fragment = name[i:j].lower()
                if len(fragment) >= 2:
                    fragments.add(fragment)
        
        # Index all fragments
        for fragment in fragments:
            self._name_fragments[fragment].add(name)
    
    def search_functions(self, query: str) -> List[str]:
        """Optimized function search with caching."""
        query_lower = query.lower().strip()
        
        # Check cache first
        with self._search_cache_lock:
            if query_lower in self._search_cache:
                timestamp = self._search_cache_timestamps.get(query_lower, 0)
                if time.time() - timestamp < self._search_cache_ttl:
                    self._stats["search_cache_hits"] += 1
                    return self._search_cache[query_lower].copy()
                else:
                    # Expired cache entry
                    del self._search_cache[query_lower]
                    del self._search_cache_timestamps[query_lower]
        
        self._stats["search_cache_misses"] += 1
        
        # Perform optimized search
        results = self._perform_optimized_search(query_lower)
        
        # Cache results
        with self._search_cache_lock:
            self._search_cache[query_lower] = results.copy()
            self._search_cache_timestamps[query_lower] = time.time()
        
        return results
    
    def _perform_optimized_search(self, query: str) -> List[str]:
        """Perform optimized search using indexes."""
        candidates = set()
        
        with self._index_lock:
            # Search by exact name match first
            if query in self._functions:
                candidates.add(query)
            
            # Search by name fragments
            for fragment, names in self._name_fragments.items():
                if query in fragment or fragment in query:
                    candidates.update(names)
            
            # Search in descriptions and metadata
            for name, metadata in self._functions.items():
                if query in metadata.description.lower():
                    candidates.add(name)
                
                # Search in tags
                for tag in metadata.tags:
                    if query in tag.lower():
                        candidates.add(name)
        
        # Score and sort results
        scored_results = []
        for name in candidates:
            score = self._calculate_search_score(name, query)
            scored_results.append((score, name))
        
        # Sort by score (higher is better) and return names
        scored_results.sort(reverse=True)
        return [name for score, name in scored_results]
    
    def _calculate_search_score(self, name: str, query: str) -> float:
        """Calculate search relevance score for a function name."""
        score = 0.0
        name_lower = name.lower()
        
        # Exact match gets highest score
        if name_lower == query:
            score += 100.0
        
        # Prefix match gets high score
        elif name_lower.startswith(query):
            score += 50.0
        
        # Contains match gets medium score
        elif query in name_lower:
            score += 25.0
        
        # Partial matches get lower scores
        else:
            # Count matching characters
            matching_chars = sum(1 for c in query if c in name_lower)
            score += (matching_chars / len(query)) * 10.0
        
        # Boost score for shorter names (more specific)
        if len(name) < 20:
            score += 5.0
        
        # Boost score for functions with common prefixes
        if name_lower.startswith(('get_', 'set_', 'is_', 'has_')):
            score += 2.0
        
        return score
    
    def get_functions_by_category(self, category: FunctionCategory) -> List[str]:
        """Get functions by category using optimized index."""
        with self._index_lock:
            return list(self._category_index.get(category, set()))
    
    def get_functions_by_tag(self, tag: str) -> List[str]:
        """Get functions by tag using optimized index."""
        with self._index_lock:
            return list(self._tag_index.get(tag.lower(), set()))
    
    def get_functions_by_tags(self, tags: List[str], match_all: bool = False) -> List[str]:
        """Get functions matching tags."""
        if not tags:
            return []
        
        with self._index_lock:
            tag_sets = [self._tag_index.get(tag.lower(), set()) for tag in tags]
            tag_sets = [s for s in tag_sets if s]  # Remove empty sets
            
            if not tag_sets:
                return []
            
            if match_all:
                # Intersection of all tag sets
                result_set = tag_sets[0]
                for tag_set in tag_sets[1:]:
                    result_set = result_set.intersection(tag_set)
            else:
                # Union of all tag sets
                result_set = set()
                for tag_set in tag_sets:
                    result_set = result_set.union(tag_set)
            
            return list(result_set)
    
    def _invalidate_search_cache(self) -> None:
        """Invalidate the search cache when functions change."""
        with self._search_cache_lock:
            self._search_cache.clear()
            self._search_cache_timestamps.clear()
    
    def get_optimization_statistics(self) -> Dict[str, Any]:
        """Get optimization statistics."""
        with self._search_cache_lock, self._index_lock:
            total_searches = self._stats["search_cache_hits"] + self._stats["search_cache_misses"]
            cache_hit_rate = (
                self._stats["search_cache_hits"] / total_searches
                if total_searches > 0 else 0.0
            )
            
            return {
                **self._stats,
                "search_cache_hit_rate": cache_hit_rate,
                "search_cache_size": len(self._search_cache),
                "category_index_size": {
                    cat.value: len(names) for cat, names in self._category_index.items()
                },
                "tag_index_size": len(self._tag_index),
                "name_fragment_index_size": len(self._name_fragments),
                "total_functions": len(self._functions)
            }
    
    def clear_caches(self) -> None:
        """Clear all optimization caches."""
        with self._search_cache_lock:
            self._search_cache.clear()
            self._search_cache_timestamps.clear()
        
        self._logger.info("Optimization caches cleared")
    
    def optimize(self) -> None:
        """Perform optimization maintenance."""
        # Rebuild indexes if needed
        if len(self._functions) > 1000 and self._stats["index_rebuilds"] == 1:
            self._rebuild_indexes()
        
        # Clean expired cache entries
        current_time = time.time()
        with self._search_cache_lock:
            expired_keys = [
                key for key, timestamp in self._search_cache_timestamps.items()
                if current_time - timestamp >= self._search_cache_ttl
            ]
            
            for key in expired_keys:
                del self._search_cache[key]
                del self._search_cache_timestamps[key]
        
        self._logger.debug("Optimization maintenance completed",
                         expired_cache_entries=len(expired_keys) if 'expired_keys' in locals() else 0)


def create_optimized_global_registry() -> OptimizedFunctionRegistry:
    """Create and populate an optimized global registry."""
    optimized_registry = OptimizedFunctionRegistry()
    
    # Copy existing functions from global registry
    for name in global_registry.list_functions():
        metadata = global_registry.get_function(name)
        if metadata:
            optimized_registry.register(
                name=name,
                func=metadata.func,
                description=metadata.description,
                category=metadata.category,
                input_types=metadata.input_types,
                output_type=metadata.output_type,
                tags=metadata.tags,
                is_safe=metadata.is_safe,
                overwrite=True
            )
    
    return optimized_registry