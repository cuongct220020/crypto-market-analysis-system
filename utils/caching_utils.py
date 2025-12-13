from typing import Set, List, TypeVar, Generic

T = TypeVar("T")

class InMemoryDedupStore(Generic[T]):
    """
    A simple in-memory deduplication store.
    Keeps track of seen items to filter out duplicates across multiple batches.
    """
    def __init__(self):
        self._seen_items: Set[T] = set()

    def filter_new_items(self, items: List[T]) -> List[T]:
        """
        Returns a list of items that have not been seen before.
        Adds new items to the store.
        """
        new_items = []
        for item in items:
            if item not in self._seen_items:
                new_items.append(item)
                self._seen_items.add(item)
        return new_items
    
    def add(self, item: T) -> None:
        self._seen_items.add(item)

    def clear(self) -> None:
        self._seen_items.clear()

    def __len__(self) -> int:
        return len(self._seen_items)