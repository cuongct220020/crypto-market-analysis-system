# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json
from typing import List
from pydantic import BaseModel


class ConsoleItemExporter(object):
    def __init__(self, entity_types: List[str] | None):
        # Convert entity types to set for faster lookup
        self.allowed_entity_types = set(entity_types) if entity_types else set()

    def open(self):
        pass

    def export_items(self, items):
        for item in items:
            # Only export if no specific entity types are set (export all) or if the item type is in allowed types
            if not self.allowed_entity_types:
                # No filtering - export all items
                self._export_item(item)
            else:
                # Check if item type is in allowed types
                item_type = self._get_item_type(item)
                if item_type in self.allowed_entity_types:
                    self._export_item(item)

    def _export_item(self, item):
        # Only export if no specific entity types are set (export all) or if the item type is in allowed types
        if not self.allowed_entity_types:
            # No filtering - export all items
            self._print_item(item)
        else:
            # Check if item type is in allowed types
            item_type = self._get_item_type(item)
            if item_type in self.allowed_entity_types:
                self._print_item(item)

    @staticmethod
    def _print_item(item):
        # Handle Pydantic models by converting to dict, otherwise serialize directly
        if isinstance(item, BaseModel):
            # Convert Pydantic model to dictionary with serialization settings
            item_dict = item.model_dump(mode='json', exclude_none=True)
            item_type = getattr(item, "type", "unknown")
            print(f"[{item_type.upper()}]: {json.dumps(item_dict, indent=2, default=str)}")
        else:
            # For non-Pydantic items, try regular JSON serialization first
            item_type = item.get("type", "unknown") if isinstance(item, dict) else "unknown"
            try:
                print(f"[{item_type.upper()}]: {json.dumps(item, indent=2, default=str)}")
            except TypeError:
                # If regular serialization fails, convert to string representation
                print(f"[{item_type.upper()}]: {str(item)}")

    @staticmethod
    def _get_item_type(item):
        """Extract item type from either Pydantic model or dict."""
        if isinstance(item, BaseModel):
            return getattr(item, "type", "unknown")
        elif isinstance(item, dict):
            return item.get("type", "unknown")
        else:
            return "unknown"

    def close(self):
        pass
