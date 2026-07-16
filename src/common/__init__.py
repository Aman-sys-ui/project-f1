"""
Common utilities and shared modules for the F1 Lakehouse Platform.

Modules:
    constants      — Centralized catalog/schema/table names and enums.
    exceptions     — Custom exception hierarchy.
    logger         — Shared logging factory (get_logger).
    models         — Shared dataclasses (EntityConfig, ValidationResult, etc.).
    config_loader  — Reads entity configuration from Unity Catalog.
    metadata       — UC infrastructure utilities (table_exists, etc.).
    retry          — Decorator-based retry with exponential backoff.
    utils          — Stateless utility functions (timestamps, string helpers).
"""
