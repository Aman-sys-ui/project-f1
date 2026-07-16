"""
Custom exceptions for the ingestion framework.
"""


class PipelineException(Exception):
    """Base exception for all pipeline errors."""


class ConfigurationException(PipelineException):
    """Raised when pipeline configuration is invalid."""


class SchemaException(PipelineException):
    """Raised when schema loading or validation fails."""


class ReadException(PipelineException):
    """Raised when source data cannot be read."""


class ValidationException(PipelineException):
    """Raised when data validation fails."""


class WriteException(PipelineException):
    """Raised when writing to the target fails."""


class StateException(PipelineException):
    """Raised when ingestion state cannot be updated."""


class AuditException(PipelineException):
    """Raised when audit logging or watermark updates fail."""