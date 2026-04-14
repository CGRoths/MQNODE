class MQNodeError(Exception):
    """Base error for MQNODE."""


class RpcError(MQNodeError):
    """Bitcoin RPC error."""


class ValidationError(MQNodeError):
    """Payload validation error."""
