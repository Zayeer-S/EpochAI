# ruff: noqa: E501

from typing import Any, Dict, Tuple


def _get_param_values(func, args: Tuple, kwargs: Dict) -> str:
    import inspect

    try:
        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        params = []
        for name, value in bound_args.arguments.items():
            if name != "self":
                params.append(f"{name}={value!r}")

        return ", ".join(params)

    except Exception:
        params = []
        if len(args) > 1:
            params.extend([repr(arg) for arg in args[1:]])
        if kwargs:
            params.extend([f"{k}={v!r}" for k, v in kwargs.items()])
        return ", ".join(params)


def _get_logger(args: Tuple):
    """Tries to find a logger instance in the 'self' argument"""
    if args:
        obj = args[0]

        for logger_attr in ["_logger", "logger"]:
            if hasattr(obj, logger_attr):
                return getattr(obj, logger_attr)

    return None


def handle_initialization_errors(operation_name: str):
    """Decorator for initialization error handling"""

    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)

            except ImportError as import_error:
                raise RuntimeError(f"Failed to import required dependencies during {operation_name}: {import_error}") from import_error  # fmt: skip
            except ConnectionError as connection_error:
                raise RuntimeError(f"Failed to connect to database during {operation_name}: {connection_error}") from connection_error  # fmt: skip
            except FileNotFoundError as file_not_found_error:
                raise RuntimeError(f"Required configuration file not found during {operation_name}: {file_not_found_error}") from file_not_found_error  # fmt: skip
            except PermissionError as permission_error:
                raise RuntimeError(f"Insufficient permissions during {operation_name}: {permission_error}") from permission_error  # fmt: skip
            except Exception as general_error:
                raise RuntimeError(f"Failed {operation_name}: {general_error}") from general_error  # fmt: skip

        return wrapper

    return decorator


def handle_generic_errors_gracefully(operation_name: str, fallback_value: Any):
    """Decorator for graceful DAO operation error handling"""

    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)

            except Exception as general_error:
                param_values = _get_param_values(func, args, kwargs)

                if param_values:
                    error_msg = f"Error during {operation_name} in '{func.__name__}' (params: {param_values})): {general_error}"
                else:
                    error_msg = f"Error during {operation_name} in '{func.__name__}': {general_error}"

                logger = _get_logger(args)
                if logger:
                    logger.error(error_msg)
                else:
                    print(f"No logger found: {error_msg}")  # Fallback logging
                return fallback_value

        return wrapper

    return decorator
