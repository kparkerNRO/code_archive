from typing import Any, Callable, Coroutine, Dict, Type, Union

from common.json import json_dumps_safe
from common.logging import getLogger
from fastapi import Request, Response, status
from fastapi.responses import JSONResponse

from agent_companion.errors import CallCenterUserNotFoundException, PotentialReportableEventNotFoundException

logger = getLogger(__name__)


class JSONErrorResponse(JSONResponse):
    def __init__(self, status_code: int, exc, **kwargs):
        message = exc.args[0] if len(exc.args) else None
        content = {"detail": {"message": message, "code": status_code}}
        logger.error("Error occurred", exc_info=exc)
        super().__init__(content=content, status_code=status_code, **kwargs)


def pre_not_found_error_handler(request: Request, exc: PotentialReportableEventNotFoundException) -> JSONErrorResponse:
    return JSONErrorResponse(status_code=status.HTTP_404_NOT_FOUND, exc=exc)


def call_center_user_not_found_error_handler(
    request: Request, exc: CallCenterUserNotFoundException
) -> JSONErrorResponse:
    return JSONErrorResponse(status_code=status.HTTP_404_NOT_FOUND, exc=exc)


async def uncaught_error_handler(request: Request, exc) -> JSONErrorResponse:
    try:
        logger.error(
            "Uncaught server exception",
            exc_info=exc,
            extra={
                "custom_dimensions": {
                    "query": json_dumps_safe(dict(request.query_params)),
                    "method": request.method,
                    "path": request.url.path,
                }
            },
        )
    except Exception:
        logger.error("Uncaught server error. Failed to add log extras.", exc_info=exc)
    return JSONErrorResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, exc=exc)


exception_handlers: Dict[
    Union[int, Type[Exception]],
    Callable[[Request, Any], Coroutine[Any, Any, Response]],
] = {
    500: uncaught_error_handler,  # if errors happen that don't match other handlers, this gets called
    PotentialReportableEventNotFoundException: pre_not_found_error_handler,  # type: ignore
    CallCenterUserNotFoundException: call_center_user_not_found_error_handler,  # type: ignore
}
