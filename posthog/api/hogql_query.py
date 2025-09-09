import re

from django.core.cache import cache

from pydantic import BaseModel
from rest_framework import status, viewsets
from rest_framework.exceptions import Throttled, ValidationError
from rest_framework.request import Request
from rest_framework.response import Response

from posthog.schema import HogQLQuery, QueryRequest, QueryResponseAlternative

from posthog.hogql.errors import ExposedHogQLError, ResolutionError

from posthog.api.documentation import extend_schema
from posthog.api.mixins import PydanticModelMixin
from posthog.api.query import _process_query_request
from posthog.api.routing import TeamAndOrgViewSetMixin
from posthog.api.services.query import process_query_model
from posthog.api.utils import action
from posthog.clickhouse.client.limit import ConcurrencyLimitExceeded
from posthog.clickhouse.query_tagging import get_query_tag_value, tag_queries
from posthog.constants import AvailableFeature
from posthog.errors import ExposedCHQueryError
from posthog.exceptions_capture import capture_exception
from posthog.hogql_queries.query_runner import ASYNC_MODES
from posthog.rate_limit import APIQueriesBurstThrottle, APIQueriesSustainedThrottle

from common.hogvm.python.utils import HogVMException


class HogQLQueryViewSet(TeamAndOrgViewSetMixin, PydanticModelMixin, viewsets.ViewSet):
    # NOTE: Do we need to override the scopes for the "create"
    scope_object = "hogql_query"
    # Special case for query - these are all essentially read actions
    scope_object_read_actions = ["retrieve", "create", "list", "destroy", "run"]
    scope_object_write_actions: list[str] = []

    def get_throttles(self):
        return [APIQueriesBurstThrottle(), APIQueriesSustainedThrottle()]

    def check_team_api_queries_concurrency(self):
        cache_key = f"team/{self.team_id}/feature/{AvailableFeature.API_QUERIES_CONCURRENCY}"
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        if self.team:
            new_val = self.team.organization.is_feature_available(AvailableFeature.API_QUERIES_CONCURRENCY)
            cache.set(cache_key, new_val)
            return new_val
        return False

    @extend_schema(
        request=QueryRequest,
        responses={
            200: QueryResponseAlternative,
        },
    )
    # @monitor(feature=Feature.QUERY, endpoint="run", method="POST")
    @action(methods=["POST"], detail=True, url_path="run")
    def run(self, request: Request, *args, **kwargs) -> Response:
        data = self.get_model(request.data, QueryRequest)

        # Only handle HogQLQuery
        if not isinstance(data.query, HogQLQuery):
            raise ValidationError("This endpoint only supports HogQLQuery")

        try:
            query, client_query_id, execution_mode = _process_query_request(
                data, self.team, data.client_query_id, request.user
            )
            self._tag_client_query_id(client_query_id)
            if execution_mode in ASYNC_MODES:
                raise ValidationError("only sync modes are supported (refresh param)")

            result = process_query_model(
                self.team,
                query,
                execution_mode=execution_mode,
                query_id=client_query_id,
                user=request.user,  # type: ignore[arg-type]
                is_query_service=(get_query_tag_value("access_method") == "personal_api_key"),
            )
            if isinstance(result, BaseModel):
                result = result.model_dump(by_alias=True)
            response_status = (
                status.HTTP_202_ACCEPTED
                if result.get("query_status") and result["query_status"].get("complete") is False
                else status.HTTP_200_OK
            )
            return Response(result, status=response_status)
        except (ExposedHogQLError, ExposedCHQueryError, HogVMException) as e:
            raise ValidationError(str(e), getattr(e, "code_name", None))
        except ResolutionError as e:
            raise ValidationError(str(e))
        except ConcurrencyLimitExceeded as c:
            raise Throttled(detail=str(c))
        except Exception as e:
            self.handle_column_ch_error(e)
            capture_exception(e)
            raise

    def handle_column_ch_error(self, error):
        if getattr(error, "message", None):
            match = re.search(r"There's no column.*in table", error.message)
            if match:
                # TODO: remove once we support all column types
                raise ValidationError(
                    match.group(0) + ". Note: While in beta, not all column types may be fully supported"
                )
        return

    def _tag_client_query_id(self, query_id: str | None):
        if query_id is None:
            return

        tag_queries(client_query_id=query_id)


MAX_QUERY_TIMEOUT = 600
