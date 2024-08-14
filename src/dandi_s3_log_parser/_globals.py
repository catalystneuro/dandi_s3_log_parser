import collections
import re

_KNOWN_OPERATION_TYPES = (
    "REST.GET.OBJECT",
    "REST.PUT.OBJECT",
    "REST.HEAD.OBJECT",
    "REST.POST.OBJECT",
    "REST.COPY.PART",
    "REST.COPY.OBJECT_GET",
    "REST.DELETE.OBJECT",
    "REST.OPTIONS.PREFLIGHT",
    "BATCH.DELETE.OBJECT",
    "WEBSITE.GET.OBJECT",
    "REST.GET.BUCKETVERSIONS",
    "REST.GET.BUCKET",
)

_IS_OPERATION_TYPE_KNOWN = collections.defaultdict(bool)
for request_type in _KNOWN_OPERATION_TYPES:
    _IS_OPERATION_TYPE_KNOWN[request_type] = True

_FULL_PATTERN_TO_FIELD_MAPPING = [
    "bucket_owner",
    "bucket",
    "timestamp",
    "ip_address",
    "requester",
    "request_id",
    "operation",
    "asset_id",
    "request_uri",
    # "http_version",  # Regex not splitting this from the request_uri...
    "status_code",
    "error_code",
    "bytes_sent",
    "object_size",
    "total_time",
    "turn_around_time",
    "referrer",
    "user_agent",
    "version",
    "host_id",
    "sigv",
    "cipher_suite",
    "auth_type",
    "endpoint",
    "tls_version",
    "access_point_arn",
]
_FullLogLine = collections.namedtuple("FullLogLine", _FULL_PATTERN_TO_FIELD_MAPPING)

_S3_LOG_REGEX = re.compile(pattern=r'"([^"]+)"|\[([^]]+)]|([^ ]+)')
