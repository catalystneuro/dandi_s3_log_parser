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
    "BATCH.DELETE.OBJECT",
    "REST.COPY.OBJECT_GET",
    "REST.COPY.PART",
    "REST.DELETE.OBJECT",
    "REST.DELETE.OBJECT_TAGGING",
    "REST.DELETE.UPLOAD",
    "REST.GET.ACCELERATE",
    "REST.GET.ACL",
    "REST.GET.ANALYTICS",
    "REST.GET.BUCKET",
    "REST.GET.BUCKETPOLICY",
    "REST.GET.BUCKETVERSIONS",
    "REST.GET.CORS",
    "REST.GET.ENCRYPTION",
    "REST.GET.INTELLIGENT_TIERING",
    "REST.GET.INVENTORY",
    "REST.GET.LIFECYCLE",
    "REST.GET.LOCATION",
    "REST.GET.LOGGING_STATUS",
    "REST.GET.METRICS",
    "REST.GET.NOTIFICATION",
    "REST.GET.OBJECT",
    "REST.GET.OBJECT_LOCK_CONFIGURATION",
    "REST.GET.OBJECT_TAGGING",
    "REST.GET.OWNERSHIP_CONTROLS",
    "REST.GET.POLICY_STATUS",
    "REST.GET.PUBLIC_ACCESS_BLOCK",
    "REST.GET.REPLICATION",
    "REST.GET.REQUEST_PAYMENT",
    "REST.GET.TAGGING",
    "REST.GET.UPLOAD",
    "REST.GET.VERSIONING",
    "REST.GET.WEBSITE",
    "REST.HEAD.BUCKET",
    "REST.HEAD.OBJECT",
    "REST.OPTIONS.PREFLIGHT",
    "REST.POST.BUCKET",
    "REST.POST.MULTI_OBJECT_DELETE",
    "REST.POST.OBJECT",
    "REST.POST.UPLOAD",
    "REST.POST.UPLOADS",
    "REST.PUT.ACL",
    "REST.PUT.BUCKETPOLICY",
    "REST.PUT.OBJECT",
    "REST.PUT.OWNERSHIP_CONTROLS",
    "REST.PUT.PART",
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
