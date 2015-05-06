from cockroach.proto import api_pb2

try:
    import enum  # >= py34
except ImportError:
    import enum34 as enum  # < py34


@enum.unique
class Methods(enum.Enum):
    """Constants defining RPC methods.

    Each has the following attributes:
    * name
    * is_write
    * request_type
    * response_type
    """
    def __init__(self, is_write, request_type, response_type):
        self.is_write = is_write
        self.request_type = request_type
        self.response_type = response_type

    # This list only includes the public methods from cockroach/proto/api.go
    # Note that capitalization of the names matters as it is used for the
    # RPC names on the wire.
    Contains = (False, api_pb2.ContainsRequest, api_pb2.ContainsResponse)
    Get = (False, api_pb2.GetRequest, api_pb2.GetResponse)
    Put = (True, api_pb2.PutRequest, api_pb2.PutResponse)
    ConditionalPut = (True, api_pb2.ConditionalPutRequest, api_pb2.ConditionalPutResponse)
    Increment = (True, api_pb2.IncrementRequest, api_pb2.IncrementResponse)
    Delete = (True, api_pb2.DeleteRequest, api_pb2.DeleteResponse)
    DeleteRange = (True, api_pb2.DeleteRangeRequest, api_pb2.DeleteRangeResponse)
    Scan = (False, api_pb2.ScanRequest, api_pb2.ScanResponse)
    EndTransaction = (True, api_pb2.EndTransactionRequest, api_pb2.EndTransactionResponse)
    Batch = (True, api_pb2.BatchRequest, api_pb2.BatchResponse)
    AdminSplit = (False, api_pb2.AdminSplitRequest, api_pb2.AdminSplitResponse)
