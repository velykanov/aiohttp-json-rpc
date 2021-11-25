from collections import namedtuple
import json

from .exceptions import (
    error_code_to_exception,
    RpcInvalidRequestError,
    RpcParseError,
    RpcError
)

JSONRPC = '2.0'
JsonRpcMsg = namedtuple('JsonRpcMsg', ['type', 'data'])


class JsonRpcMsgTyp:
    REQUEST = 10
    NOTIFICATION = 11
    RESPONSE = 20
    RESULT = 21
    ERROR = 22


def decode_msg(raw_msg):
    """
    Decodes jsonrpc 2.0 raw message objects into JsonRpcMsg objects.

    Examples:
        Request:
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "subtract",
                "params": [42, 23]
            }

        Batch request:
            [
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "subtract",
                    "params": [42, 23]
                },
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "subtract",
                    "params": [42, 17]
                }
            ]

        Notification:
            {
                "jsonrpc": "2.0",
                "method": "clock",
                "params": "12:00",
            }

        Response:
            {
                "jsonrpc": "2.0",
                "id": 1,
                "result": 0,
            }

        Error:
            {
                "jsonrpc": "2.0",
                "id": 1,
                "error": {
                    "code": -32600,
                    "message": "Invalid request",
                    "data": null
                }
            }
    """

    try:
        msg_data = json.loads(raw_msg)
    except ValueError:
        raise RpcParseError

    # TODO: inconvenient data conversion
    if isinstance(msg_data, list):
        return [decode_msg(json.dumps(msg_piece)) for msg_piece in msg_data]

    # check jsonrpc version
    if 'jsonrpc' not in msg_data or msg_data['jsonrpc'] != JSONRPC:
        raise RpcInvalidRequestError(msg_id=msg_data.get('id'))

    # check required fields
    if len({'error', 'result', 'method'} & set(msg_data)) != 1:
        raise RpcInvalidRequestError(msg_id=msg_data.get('id'))

    # find message type
    if 'method' in msg_data:
        if msg_data.get('id') is not None:
            msg_type = JsonRpcMsgTyp.REQUEST

        else:
            msg_type = JsonRpcMsgTyp.NOTIFICATION

    elif 'result' in msg_data:
        msg_type = JsonRpcMsgTyp.RESULT

    else:
        msg_type = JsonRpcMsgTyp.ERROR

    # Request Objects
    if msg_type in (JsonRpcMsgTyp.REQUEST, JsonRpcMsgTyp.NOTIFICATION):

        # 'method' fields have to be strings
        if not isinstance(msg_data['method'], str):
            raise RpcInvalidRequestError

        # set empty 'params' if not set
        if 'params' not in msg_data:
            msg_data['params'] = None

        # set empty 'id' if not set
        if 'id' not in msg_data:
            msg_data['id'] = None

    # Response Objects
    if msg_type in (JsonRpcMsgTyp.RESULT, JsonRpcMsgTyp.ERROR):

        # every Response object has to define an id
        if 'id' not in msg_data:
            raise RpcInvalidRequestError(msg_id=msg_data.get('id'))

    # Error objects
    if msg_type == JsonRpcMsgTyp.ERROR:

        # the error field has to be a dict
        if not isinstance(msg_data['error'], dict):
            raise RpcInvalidRequestError(msg_id=msg_data.get('id'))

        # the error field has to define 'code' and 'message'
        if len({'code', 'message'} & set(msg_data['error'])) != 2:
            raise RpcInvalidRequestError(msg_id=msg_data.get('id'))

        # the error code has to be in the specified ranges
        if msg_data['error']['code'] not in RpcError.lookup_table.keys():
            raise RpcInvalidRequestError(msg_id=msg_data.get('id'))

        # set empty 'data' field if not set
        if 'data' not in msg_data['error']:
            msg_data['error']['data'] = None

    return JsonRpcMsg(msg_type, msg_data)


def encode_request(method, id=None, params=None):
    if type(method) is not str:
        raise ValueError('method has to be a string')

    msg = {
        'jsonrpc': JSONRPC,
        'method': method,
    }

    if id is not None:
        msg['id'] = id

    if params is not None:
        msg['params'] = params

    return json.dumps(msg)


def encode_notification(method, params=None):
    return encode_request(method, id=None, params=params)


def encode_result(id, result):
    msg = {
        'jsonrpc': JSONRPC,
        'id': id,
        'result': result
    }

    return json.dumps(msg)


def encode_error(error, id=None):
    if not isinstance(error, RpcError):
        raise ValueError

    msg = {
        'jsonrpc': JSONRPC,
        'error': {
            'code': error.error_code,
            'message': error.message,
        }
    }

    if id is not None:
        msg['id'] = id

    elif error.msg_id is not None:
        msg['id'] = error.msg_id

    if error.data is not None:
        msg['error']['data'] = error.data

    return json.dumps(msg)


def decode_error(msg: JsonRpcMsg):
    error_code = msg.data['error']['code']
    exception = error_code_to_exception(error_code)

    return exception(
        msg_id=msg.data.get('id'),
        data=msg.data,
        error_code=error_code,
        message=msg.data['error'].get('message', ''),
    )
