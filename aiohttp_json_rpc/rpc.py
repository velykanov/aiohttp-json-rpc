from copy import copy
import asyncio
import aiohttp
import importlib
import logging
import inspect
import types

from .communicaton import JsonRpcRequest, SyncJsonRpcRequest
from .threading import ThreadedWorkerPool
from .auth import DummyAuthBackend

from .protocol import (
    encode_notification,
    JsonRpcMsgTyp,
    encode_result,
    encode_error,
    decode_msg,
)

from .exceptions import (
    RpcGenericServerDefinedError,
    RpcInvalidRequestError,
    RpcMethodNotFoundError,
    RpcInvalidParamsError,
    RpcInternalError,
    RpcError,
)


class JsonRpcMethod:
    CREDENTIAL_KEYS = ['request', 'worker_pool']

    def __init__(self, method):
        self.method = method

        # method introspection
        try:
            self.argspec = inspect.getfullargspec(method)
            self.introspected = True

        except TypeError:  # unsupported callable
            self.argspec = inspect.getfullargspec(lambda request: None)
            self.introspected = False

        self.defaults = copy(self.argspec.defaults)

        self.args = [
            i for i in self.argspec.args
            if i not in self.CREDENTIAL_KEYS + ['self']
        ]

        # required args
        self.required_args = copy(self.args)

        if self.defaults:
            self.required_args = [
                i for i in self.args[:-len(self.defaults or ())]
                if i not in self.CREDENTIAL_KEYS
            ]

        # optional args
        self.optional_args = [
            i for i in self.args[len(self.args) - len(self.defaults or ()):]
            if i not in self.CREDENTIAL_KEYS + ['self']
        ]

        # gen repr string
        args = []

        for i, v in enumerate(self.args[::-1]):
            if self.defaults and not i >= len(self.defaults):
                args.append('{}={}'.format(v, repr(self.defaults[i])))

            else:
                args.append(v)

        args = [
            *[i for i in self.CREDENTIAL_KEYS if i in self.argspec.args],
            *args[::-1],
        ]

        self._repr_str = 'JsonRpcMethod({}({}))'.format(
            self.method.__name__,
            ', '.join(args),
        )

    def __repr__(self):
        return self._repr_str

    async def __call__(self, http_request, rpc, msg):
        params = msg.data['params']
        method_params = {}

        # convert args
        if params is None:
            params = {}

        if not isinstance(params, (dict, list)):
            params = [params]

        if isinstance(params, list):
            params = {
                self.args[i]: v for i, v in enumerate(params)
                if i < len(self.args)
            }

        # required args
        for i in self.required_args:
            if i not in params:
                raise RpcInvalidParamsError(message='to few arguments')

            method_params[i] = params[i]

        # optional args
        for i, v in enumerate(self.optional_args):
            method_params[v] = params.get(v, self.defaults[i])

        # validators
        if hasattr(self.method, 'validators'):
            for arg_name, validator_list in self.method.validators.items():
                if not isinstance(validator_list, (list, tuple)):
                    validator_list = [validator_list]

                for validator in validator_list:
                    # TODO: this validation could be made based on annotations
                    if isinstance(validator, type):
                        if not isinstance(method_params[arg_name], validator):
                            raise RpcInvalidParamsError(message="'{}' has to be '{}'".format(arg_name, validator.__name__))  # NOQA

                    elif isinstance(validator, types.FunctionType):
                        if not validator(method_params[arg_name]):
                            raise RpcInvalidParamsError(message="'{}': validation error".format(arg_name))  # NOQA

        # credentials
        if 'request' in self.argspec.args:
            if asyncio.iscoroutinefunction(self.method):
                method_params['request'] = JsonRpcRequest(
                    rpc=rpc,
                    http_request=http_request,
                    msg=msg,
                )

            else:
                method_params['request'] = SyncJsonRpcRequest(
                    rpc=rpc,
                    http_request=http_request,
                    msg=msg,
                )

        if 'worker_pool' in self.argspec.args:
            method_params['worker_pool'] = rpc.worker_pool

        # run method
        if asyncio.iscoroutinefunction(self.method):
            return await self.method(**method_params)

        return await rpc.worker_pool.run(self.method, **method_params)


class JsonRpc(object):
    def __init__(
        self,
        loop=None,
        max_workers=0,
        auth_backend=None,
        logger=None,
    ):
        self.clients = []
        self.methods = {}
        self.topics = {}
        self.state = {}
        self.logger = logger or logging.getLogger('aiohttp-json-rpc.server')
        self.auth_backend = auth_backend or DummyAuthBackend()
        self.loop = loop or asyncio.get_event_loop()
        self.worker_pool = ThreadedWorkerPool(max_workers=max_workers)

        self.add_methods(
            ('', self.get_methods),
            ('', self.get_topics),
            ('', self.get_subscriptions),
            ('', self.subscribe),
            ('', self.unsubscribe),
        )

    def _add_method(self, method, name='', prefix='', separator='__'):
        if not callable(method):
            return

        name = name or getattr(method, '__name__', repr(method))

        if prefix:
            name = '{}{}{}'.format(prefix, separator, name)

        self.methods[name] = JsonRpcMethod(method)

    # TODO: `ignore` is useful but unused
    def _add_methods_from_object(self, obj, prefix='', ignore=None, separator='__'):
        if ignore is None:
            ignore = []

        for attr_name in dir(obj):
            if attr_name.startswith('_') or attr_name in ignore:
                continue

            self._add_method(getattr(obj, attr_name), prefix=prefix, separator=separator)

    def _add_methods_by_name(self, name, prefix='', separator='__'):
        try:
            module = importlib.import_module(name)
            self._add_methods_from_object(module, prefix=prefix, separator=separator)

        except ImportError:
            name = name.split('.')
            module = importlib.import_module('.'.join(name[:-1]))

            self._add_method(getattr(module, name[-1]), prefix=prefix, separator=separator)

    def add_methods(self, *methods, prefix='', separator='__'):
        for row in methods:
            if not (isinstance(row, tuple) and len(row) >= 2):
                raise ValueError('invalid format')

            prefix_ = prefix or row[0]
            if not isinstance(prefix, str):
                raise ValueError('prefix has to be str')

            method = row[1]

            if callable(method):
                name = row[2] if len(row) >= 3 else ''
                self._add_method(method, name=name, prefix=prefix_, separator=separator)

            elif type(method) == str:
                self._add_methods_by_name(method, prefix=prefix_, separator=separator)

            else:
                self._add_methods_from_object(method, prefix=prefix_, separator=separator)

    def add_topics(self, *topics):
        for topic in topics:
            if not isinstance(topic, (str, tuple)):
                raise ValueError('Topic has to be string or tuple')

            # find and apply decorators
            def func(request):
                return True

            # find name
            if isinstance(topic, str):
                name = topic
            else:
                name, *decorators = topic
                for decorator in decorators:
                    func = decorator(func)

            self.topics[name] = func

    def __call__(self, request):
        return self.handle_request(request)

    async def handle_request(self, request):
        # prepare request
        request.rpc = self
        coroutine = self.auth_backend.prepare_request(request)

        if asyncio.iscoroutine(coroutine):
            await coroutine

        # handle request
        if request.method == 'GET':
            # handle Websocket
            if request.headers.get('upgrade', '').lower() == 'websocket':
                return await self.handle_websocket_request(request)

            # handle GET
            return aiohttp.web.Response(status=405)

        # handle POST
        if request.method == 'POST':
            return aiohttp.web.Response(status=405)

    async def _ws_send_str(self, client, string):
        if client.ws._writer.transport.is_closing():
            self.clients.remove(client)
            await client.ws.close()

        await client.ws.send_str(string)

    async def _handle_rpc_msg(self, http_request, raw_msg):
        try:
            msg = decode_msg(raw_msg.data)
            self.logger.debug('message decoded: %s', msg)

        except RpcError as error:
            await self._ws_send_str(http_request, encode_error(error))

            return

        # TODO: make a single response encoder
        if isinstance(msg, list):
            response = await self._handle_batch_request(http_request, msg)
        else:
            response = await self._handle_single_request(http_request, msg)

        await self._ws_send_str(http_request, response)

    async def _handle_single_request(self, http_request, msg):
        if msg.type == JsonRpcMsgTyp.REQUEST:
            self.logger.debug('msg gets handled as request')

            # check if method is available
            if msg.data['method'] not in http_request.methods:
                self.logger.debug(
                    'method %s is unknown or restricted',
                    msg.data['method'],
                )

                return encode_error(RpcMethodNotFoundError(msg_id=msg.data.get('id')))

            # call method
            raw_response = getattr(
                http_request.methods[msg.data['method']].method,
                'raw_response',
                False,
            )

            try:
                result = await http_request.methods[msg.data['method']](
                    http_request=http_request,
                    rpc=self,
                    msg=msg,
                )

                if not raw_response:
                    result = encode_result(msg.data['id'], result)

                return result

            except (
                RpcGenericServerDefinedError,
                RpcInvalidRequestError,
                RpcInvalidParamsError,
            ) as error:
                return encode_error(error, id=msg.data.get('id'))

            except Exception as error:
                self.logger.error(error, exc_info=True)

                return encode_error(RpcInternalError(msg_id=msg.data.get('id')))

        # handle result
        elif msg.type == JsonRpcMsgTyp.RESULT:
            self.logger.debug('msg gets handled as result')

            http_request.pending[msg.data['id']].set_result(
                msg.data['result'],
            )

        else:
            self.logger.debug('unsupported msg type (%s)', msg.type)

            return encode_error(RpcInvalidRequestError(msg_id=msg.data.get('id')))

    # TODO: remove this ugly response builder
    async def _handle_batch_request(self, http_request, msgs):
        responses = [
            await self._handle_single_request(http_request, msg) for msg in msgs
        ]

        return '[{}]'.format(','.join(responses))

    async def handle_websocket_request(self, http_request):
        http_request.msg_id = 0
        http_request.pending = {}

        # prepare and register websocket
        ws = aiohttp.web_ws.WebSocketResponse()
        await ws.prepare(http_request)
        http_request.ws = ws
        self.clients.append(http_request)

        while not ws.closed:
            self.logger.debug('waiting for messages')
            raw_msg = await ws.receive()

            if not raw_msg.type == aiohttp.WSMsgType.TEXT:
                continue

            self.logger.debug('raw msg received: %s', raw_msg.data)
            self.loop.create_task(self._handle_rpc_msg(http_request, raw_msg))

        self.clients.remove(http_request)
        return ws

    async def get_methods(self, request):
        return list(request.methods.keys())

    async def get_topics(self, request):
        return list(request.topics)

    async def get_subscriptions(self, request):
        return list(request.subscriptions)

    async def subscribe(self, request):
        if not isinstance(request.params, list):
            request.params = [request.params]

        for topic in request.params:
            if topic and topic in request.topics:
                request.subscriptions.add(topic)

                if topic in self.state:
                    await request.send_notification(topic, self.state[topic])

        return list(request.subscriptions)

    async def unsubscribe(self, request):
        if not isinstance(request.params, list):
            request.params = [request.params]

        for topic in request.params:
            if topic and topic in request.subscriptions:
                request.subscriptions.remove(topic)

        return list(request.subscriptions)

    def filter(self, topics):
        if not isinstance(topics, list):
            topics = [topics]

        topics = set(topics)

        for client in self.clients:
            if client.ws.closed:
                continue

            if len(topics & client.subscriptions) > 0:
                yield client

    async def notify(self, topic, data=None, state=False):
        if not isinstance(topic, str):
            raise ValueError

        if state:
            self.state[topic] = data

        notification = encode_notification(topic, data)
        for client in self.filter(topic):
            try:
                await self._ws_send_str(client, notification)
            except Exception as e:
                self.logger.exception(e)
