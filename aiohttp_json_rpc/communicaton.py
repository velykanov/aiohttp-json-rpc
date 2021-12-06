import asyncio
import json

from .protocol import encode_request, encode_notification


class JsonRpcRequest:
    def __init__(self, http_request, rpc, msg, json_package=json):
        self.http_request = http_request
        self.rpc = rpc
        self.msg = msg

        self._json_package = json_package

    @property
    def ws(self):
        return getattr(self.http_request, 'ws', None)

    @property
    def params(self):
        if 'params' not in self.msg.data:
            self.params = None

        return self.msg.data['params']

    @params.setter
    def params(self, value):
        self.msg.data['params'] = value

    @property
    def methods(self):
        return self.http_request.methods

    @methods.setter
    def methods(self, value):
        self.http_request.methods = value

    @property
    def topics(self):
        if not hasattr(self.http_request, 'topics'):
            self.http_request.topics = set()

        return self.http_request.topics

    @topics.setter
    def topics(self, value):
        self.http_request.topics = value

    @property
    def subscriptions(self):
        if not hasattr(self.http_request, 'subscriptions'):
            self.http_request.subscriptions = set()

        return self.http_request.subscriptions

    @subscriptions.setter
    def subscriptions(self, value):
        self.http_request.subscriptions = value

    async def call(self, method, params=None, timeout=None):
        msg_id = self.http_request.msg_id
        self.http_request.msg_id += 1
        self.http_request.pending[msg_id] = asyncio.Future()

        request = encode_request(
            method,
            id=msg_id,
            params=params,
            json_package=self._json_package,
        )

        await self.http_request.ws.send_str(request)

        if timeout:
            await asyncio.wait_for(
                self.http_request.pending[msg_id],
                timeout=timeout,
            )

        else:
            await self.http_request.pending[msg_id]

        result = self.http_request.pending[msg_id].result()
        del self.http_request.pending[msg_id]

        return result

    async def confirm(self, message='', timeout=None):
        return await self.call(
            'confirm',
            params={'message': message},
            timeout=timeout,
        )

    async def send_notification(self, method, params=None):
        await self.ws.send_str(
            encode_notification(method, params, json_package=self._json_package),
        )


class SyncJsonRpcRequest(JsonRpcRequest):
    def call(self, method, params=None, wait=True):
        return self.rpc.worker_pool.run_sync(
            super().call,
            method,
            params,
            wait=wait,
        )

    def send_notification(self, method, params=None, wait=True):
        self.rpc.worker_pool.run_sync(
            super().send_notification,
            method,
            params,
            wait=wait,
        )
