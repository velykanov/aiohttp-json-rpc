RPC = (host) => {
    this._host = host;
    this._message_id = 1;
    this._pending = {};
    this._topic_handler = {};
    this._onopen_handler = [];
    this._onclose_handler = [];
    this.methods = {};
    this.DEBUG = false;

    this._handle_request = (data) => {
        if (data.method in this.methods) {
            let result = this.methods[data.method](data.params);
            let response = {
                jsonrpc: '2.0',
                id: data.id,
                result: result,
            }

            this._ws.send(JSON.stringify(response));
        }
    }

    this.connect = () => {
        this._ws = new WebSocket(this._host);
        this._ws._rpc = this;

        // onopen
        this._ws.onopen = (event) => {
            for (let i in this._rpc._onopen_handler) {
                this._rpc._onopen_handler[i](this._rpc);
            }
        };

        // onclose
        this._ws.onclose = (event) => {
            for (let i in this._rpc._onclose_handler) {
                this._rpc._onclose_handler[i](this._rpc);
            }
        };

        // onmessage
        this._ws.onmessage = (event) => {
            let data = JSON.parse(event.data);
            if (this._rpc.DEBUG) {
                console.log('RPC <<', data);
            }

            if ('method' in data) {
                if ('id' in data && data.id != null) {  // request
                    this._rpc._handle_request(data);
                } else {  // notification
                    if (data.method in this._rpc._topic_handler) {
                        this._rpc._topic_handler[data.method](data.params);
                    }
                }

            } else {
                if ('id' in data && data.id in this._rpc._pending) {
                    if ('error' in data) {  // error
                        this._rpc._pending[data.id].error_handler(data.error, this._rpc);

                    } else if ('result' in data) {  // result
                        this._rpc._pending[data.id].success_handler(data.result, this._rpc);
                    }

                    delete this._rpc._pending[data.id];
                }
            }
        };
    };

    this.disconnect = (event) => {
        this._ws.close();
    };

    this.on = (event_name, handler) => {
        switch(event_name) {
            case 'open':
                this._onopen_handler.push(handler);
                break;

            case 'close':
                this._onclose_handler.push(handler);
                break;

            default:
                throw `RPC.on: unknown event '${event_name}'`;
        }
    };

    this.call = (method, params, success_handler, error_handler) => {
        let id = this._message_id;
        this._message_id += 1;

        let request = {
            jsonrpc: '2.0',
            id: id,
            method: method,
            params: params
        };

        // default handler
        if (this.DEBUG) {
            this._pending[id] = {
                success_handler: (data) => {
                    console.log('RPC success:', data)
                },
                error_handler: (data) => {
                    console.log('RPC error:', data)
                },
            }
        } else {
            this._pending[id] = {
                success_handler: (data) => {},
                error_handler: (data) => {},
            }
        }

        // custom handler
        if (success_handler) {
            this._pending[id].success_handler = success_handler;
        }

        if (error_handler) {
            this._pending[id].error_handler = error_handler;
        }

        let request_data = JSON.stringify(request);
        this._ws.send(request_data);

        if (this.DEBUG) {
            console.log('RPC >>', request);
        }
    };

    this.subscribe = (topic, handler) => {
        this.call('subscribe', topic, (data) => {
            if (data.indexOf(topic) > -1 && handler) {
                rpc._topic_handler[topic] = handler;
            } else {
                throw `RPC.subscribe: unknown topic '${topic}'`;
            }
        });
    };
}
