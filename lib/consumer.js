"use strict";
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createConsumer = void 0;
var amqp = __importStar(require("amqplib"));
var consumerDefaultOptions = {
    username: 'guest',
    password: 'guest',
    host: 'localhost',
    port: 5672,
    maxDelay: 15 * 60 * 1000,
    defaultDelay: 1 * 60 * 1000,
    delayStep: 3 * 60 * 1000,
    exchangeService: '',
    serviceName: '',
};
var createConsumer = function (consumerDeclaration, options) { return function () { return __awaiter(void 0, void 0, void 0, function () {
    var exchanges, arr;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                exchanges = Object.keys(consumerDeclaration);
                arr = exchanges.map(function (exchange) {
                    var handler = consumerDeclaration[exchange];
                    return consumer(exchange.toString(), handler, options);
                });
                return [4 /*yield*/, Promise.all(arr)];
            case 1:
                _a.sent();
                return [2 /*return*/];
        }
    });
}); }; };
exports.createConsumer = createConsumer;
var consumer = function (exchange, externalFn, options) { return __awaiter(void 0, void 0, void 0, function () {
    var consumerOptions, conn, channel, exchangeName, queueName, queue, err_1;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                consumerOptions = __assign(__assign({}, consumerDefaultOptions), options);
                return [4 /*yield*/, amqp.connect("amqp://" + consumerOptions.username + ":" + consumerOptions.password + "@" + consumerOptions.host + ":" + consumerOptions.port + "/")];
            case 1:
                conn = _a.sent();
                return [4 /*yield*/, conn.createChannel()];
            case 2:
                channel = _a.sent();
                _a.label = 3;
            case 3:
                _a.trys.push([3, 8, , 9]);
                exchangeName = consumerOptions.exchangeService + "." + exchange;
                return [4 /*yield*/, channel.assertExchange(exchangeName, 'fanout', { durable: true })];
            case 4:
                _a.sent();
                queueName = consumerOptions.serviceName + "." + exchange;
                return [4 /*yield*/, channel.assertQueue(queueName, { durable: true })];
            case 5:
                queue = _a.sent();
                return [4 /*yield*/, channel.bindQueue(queue.queue, exchangeName, '')];
            case 6:
                _a.sent();
                return [4 /*yield*/, channel.consume(queueName, consumerHandlerWrapper(channel, exchangeName, queueName, consumerOptions, externalFn))];
            case 7:
                _a.sent();
                return [3 /*break*/, 9];
            case 8:
                err_1 = _a.sent();
                console.error(err_1);
                return [3 /*break*/, 9];
            case 9: return [2 /*return*/];
        }
    });
}); };
var consumerHandlerWrapper = function (channel, exchangeName, queueName, options, handler) { return function (msg) { return __awaiter(void 0, void 0, void 0, function () {
    var content, err_2;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                if (!msg)
                    return [2 /*return*/];
                content = JSON.parse(Buffer.from(msg.content).toString());
                _a.label = 1;
            case 1:
                _a.trys.push([1, 3, , 6]);
                return [4 /*yield*/, handler(__assign(__assign({}, msg), { content: content }))];
            case 2:
                _a.sent();
                channel.ack(msg);
                return [3 /*break*/, 6];
            case 3:
                err_2 = _a.sent();
                console.log("Message " + msg.fields.deliveryTag + " failed to deliver with error", err_2);
                if (!(msg.fields.redelivered || msg.properties.headers['x-delay'])) return [3 /*break*/, 5];
                return [4 /*yield*/, publishDelayedQueue(channel, exchangeName, queueName, options, msg)];
            case 4: return [2 /*return*/, _a.sent()];
            case 5: return [2 /*return*/, channel.reject(msg, true)];
            case 6: return [2 /*return*/];
        }
    });
}); }; };
var publishDelayedQueue = function (channel, exchangeName, queueName, options, msg) { return __awaiter(void 0, void 0, void 0, function () {
    var delayInMillisecond, delayedExchangeName;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                delayInMillisecond = getDelayInMillisecond(msg.properties.headers['x-delay'], options);
                delayedExchangeName = "dlx." + exchangeName;
                return [4 /*yield*/, channel.assertExchange(delayedExchangeName, 'x-delayed-message', { durable: true, arguments: { 'x-delayed-type': 'fanout' } })];
            case 1:
                _a.sent();
                return [4 /*yield*/, channel.bindQueue(queueName, exchangeName, '')];
            case 2:
                _a.sent();
                console.log("Republishing message " + msg.fields.deliveryTag + " on " + delayedExchangeName + " with delay of " + delayInMillisecond);
                channel.publish(delayedExchangeName, '', msg.content, { headers: { 'x-delay': delayInMillisecond }, persistent: true });
                channel.ack(msg);
                return [2 /*return*/];
        }
    });
}); };
var getDelayInMillisecond = function (currentDelay, options) {
    if (!currentDelay)
        return options === null || options === void 0 ? void 0 : options.defaultDelay;
    if (currentDelay <= options.maxDelay)
        return options.maxDelay;
    return currentDelay + options.delayStep;
};
