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
exports.createProducer = void 0;
var amqp = __importStar(require("amqplib"));
var uuid_1 = require("uuid");
var producerDefaultOptions = {
    username: 'guest',
    password: 'guest',
    host: 'localhost',
    port: 5672
};
var createProducer = function (exchanges, options) {
    return {
        producer: function (exchange, params) { return __awaiter(void 0, void 0, void 0, function () {
            var producerOptions, _a, conn, channel, exchangeName, content, sent, err_1;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        producerOptions = __assign(__assign({}, producerDefaultOptions), options);
                        return [4 /*yield*/, getConnectionAndChannel(options)];
                    case 1:
                        _a = _b.sent(), conn = _a.conn, channel = _a.channel;
                        _b.label = 2;
                    case 2:
                        _b.trys.push([2, 6, 7, 10]);
                        exchangeName = producerOptions.serviceName + "." + exchange.toString();
                        return [4 /*yield*/, assertExchange(channel, exchangeName)];
                    case 3:
                        _b.sent();
                        content = Buffer.from(JSON.stringify(params));
                        sent = channel.publish(exchangeName, '', content, { persistent: true, messageId: uuid_1.v4() });
                        if (!!sent) return [3 /*break*/, 5];
                        return [4 /*yield*/, new Promise(function (resolve) { return channel.once('drain', function () { return resolve; }); })];
                    case 4:
                        _b.sent();
                        _b.label = 5;
                    case 5: return [3 /*break*/, 10];
                    case 6:
                        err_1 = _b.sent();
                        throw err_1;
                    case 7: return [4 /*yield*/, channel.close()];
                    case 8:
                        _b.sent();
                        return [4 /*yield*/, conn.close()];
                    case 9:
                        _b.sent();
                        return [7 /*endfinally*/];
                    case 10: return [2 /*return*/];
                }
            });
        }); },
        defineExchanges: function () { return __awaiter(void 0, void 0, void 0, function () {
            var _a, conn, channel, arrPromises, err_2;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0: return [4 /*yield*/, getConnectionAndChannel(options)];
                    case 1:
                        _a = _b.sent(), conn = _a.conn, channel = _a.channel;
                        _b.label = 2;
                    case 2:
                        _b.trys.push([2, 4, 5, 8]);
                        arrPromises = exchanges.map(function (exchange) {
                            var exchangeName = options.serviceName + "." + exchange.toString();
                            return assertExchange(channel, exchangeName);
                        });
                        return [4 /*yield*/, Promise.all(arrPromises)];
                    case 3:
                        _b.sent();
                        return [3 /*break*/, 8];
                    case 4:
                        err_2 = _b.sent();
                        throw err_2;
                    case 5: return [4 /*yield*/, channel.close()];
                    case 6:
                        _b.sent();
                        return [4 /*yield*/, conn.close()];
                    case 7:
                        _b.sent();
                        return [7 /*endfinally*/];
                    case 8: return [2 /*return*/];
                }
            });
        }); }
    };
};
exports.createProducer = createProducer;
var getConnectionAndChannel = function (options) { return __awaiter(void 0, void 0, void 0, function () {
    var conn, channel;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0: return [4 /*yield*/, amqp.connect("amqp://" + options.username + ":" + options.password + "@" + options.host + ":" + options.port + "/")];
            case 1:
                conn = _a.sent();
                return [4 /*yield*/, conn.createChannel()];
            case 2:
                channel = _a.sent();
                return [2 /*return*/, { conn: conn, channel: channel }];
        }
    });
}); };
var assertExchange = function (channel, exchangeName) { return __awaiter(void 0, void 0, void 0, function () { return __generator(this, function (_a) {
    return [2 /*return*/, channel.assertExchange(exchangeName, 'topic', { durable: true })];
}); }); };
