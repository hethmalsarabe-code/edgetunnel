// نسخة مبسطة ومستقرة - لا تحتاج إلى env variables
// @ts-ignore
import { connect } from 'cloudflare:sockets';

// استخدم هذا UUID مباشرة - غير قابل للتغيير من الخارج
const userID = '449e7b2a-830d-4829-b365-62686e1536cc';
const proxyIP = 'scholar.google.com';

if (!isValidUUID(userID)) {
    throw new Error('uuid is not valid');
}

export default {
    async fetch(request) {
        try {
            const upgradeHeader = request.headers.get('Upgrade');
            if (!upgradeHeader || upgradeHeader !== 'websocket') {
                const url = new URL(request.url);
                if (url.pathname === '/') {
                    return new Response('VLESS Worker is running!', { status: 200 });
                }
                if (url.pathname === `/${userID}`) {
                    const hostname = request.headers.get('Host') || url.hostname;
                    const vlessConfig = getVLESSConfig(userID, hostname);
                    return new Response(vlessConfig, {
                        status: 200,
                        headers: { "Content-Type": "text/plain;charset=utf-8" }
                    });
                }
                if (url.pathname === `/sub/${userID}`) {
                    const hostname = request.headers.get('Host') || url.hostname;
                    const vlessConfig = getVLESSConfig(userID, hostname);
                    return new Response(vlessConfig, {
                        status: 200,
                        headers: { "Content-Type": "text/plain;charset=utf-8" }
                    });
                }
                return new Response('Not found', { status: 404 });
            } else {
                return await vlessOverWSHandler(request);
            }
        } catch (err) {
            return new Response(err.toString(), { status: 500 });
        }
    },
};

async function vlessOverWSHandler(request) {
    const webSocketPair = new WebSocketPair();
    const [client, webSocket] = Object.values(webSocketPair);
    webSocket.accept();

    const readableWebSocketStream = makeReadableWebSocketStream(webSocket);
    let remoteSocketWapper = { value: null };
    let udpStreamWrite = null;
    let isDns = false;

    readableWebSocketStream.pipeTo(new WritableStream({
        async write(chunk) {
            if (isDns && udpStreamWrite) {
                return udpStreamWrite(chunk);
            }
            if (remoteSocketWapper.value) {
                const writer = remoteSocketWapper.value.writable.getWriter();
                await writer.write(chunk);
                writer.releaseLock();
                return;
            }

            const {
                hasError,
                message,
                portRemote = 443,
                addressRemote = '',
                rawDataIndex,
                vlessVersion = new Uint8Array([0, 0]),
                isUDP,
            } = processVlessHeader(chunk, userID);

            if (hasError) {
                throw new Error(message);
            }

            if (isUDP) {
                if (portRemote === 53) {
                    isDns = true;
                } else {
                    throw new Error('UDP proxy only enable for DNS which is port 53');
                }
            }

            const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
            const rawClientData = chunk.slice(rawDataIndex);

            if (isDns) {
                const { write } = await handleUDPOutBound(webSocket, vlessResponseHeader);
                udpStreamWrite = write;
                udpStreamWrite(rawClientData);
                return;
            }
            handleTCPOutBound(remoteSocketWapper, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader);
        },
    })).catch((err) => {
        console.log('pipeTo error', err);
    });

    return new Response(null, {
        status: 101,
        webSocket: client,
    });
}

async function handleTCPOutBound(remoteSocket, addressRemote, portRemote, rawClientData, webSocket, vlessResponseHeader) {
    async function connectAndWrite(address, port) {
        const tcpSocket = connect({ hostname: address, port: port });
        remoteSocket.value = tcpSocket;
        const writer = tcpSocket.writable.getWriter();
        await writer.write(rawClientData);
        writer.releaseLock();
        return tcpSocket;
    }

    async function retry() {
        const tcpSocket = await connectAndWrite(proxyIP || addressRemote, portRemote);
        tcpSocket.closed.catch(error => {
            console.log('retry error', error);
        }).finally(() => {
            safeCloseWebSocket(webSocket);
        });
        remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader);
    }

    const tcpSocket = await connectAndWrite(addressRemote, portRemote);
    remoteSocketToWS(tcpSocket, webSocket, vlessResponseHeader, retry);
}

function makeReadableWebSocketStream(webSocketServer) {
    let readableStreamCancel = false;
    const stream = new ReadableStream({
        start(controller) {
            webSocketServer.addEventListener('message', (event) => {
                if (readableStreamCancel) return;
                controller.enqueue(event.data);
            });
            webSocketServer.addEventListener('close', () => {
                safeCloseWebSocket(webSocketServer);
                if (readableStreamCancel) return;
                controller.close();
            });
            webSocketServer.addEventListener('error', (err) => {
                controller.error(err);
            });
        },
        cancel(reason) {
            if (readableStreamCancel) return;
            readableStreamCancel = true;
            safeCloseWebSocket(webSocketServer);
        }
    });
    return stream;
}

function processVlessHeader(vlessBuffer, userID) {
    if (vlessBuffer.byteLength < 24) {
        return { hasError: true, message: 'invalid data' };
    }
    const version = new Uint8Array(vlessBuffer.slice(0, 1));
    let isValidUser = false;
    let isUDP = false;
    
    if (stringify(new Uint8Array(vlessBuffer.slice(1, 17))) === userID) {
        isValidUser = true;
    }
    if (!isValidUser) {
        return { hasError: true, message: 'invalid user' };
    }

    const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
    const command = new Uint8Array(vlessBuffer.slice(18 + optLength, 18 + optLength + 1))[0];

    if (command === 2) {
        isUDP = true;
    } else if (command !== 1) {
        return { hasError: true, message: `command ${command} is not support` };
    }

    const portIndex = 18 + optLength + 1;
    const portBuffer = vlessBuffer.slice(portIndex, portIndex + 2);
    const portRemote = new DataView(portBuffer).getUint16(0);

    let addressIndex = portIndex + 2;
    const addressType = new Uint8Array(vlessBuffer.slice(addressIndex, addressIndex + 1))[0];
    let addressLength = 0;
    let addressValueIndex = addressIndex + 1;
    let addressValue = '';

    switch (addressType) {
        case 1:
            addressLength = 4;
            addressValue = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)).join('.');
            break;
        case 2:
            addressLength = new Uint8Array(vlessBuffer.slice(addressValueIndex, addressValueIndex + 1))[0];
            addressValueIndex += 1;
            addressValue = new TextDecoder().decode(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
            break;
        case 3:
            addressLength = 16;
            const dataView = new DataView(vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength));
            const ipv6 = [];
            for (let i = 0; i < 8; i++) {
                ipv6.push(dataView.getUint16(i * 2).toString(16));
            }
            addressValue = ipv6.join(':');
            break;
        default:
            return { hasError: true, message: `invalid addressType is ${addressType}` };
    }

    return {
        hasError: false,
        addressRemote: addressValue,
        portRemote,
        rawDataIndex: addressValueIndex + addressLength,
        vlessVersion: version,
        isUDP,
    };
}

async function remoteSocketToWS(remoteSocket, webSocket, vlessResponseHeader, retry) {
    let vlessHeader = vlessResponseHeader;
    let hasIncomingData = false;
    
    await remoteSocket.readable.pipeTo(new WritableStream({
        async write(chunk) {
            hasIncomingData = true;
            if (webSocket.readyState !== 1) return;
            if (vlessHeader) {
                webSocket.send(await new Blob([vlessHeader, chunk]).arrayBuffer());
                vlessHeader = null;
            } else {
                webSocket.send(chunk);
            }
        },
        close() {},
        abort(reason) {
            console.error('abort', reason);
        },
    })).catch((error) => {
        console.error('remoteSocketToWS error', error);
        safeCloseWebSocket(webSocket);
    });

    if (hasIncomingData === false && retry) {
        retry();
    }
}

async function handleUDPOutBound(webSocket, vlessResponseHeader) {
    let isVlessHeaderSent = false;
    const transformStream = new TransformStream({
        transform(chunk, controller) {
            for (let index = 0; index < chunk.byteLength;) {
                const lengthBuffer = chunk.slice(index, index + 2);
                const udpPakcetLength = new DataView(lengthBuffer).getUint16(0);
                const udpData = new Uint8Array(chunk.slice(index + 2, index + 2 + udpPakcetLength));
                index = index + 2 + udpPakcetLength;
                controller.enqueue(udpData);
            }
        },
    });

    transformStream.readable.pipeTo(new WritableStream({
        async write(chunk) {
            const resp = await fetch('https://1.1.1.1/dns-query', {
                method: 'POST',
                headers: { 'content-type': 'application/dns-message' },
                body: chunk,
            });
            const dnsQueryResult = await resp.arrayBuffer();
            const udpSize = dnsQueryResult.byteLength;
            const udpSizeBuffer = new Uint8Array([(udpSize >> 8) & 0xff, udpSize & 0xff]);
            if (webSocket.readyState === 1) {
                if (isVlessHeaderSent) {
                    webSocket.send(await new Blob([udpSizeBuffer, dnsQueryResult]).arrayBuffer());
                } else {
                    webSocket.send(await new Blob([vlessResponseHeader, udpSizeBuffer, dnsQueryResult]).arrayBuffer());
                    isVlessHeaderSent = true;
                }
            }
        }
    }));

    const writer = transformStream.writable.getWriter();
    return { write: (chunk) => writer.write(chunk) };
}

function isValidUUID(uuid) {
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[4][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    return uuidRegex.test(uuid);
}

function safeCloseWebSocket(socket) {
    try {
        if (socket.readyState === 1 || socket.readyState === 2) {
            socket.close();
        }
    } catch (error) {
        console.error('safeCloseWebSocket error', error);
    }
}

const byteToHex = [];
for (let i = 0; i < 256; ++i) {
    byteToHex.push((i + 256).toString(16).slice(1));
}

function unsafeStringify(arr, offset = 0) {
    return (byteToHex[arr[offset + 0]] + byteToHex[arr[offset + 1]] + byteToHex[arr[offset + 2]] + byteToHex[arr[offset + 3]] + "-" + byteToHex[arr[offset + 4]] + byteToHex[arr[offset + 5]] + "-" + byteToHex[arr[offset + 6]] + byteToHex[arr[offset + 7]] + "-" + byteToHex[arr[offset + 8]] + byteToHex[arr[offset + 9]] + "-" + byteToHex[arr[offset + 10]] + byteToHex[arr[offset + 11]] + byteToHex[arr[offset + 12]] + byteToHex[arr[offset + 13]] + byteToHex[arr[offset + 14]] + byteToHex[arr[offset + 15]]).toLowerCase();
}

function stringify(arr, offset = 0) {
    const uuid = unsafeStringify(arr, offset);
    if (!isValidUUID(uuid)) {
        throw TypeError("Stringified UUID is invalid");
    }
    return uuid;
}

function getVLESSConfig(userID, hostName) {
    return `vless://${userID}@${hostName}:443?encryption=none&security=tls&sni=${hostName}&fp=randomized&type=ws&host=${hostName}&path=%2F%3Fed%3D2048#${hostName}`;
}
