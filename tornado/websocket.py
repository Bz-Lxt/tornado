"""Implementation of the WebSocket protocol.

`WebSockets <http://dev.w3.org/html5/websockets/>`_ allow for bidirectional
communication between the browser and server. WebSockets are supported in the
current versions of all major browsers.

This module implements the final version of the WebSocket protocol as
defined in `RFC 6455 <http://tools.ietf.org/html/rfc6455>`_.

.. versionchanged:: 4.0
   Removed support for the draft 76 protocol version.
"""

import abc
import asyncio
import base64
import codecs
import functools
import hashlib
import logging
import os
import struct
import sys
import warnings
import zlib
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import (
    Any,
    Optional,
    Protocol,
    Type,
    Union,
    cast,
)

import tornado
from tornado import gen, httpclient, httputil, simple_httpclient
from tornado.concurrent import (
    Future,
    future_add_done_callback,
    future_set_result_unless_cancelled,
)
from tornado.escape import native_str, to_unicode, utf8
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream, StreamClosedError
from tornado.log import app_log, gen_log
from tornado.netutil import Resolver
from tornado.queues import Queue
from tornado.tcpclient import TCPClient
from tornado.util import _websocket_mask


# The zlib compressor types aren't actually exposed anywhere
# publicly, so declare protocols for the portions we use.
class _Compressor(Protocol):
    def compress(self, data: bytes) -> bytes:
        pass

    def flush(self, mode: int) -> bytes:
        pass


class _Decompressor(Protocol):
    @property
    def unconsumed_tail(self) -> bytes:
        pass

    def decompress(self, data: bytes, max_length: int) -> bytes:
        pass

    def flush(self) -> bytes:
        pass


class _WebSocketDelegate(Protocol):
    # The common base interface implemented by WebSocketHandler on
    # the server side and WebSocketClientConnection on the client
    # side.
    def on_ws_connection_close(
        self,
        close_code: int | None = None,
        close_reason: str | None = None,
        message_in_progress: bool = False,
    ) -> None:
        pass

    def on_message(self, message: str | bytes) -> Optional["Awaitable[None]"]:
        pass

    def on_message_begin(self, opcode: int) -> Optional["Awaitable[None]"]:
        pass

    def on_message_chunk(self, data: str | bytes) -> Optional["Awaitable[None]"]:
        pass

    def on_message_end(self) -> Optional["Awaitable[None]"]:
        pass

    def on_ping(self, data: bytes) -> None:
        pass

    def on_pong(self, data: bytes) -> None:
        pass

    def log_exception(
        self,
        typ: type[BaseException] | None,
        value: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        pass


_default_max_message_size = 10 * 1024 * 1024
# Incoming message payloads are read in chunks of at most this many bytes
# (before decompression). Streaming the reads means a slow consumer only has
# to buffer one chunk at a time instead of the whole message, and a single
# (unfragmented) frame is delivered in several chunks the same way as a
# fragmented one.
_default_read_chunk_size = 64 * 1024

# Reads of frames that have already arrived complete without returning
# control to the IOLoop, so a peer that sends frames faster than we can
# process them could otherwise monopolize the IOLoop. We yield to the IOLoop
# after receiving this many bytes, with each frame also counting as
# _FRAME_YIELD_COST bytes since the per-frame overhead dominates when the
# frames are small.
_YIELD_THRESHOLD = 256 * 1024
_FRAME_YIELD_COST = 1024

# log to "gen_log" but suppress duplicate log messages
de_dupe_gen_log = functools.lru_cache(gen_log.log)


class WebSocketError(Exception):
    pass


class WebSocketClosedError(WebSocketError):
    """Raised by operations on a closed connection.

    .. versionadded:: 3.2
    """

    pass


class _WebSocketAborted(Exception):
    """Internal signal that the connection was aborted while reading.

    Raised after `WebSocketProtocol._abort` has already been called, so the
    frame loop only needs to stop and run its normal cleanup.
    """


class _WebSocketParams:
    def __init__(
        self,
        ping_interval: float | None = None,
        ping_timeout: float | None = None,
        max_message_size: int = _default_max_message_size,
        compression_options: dict[str, Any] | None = None,
        read_chunk_size: int = _default_read_chunk_size,
    ) -> None:
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.max_message_size = max_message_size
        self.compression_options = compression_options
        self.read_chunk_size = read_chunk_size


class WebSocketHandler(tornado.web.RequestHandler):
    """Subclass this class to create a basic WebSocket handler.

    Override `on_message` to handle incoming messages, and use
    `write_message` to send messages to the client. You can also
    override `open` and `on_close` to handle opened and closed
    connections.

    Custom upgrade response headers can be sent by overriding
    `~tornado.web.RequestHandler.set_default_headers` or
    `~tornado.web.RequestHandler.prepare`.

    See http://dev.w3.org/html5/websockets/ for details on the
    JavaScript interface.  The protocol is specified at
    http://tools.ietf.org/html/rfc6455.

    Here is an example WebSocket handler that echos back all received messages
    back to the client:

    .. testcode::

      class EchoWebSocket(tornado.websocket.WebSocketHandler):
          def open(self):
              print("WebSocket opened")

          def on_message(self, message):
              self.write_message(u"You said: " + message)

          def on_close(self):
              print("WebSocket closed")

    WebSockets are not standard HTTP connections. The "handshake" is
    HTTP, but after the handshake, the protocol is
    message-based. Consequently, most of the Tornado HTTP facilities
    are not available in handlers of this type. The only communication
    methods available to you are `write_message()`, `ping()`, and
    `close()`. Likewise, your request handler class should implement
    `open()` method rather than ``get()`` or ``post()``.

    If you map the handler above to ``/websocket`` in your application, you can
    invoke it in JavaScript with::

      var ws = new WebSocket("ws://localhost:8888/websocket");
      ws.onopen = function() {
         ws.send("Hello, world");
      };
      ws.onmessage = function (evt) {
         alert(evt.data);
      };

    This script pops up an alert box that says "You said: Hello, world".

    Web browsers allow any site to open a websocket connection to any other,
    instead of using the same-origin policy that governs other network
    access from JavaScript.  This can be surprising and is a potential
    security hole, so since Tornado 4.0 `WebSocketHandler` requires
    applications that wish to receive cross-origin websockets to opt in
    by overriding the `~WebSocketHandler.check_origin` method (see that
    method's docs for details).  Failure to do so is the most likely
    cause of 403 errors when making a websocket connection.

    When using a secure websocket connection (``wss://``) with a self-signed
    certificate, the connection from a browser may fail because it wants
    to show the "accept this certificate" dialog but has nowhere to show it.
    You must first visit a regular HTML page using the same certificate
    to accept it before the websocket connection will succeed.

    If the application setting ``websocket_ping_interval`` has a non-zero
    value, a ping will be sent periodically, and the connection will be
    closed if a response is not received before the ``websocket_ping_timeout``.
    Both settings are in seconds; floating point values are allowed.
    The default timeout is equal to the interval.

    Messages larger than the ``websocket_max_message_size`` application setting
    (default 10MiB) will not be accepted.

    .. versionchanged:: 4.5
       Added ``websocket_ping_interval``, ``websocket_ping_timeout``, and
       ``websocket_max_message_size``.
    """

    def __init__(
        self,
        application: tornado.web.Application,
        request: httputil.HTTPServerRequest,
        **kwargs: Any,
    ) -> None:
        super().__init__(application, request, **kwargs)
        self.ws_connection: WebSocketProtocol | None = None
        self.close_code: int | None = None
        self.close_reason: str | None = None
        self._on_close_called = False
        # Overriding on_message_chunk switches this handler from the
        # buffered on_message API to streaming message delivery.
        self._message_streaming = (
            type(self).on_message_chunk is not WebSocketHandler.on_message_chunk
        )
        # Buffer used to reassemble chunks for the non-streaming on_message
        # API. Text chunks (str) are kept in a list, binary chunks (bytes)
        # in a bytearray.
        self._message_chunks: list[str] | bytearray | None = None
        #: True between `on_message_begin` and `on_message_end`, so
        #: `on_close` can tell a mid-message disconnect from a clean one.
        #: It is valid regardless of whether ``on_message_*`` were
        #: overridden, so it also marks a message that was buffered for
        #: delivery to `on_message`.
        self.streaming_message_in_progress = False

    async def get(self, *args: Any, **kwargs: Any) -> None:
        self.open_args = args
        self.open_kwargs = kwargs

        # Upgrade header should be present and should be equal to WebSocket
        if self.request.headers.get("Upgrade", "").lower() != "websocket":
            self.set_status(400)
            log_msg = 'Can "Upgrade" only to "WebSocket".'
            self.finish(log_msg)
            gen_log.debug(log_msg)
            return

        # Connection header should be upgrade.
        # Some proxy servers/load balancers
        # might mess with it.
        headers = self.request.headers
        connection = map(
            lambda s: s.strip().lower(), headers.get("Connection", "").split(",")
        )
        if "upgrade" not in connection:
            self.set_status(400)
            log_msg = '"Connection" must be "Upgrade".'
            self.finish(log_msg)
            gen_log.debug(log_msg)
            return

        # Browsers do not apply the same-origin policy to websockets, so
        # cross-origin protection is always enabled for them, independent
        # of the cross_origin_protection application setting. This happens
        # here, after prepare(), instead of with the rest of cross-origin
        # protection in RequestHandler._execute, because check_origin
        # overrides may rely on things done in prepare() (such as
        # authentication).
        error = self._check_cross_origin(self.check_origin)
        if error is not None:
            self.set_status(403)
            log_msg = "Cross origin websockets not allowed"
            self.finish(log_msg)
            gen_log.debug("%s: %s", log_msg, error)
            return

        self.ws_connection = self.get_websocket_protocol()
        if self.ws_connection:
            await self.ws_connection.accept_connection(self)
        else:
            self.set_status(426, "Upgrade Required")
            self.set_header("Sec-WebSocket-Version", "13")

    @property
    def ping_interval(self) -> float | None:
        """The interval for sending websocket pings.

        If this is non-zero, the websocket will send a ping every
        ping_interval seconds.
        The client will respond with a "pong". The connection can be configured
        to timeout on late pong delivery using ``websocket_ping_timeout``.

        Set ``websocket_ping_interval = 0`` to disable pings.

        Default: ``0``
        """
        return self.settings.get("websocket_ping_interval", None)

    @property
    def ping_timeout(self) -> float | None:
        """Timeout if no pong is received in this many seconds.

        To be used in combination with ``websocket_ping_interval > 0``.
        If a ping response (a "pong") is not received within
        ``websocket_ping_timeout`` seconds, then the websocket connection
        will be closed.

        This can help to clean up clients which have disconnected without
        cleanly closing the websocket connection.

        Note, the ping timeout cannot be longer than the ping interval.

        Set ``websocket_ping_timeout = 0`` to disable the ping timeout.

        Default: equal to the ``ping_interval``.

        .. versionchanged:: 6.5.0
           Default changed from the max of 3 pings or 30 seconds.
           The ping timeout can no longer be configured longer than the
           ping interval.
        """
        return self.settings.get("websocket_ping_timeout", None)

    @property
    def max_message_size(self) -> int:
        """Maximum allowed message size.

        If the remote peer sends a message larger than this, the connection
        will be closed.

        Default is 10MiB.
        """
        return cast(
            int,
            self.settings.get("websocket_max_message_size", _default_max_message_size),
        )

    @property
    def read_chunk_size(self) -> int:
        """Maximum number of bytes read at once from an incoming message.

        Incoming message payloads (including a single, unfragmented frame)
        are streamed in chunks of this size and passed to
        `on_message_chunk` as they arrive, so even a slow consumer never
        has to buffer a whole large message. The limit applies to the
        compressed bytes on the wire when compression is in use.

        Default is 64KiB.

        .. versionadded:: 6.7
        """
        return cast(
            int,
            self.settings.get("websocket_read_chunk_size", _default_read_chunk_size),
        )

    def write_message(
        self, message: bytes | str | dict[str, Any], binary: bool = False
    ) -> "Future[None]":
        """Sends the given message to the client of this Web Socket.

        The message may be either a string or a dict (which will be
        encoded as json).  If the ``binary`` argument is false, the
        message will be sent as utf8; in binary mode any byte string
        is allowed.

        If the connection is already closed, raises `WebSocketClosedError`.
        Returns a `.Future` which can be used for flow control.

        .. versionchanged:: 3.2
           `WebSocketClosedError` was added (previously a closed connection
           would raise an `AttributeError`)

        .. versionchanged:: 4.3
           Returns a `.Future` which can be used for flow control.

        .. versionchanged:: 5.0
           Consistently raises `WebSocketClosedError`. Previously could
           sometimes raise `.StreamClosedError`.
        """
        if self.ws_connection is None or self.ws_connection.is_closing():
            raise WebSocketClosedError()
        if isinstance(message, dict):
            message = tornado.escape.json_encode(message)
        return self.ws_connection.write_message(message, binary=binary)

    def select_subprotocol(self, subprotocols: list[str]) -> str | None:
        """Override to implement subprotocol negotiation.

        ``subprotocols`` is a list of strings identifying the
        subprotocols proposed by the client.  This method may be
        overridden to return one of those strings to select it, or
        ``None`` to not select a subprotocol.

        Failure to select a subprotocol does not automatically abort
        the connection, although clients may close the connection if
        none of their proposed subprotocols was selected.

        The list may be empty, in which case this method must return
        None. This method is always called exactly once even if no
        subprotocols were proposed so that the handler can be advised
        of this fact.

        .. versionchanged:: 5.1

           Previously, this method was called with a list containing
           an empty string instead of an empty list if no subprotocols
           were proposed by the client.
        """
        return None

    @property
    def selected_subprotocol(self) -> str | None:
        """The subprotocol returned by `select_subprotocol`.

        .. versionadded:: 5.1
        """
        assert self.ws_connection is not None
        return self.ws_connection.selected_subprotocol

    def get_compression_options(self) -> dict[str, Any] | None:
        """Override to return compression options for the connection.

        If this method returns None (the default), compression will
        be disabled.  If it returns a dict (even an empty one), it
        will be enabled.  The contents of the dict may be used to
        control the following compression options:

        ``compression_level`` specifies the compression level.

        ``mem_level`` specifies the amount of memory used for the internal compression state.

         These parameters are documented in detail here:
         https://docs.python.org/3.13/library/zlib.html#zlib.compressobj

        .. versionadded:: 4.1

        .. versionchanged:: 4.5

           Added ``compression_level`` and ``mem_level``.
        """
        # TODO: Add wbits option.
        return None

    def _open(self, *args: str, **kwargs: str) -> Awaitable[None] | None:
        pass

    open: Callable[..., Awaitable[None] | None] = _open
    """Invoked when a new WebSocket is opened.

    The arguments to `open` are extracted from the `tornado.web.URLSpec`
    regular expression, just like the arguments to
    `tornado.web.RequestHandler.get`.

    `open` may be a coroutine. `on_message` will not be called until
    `open` has returned.

    .. versionchanged:: 5.1

        ``open`` may be a coroutine.
    """

    def on_message(self, message: str | bytes) -> Awaitable[None] | None:
        """Handle incoming messages on the WebSocket

        This method must be overridden.

        .. versionchanged:: 4.5

           ``on_message`` can be a coroutine.
        """
        raise NotImplementedError

    def on_message_begin(self, opcode: int) -> Awaitable[None] | None:
        """Called once when a new message starts arriving.

        ``opcode`` is ``1`` for a text message (chunks will be ``str``)
        and ``2`` for a binary message (chunks will be ``bytes``). It
        reflects the opcode of the first frame even when the message is
        split across several fragmented frames.

        To receive messages one chunk at a time instead of all at once in
        `on_message`, override `on_message_chunk` (and optionally this
        method and `on_message_end`). While streaming, the frame loop does
        not read the next chunk until the awaitable returned by the
        previous chunk has completed, which provides backpressure for slow
        consumers.

        .. versionadded:: 6.7
        """
        self.streaming_message_in_progress = True
        if not self._message_streaming:
            self._message_chunks = [] if opcode == 0x1 else bytearray()
        return None

    def on_message_chunk(self, chunk: str | bytes) -> Awaitable[None] | None:
        """Called with each chunk of an incoming message.

        Chunks are ``str`` for text messages (decoded incrementally as
        UTF-8, so multibyte characters are never split) and ``bytes`` for
        binary ones. This may be a coroutine; the next chunk (and the rest
        of the frame stream, including control frames between fragments)
        will not be read until it finishes.

        Overriding this method replaces the buffered `on_message` API with
        streaming delivery: messages are no longer assembled or passed to
        `on_message`.

        .. versionadded:: 6.7
        """
        chunks = self._message_chunks
        if chunks is not None:
            if isinstance(chunk, str):
                assert isinstance(chunks, list)
                chunks.append(chunk)
            else:
                assert isinstance(chunks, bytearray)
                chunks.extend(chunk)
        return None

    def on_message_end(self) -> Awaitable[None] | None:
        """Called once after all chunks of a message have been delivered.

        If the connection is lost in the middle of a message this method
        is not called; `on_close` is called instead, so the two can be
        distinguished by checking
        ``self.streaming_message_in_progress``.

        .. versionadded:: 6.7
        """
        self.streaming_message_in_progress = False
        if self._message_streaming:
            return None
        chunks = self._message_chunks
        self._message_chunks = None
        if isinstance(chunks, bytearray):
            message: str | bytes = bytes(chunks)
        else:
            assert chunks is not None
            message = "".join(chunks)
        return self.on_message(message)

    def ping(self, data: str | bytes = b"") -> None:
        """Send ping frame to the remote end.

        The data argument allows a small amount of data (up to 125
        bytes) to be sent as a part of the ping message. Note that not
        all websocket implementations expose this data to
        applications.

        Consider using the ``websocket_ping_interval`` application
        setting instead of sending pings manually.

        .. versionchanged:: 5.1

           The data argument is now optional.

        """
        data = utf8(data)
        if self.ws_connection is None or self.ws_connection.is_closing():
            raise WebSocketClosedError()
        self.ws_connection.write_ping(data)

    def on_pong(self, data: bytes) -> None:
        """Invoked when the response to a ping frame is received."""
        pass

    def on_ping(self, data: bytes) -> None:
        """Invoked when the a ping frame is received."""
        pass

    def on_close(self) -> None:
        """Invoked when the WebSocket is closed.

        If the connection was closed cleanly and a status code or reason
        phrase was supplied, these values will be available as the attributes
        ``self.close_code`` and ``self.close_reason``.

        .. versionchanged:: 4.0

           Added ``close_code`` and ``close_reason`` attributes.
        """
        pass

    def close(self, code: int | None = None, reason: str | None = None) -> None:
        """Closes this Web Socket.

        Once the close handshake is successful the socket will be closed.

        ``code`` may be a numeric status code, taken from the values
        defined in `RFC 6455 section 7.4.1
        <https://tools.ietf.org/html/rfc6455#section-7.4.1>`_.
        ``reason`` may be a textual message about why the connection is
        closing.  These values are made available to the client, but are
        not otherwise interpreted by the websocket protocol.

        .. versionchanged:: 4.0

           Added the ``code`` and ``reason`` arguments.
        """
        if self.ws_connection:
            self.ws_connection.close(code, reason)
            self.ws_connection = None

    def check_origin(self, origin: str) -> bool:
        """Override to enable support for allowing alternate origins.

        The ``origin`` argument is the value of the ``Origin`` HTTP header,
        the url responsible for initiating this request. This method is
        only called for requests that appear to be cross-origin: it is not
        called when the ``Sec-Fetch-Site`` header indicates a same-origin
        request, when (in the absence of ``Sec-Fetch-Site``) the ``Origin``
        header matches the ``Host`` header, or for clients that do not send
        an ``Origin`` header (because all browsers that implement WebSockets
        support this header, and non-browser clients do not have the same
        cross-site security concerns).

        Should return ``True`` to accept the request or ``False`` to
        reject it. By default, it calls `.RequestHandler.check_trusted_origin`,
        which accepts origins listed in the ``trusted_origins`` application
        setting.

        This method is called after `~.RequestHandler.prepare`, so it may
        rely on anything that ``prepare`` does (such as authentication).

        This is a security protection against cross site scripting attacks on
        browsers, since WebSockets are allowed to bypass the usual same-origin
        policies and don't use CORS headers.

        .. warning::

           This is an important security measure; don't disable it
           without understanding the security implications. In
           particular, if your authentication is cookie-based, you
           must either restrict the origins allowed by
           ``check_origin()`` or implement your own XSRF-like
           protection for websocket connections. See `these
           <https://www.christian-schneider.net/CrossSiteWebSocketHijacking.html>`_
           `articles
           <https://devcenter.heroku.com/articles/websocket-security>`_
           for more.

        To accept all cross-origin traffic (which was the default prior to
        Tornado 4.0), simply override this method to always return ``True``::

            def check_origin(self, origin):
                return True

        To allow connections from any subdomain of your site, you might
        do something like::

            def check_origin(self, origin):
                parsed_origin = urllib.parse.urlparse(origin)
                return parsed_origin.netloc.endswith(".mydomain.com")

        .. versionadded:: 4.0

        .. versionchanged:: 6.6
           This method is no longer called for requests that can be
           determined to be same-origin (including via the ``Sec-Fetch-Site``
           header). The default implementation now calls
           `.RequestHandler.check_trusted_origin` (which uses the
           ``trusted_origins`` application setting); the comparison of the
           ``Origin`` and ``Host`` headers is performed before this method
           is called.
        """
        return self.check_trusted_origin(origin)

    def set_nodelay(self, value: bool) -> None:
        """Set the no-delay flag for this stream.

        By default, small messages may be delayed and/or combined to minimize
        the number of packets sent.  This can sometimes cause 200-500ms delays
        due to the interaction between Nagle's algorithm and TCP delayed
        ACKs.  To reduce this delay (at the expense of possibly increasing
        bandwidth usage), call ``self.set_nodelay(True)`` once the websocket
        connection is established.

        See `.BaseIOStream.set_nodelay` for additional details.

        .. versionadded:: 3.1
        """
        assert self.ws_connection is not None
        self.ws_connection.set_nodelay(value)

    def on_connection_close(self) -> None:
        if self.ws_connection:
            self.ws_connection.on_connection_close()
            self.ws_connection = None
        if not self._on_close_called:
            self._on_close_called = True
            self.on_close()
            self._break_cycles()

    def on_ws_connection_close(
        self,
        close_code: int | None = None,
        close_reason: str | None = None,
        message_in_progress: bool = False,
    ) -> None:
        self.close_code = close_code
        self.close_reason = close_reason
        # Use the protocol's authoritative state in case on_message_begin
        # / on_message_end were overridden without calling super().
        self.streaming_message_in_progress = message_in_progress
        self.on_connection_close()

    def _break_cycles(self) -> None:
        # WebSocketHandlers call finish() early, but we don't want to
        # break up reference cycles (which makes it impossible to call
        # self.render_string) until after we've really closed the
        # connection (if it was established in the first place,
        # indicated by status code 101).
        if self.get_status() != 101 or self._on_close_called:
            super()._break_cycles()

    def get_websocket_protocol(self) -> Optional["WebSocketProtocol"]:
        websocket_version = self.request.headers.get("Sec-WebSocket-Version")
        if websocket_version == "13":
            params = _WebSocketParams(
                ping_interval=self.ping_interval,
                ping_timeout=self.ping_timeout,
                max_message_size=self.max_message_size,
                compression_options=self.get_compression_options(),
                read_chunk_size=self.read_chunk_size,
            )
            return WebSocketProtocol13(self, False, params)
        return None

    def _detach_stream(self) -> IOStream:
        # disable non-WS methods
        for method in [
            "write",
            "redirect",
            "set_header",
            "set_cookie",
            "set_status",
            "flush",
            "finish",
        ]:
            setattr(self, method, _raise_not_supported_for_websockets)
        return self.detach()


def _raise_not_supported_for_websockets(*args: Any, **kwargs: Any) -> None:
    raise RuntimeError("Method not supported for Web Sockets")


class WebSocketProtocol(abc.ABC):
    """Base class for WebSocket protocol versions."""

    def __init__(self, handler: "_WebSocketDelegate") -> None:
        self.handler = handler
        self.stream: IOStream | None = None
        self.client_terminated = False
        self.server_terminated = False

    def _run_callback(
        self, callback: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> "Optional[Future[Any]]":
        """Runs the given callback with exception handling.

        If the callback is a coroutine, returns a Future that resolves when it
        completes. On error (including cancellation), logs the exception and
        aborts the websocket connection. The returned Future never raises.
        """
        try:
            result = callback(*args, **kwargs)
        except (Exception, asyncio.CancelledError):
            self.handler.log_exception(*sys.exc_info())
            self._abort()
            return None
        if result is None:
            return None
        done_future: Future[None] = Future()

        def on_done(f: Future[Any]) -> None:
            try:
                f.result()
            except (Exception, asyncio.CancelledError):
                self.handler.log_exception(*sys.exc_info())
                self._abort()
            future_set_result_unless_cancelled(done_future, None)

        future_add_done_callback(gen.convert_yielded(result), on_done)
        return done_future

    def on_connection_close(self) -> None:
        self._abort()

    def _abort(self) -> None:
        """Instantly aborts the WebSocket connection by closing the socket"""
        self.client_terminated = True
        self.server_terminated = True
        if self.stream is not None:
            self.stream.close()  # forcibly tear down the connection
        self.close()  # let the subclass cleanup

    @abc.abstractmethod
    def close(self, code: int | None = None, reason: str | None = None) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def is_closing(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    async def accept_connection(self, handler: WebSocketHandler) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def write_message(
        self, message: str | bytes | dict[str, Any], binary: bool = False
    ) -> "Future[None]":
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def selected_subprotocol(self) -> str | None:
        raise NotImplementedError()

    @abc.abstractmethod
    def write_ping(self, data: bytes) -> None:
        raise NotImplementedError()

    # The entry points below are used by WebSocketClientConnection,
    # which was introduced after we only supported a single version of
    # WebSocketProtocol. The WebSocketProtocol/WebSocketProtocol13
    # boundary is currently pretty ad-hoc.
    @abc.abstractmethod
    def _process_server_headers(
        self, key: str | bytes, headers: httputil.HTTPHeaders
    ) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def start_pinging(self) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def _receive_frame_loop(self) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def set_nodelay(self, x: bool) -> None:
        raise NotImplementedError()


class _PerMessageDeflateCompressor:
    def __init__(
        self,
        persistent: bool,
        max_wbits: int | None,
        compression_options: dict[str, Any] | None = None,
    ) -> None:
        if max_wbits is None:
            max_wbits = zlib.MAX_WBITS
        # There is no symbolic constant for the minimum wbits value.
        if not (8 <= max_wbits <= zlib.MAX_WBITS):
            raise ValueError(
                "Invalid max_wbits value %r; allowed range 8-%d"
                % (max_wbits, zlib.MAX_WBITS)
            )
        self._max_wbits = max_wbits

        if (
            compression_options is None
            or "compression_level" not in compression_options
        ):
            self._compression_level = tornado.web.GZipContentEncoding.GZIP_LEVEL
        else:
            self._compression_level = compression_options["compression_level"]

        if compression_options is None or "mem_level" not in compression_options:
            self._mem_level = 8
        else:
            self._mem_level = compression_options["mem_level"]

        if persistent:
            self._compressor: _Compressor | None = self._create_compressor()
        else:
            self._compressor = None

    def _create_compressor(self) -> "_Compressor":
        return zlib.compressobj(
            self._compression_level, zlib.DEFLATED, -self._max_wbits, self._mem_level
        )

    def compress(self, data: bytes) -> bytes:
        compressor = self._compressor or self._create_compressor()
        data = compressor.compress(data) + compressor.flush(zlib.Z_SYNC_FLUSH)
        assert data.endswith(b"\x00\x00\xff\xff")
        return data[:-4]


class _PerMessageDeflateDecompressor:
    def __init__(
        self,
        persistent: bool,
        max_wbits: int | None,
        max_message_size: int,
        compression_options: dict[str, Any] | None = None,
    ) -> None:
        self._max_message_size = max_message_size
        self._persistent = persistent
        if max_wbits is None:
            max_wbits = zlib.MAX_WBITS
        if not (8 <= max_wbits <= zlib.MAX_WBITS):
            raise ValueError(
                "Invalid max_wbits value %r; allowed range 8-%d"
                % (max_wbits, zlib.MAX_WBITS)
            )
        self._max_wbits = max_wbits
        if persistent:
            self._decompressor: _Decompressor | None = self._create_decompressor()
        else:
            self._decompressor = None

    def _create_decompressor(self) -> "_Decompressor":
        return zlib.decompressobj(-self._max_wbits)

    def begin_message(self) -> None:
        """Reset the per-message decompression state.

        With context takeover (``persistent=True``) the same decompressor
        is reused across messages; otherwise a fresh one is created for
        each message.
        """
        if self._decompressor is None:
            self._decompressor = self._create_decompressor()

    def end_message(self) -> None:
        if not self._persistent:
            self._decompressor = None

    def inflate_step(self, data: bytes, max_length: int) -> tuple[bytes, bytes]:
        """Inflate one bounded step of a compressed message.

        Feeds ``data`` to zlib with ``max_length`` capping the output.
        Returns ``(output, unconsumed_input)``; the unconsumed input
        (non-empty only when the output cap was reached) must be fed
        back in before any more wire data. Each call returns at most
        ``max_length`` bytes, which keeps decompression memory bounded
        for highly-compressible messages.

        Raises ``zlib.error`` if the compressed data is invalid.
        """
        decompressor = self._decompressor
        assert decompressor is not None
        output = decompressor.decompress(data, max_length)
        return output, decompressor.unconsumed_tail

    def flush(self) -> bytes:
        decompressor = self._decompressor
        assert decompressor is not None
        return decompressor.flush()


class WebSocketProtocol13(WebSocketProtocol):
    """Implementation of the WebSocket protocol from RFC 6455.

    This class supports protocol version 13.

    .. versionchanged:: 6.6
       Previously, this class supported versions 7 and 8 of the WebSocket protocol in addition to
       version 13.
    """

    # Bit masks for the first byte of a frame.
    FIN = 0x80
    RSV1 = 0x40
    RSV2 = 0x20
    RSV3 = 0x10
    RSV_MASK = RSV1 | RSV2 | RSV3
    OPCODE_MASK = 0x0F

    stream: IOStream

    def __init__(
        self,
        handler: "_WebSocketDelegate",
        mask_outgoing: bool,
        params: _WebSocketParams,
    ) -> None:
        WebSocketProtocol.__init__(self, handler)
        self.mask_outgoing = mask_outgoing
        self.params = params
        self._waiting: object = None
        self._compression_options = params.compression_options
        self._decompressor: _PerMessageDeflateDecompressor | None = None
        self._compressor: _PerMessageDeflateCompressor | None = None
        # State of the data message currently being received. Control
        # frames may be interleaved between the fragments of a data
        # message, so this state spans multiple calls to _receive_frame.
        self._message_opcode: int | None = None  # 0x1 (text) or 0x2 (binary)
        self._message_wire_size = 0
        self._message_size = 0
        self._message_compressed = False
        self._message_decoder: codecs.IncrementalDecoder | None = None
        # The total uncompressed size of all messages received or sent.
        # Unicode messages are encoded to utf8.
        # Only for testing; subject to change.
        self._message_bytes_in = 0
        self._message_bytes_out = 0
        # The total size of all packets received or sent.  Includes
        # the effect of compression, frame overhead, and control frames.
        self._wire_bytes_in = 0
        self._wire_bytes_out = 0
        self._received_pong: bool = False
        # The Future for our most recent pong write, if it has not completed,
        # and the payload of the most recent ping received while it was
        # outstanding. See _send_pong.
        self._pong_future: Future[None] | None = None
        self._pending_pong: bytes | None = None
        self.close_code: int | None = None
        self.close_reason: str | None = None
        self._ping_coroutine: asyncio.Task[Any] | None = None

    # Use a property for this to satisfy the abc.
    @property
    def selected_subprotocol(self) -> str | None:
        return self._selected_subprotocol

    @selected_subprotocol.setter
    def selected_subprotocol(self, value: str | None) -> None:
        self._selected_subprotocol = value

    async def accept_connection(self, handler: WebSocketHandler) -> None:
        try:
            self._handle_websocket_headers(handler)
        except ValueError:
            handler.set_status(400)
            log_msg = "Missing/Invalid WebSocket headers"
            handler.finish(log_msg)
            gen_log.debug(log_msg)
            return

        try:
            await self._accept_connection(handler)
        except asyncio.CancelledError:
            self._abort()
            return
        except ValueError:
            gen_log.debug("Malformed WebSocket request received", exc_info=True)
            self._abort()
            return

    def _handle_websocket_headers(self, handler: WebSocketHandler) -> None:
        """Verifies all invariant- and required headers

        If a header is missing or have an incorrect value ValueError will be
        raised
        """
        fields = ("Host", "Sec-Websocket-Key", "Sec-Websocket-Version")
        if not all(map(lambda f: handler.request.headers.get(f), fields)):
            raise ValueError("Missing/Invalid WebSocket headers")

    @staticmethod
    def compute_accept_value(key: str | bytes) -> str:
        """Computes the value for the Sec-WebSocket-Accept header,
        given the value for Sec-WebSocket-Key.
        """
        sha1 = hashlib.sha1()
        sha1.update(utf8(key))
        sha1.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11")  # Magic value
        return native_str(base64.b64encode(sha1.digest()))

    def _challenge_response(self, handler: WebSocketHandler) -> str:
        return WebSocketProtocol13.compute_accept_value(
            cast(str, handler.request.headers.get("Sec-Websocket-Key"))
        )

    async def _accept_connection(self, handler: WebSocketHandler) -> None:
        subprotocol_header = handler.request.headers.get("Sec-WebSocket-Protocol")
        if subprotocol_header:
            subprotocols = [s.strip() for s in subprotocol_header.split(",")]
        else:
            subprotocols = []
        self.selected_subprotocol = handler.select_subprotocol(subprotocols)
        if self.selected_subprotocol:
            assert self.selected_subprotocol in subprotocols
            handler.set_header("Sec-WebSocket-Protocol", self.selected_subprotocol)

        extensions = self._parse_extensions_header(handler.request.headers)
        for ext in extensions:
            if ext[0] == "permessage-deflate" and self._compression_options is not None:
                # TODO: negotiate parameters if compression_options
                # specifies limits.
                self._create_compressors("server", ext[1], self._compression_options)
                if (
                    "client_max_window_bits" in ext[1]
                    and ext[1]["client_max_window_bits"] is None
                ):
                    # Don't echo an offered client_max_window_bits
                    # parameter with no value.
                    del ext[1]["client_max_window_bits"]
                handler.set_header(
                    "Sec-WebSocket-Extensions",
                    httputil._encode_header("permessage-deflate", ext[1]),
                )
                break

        handler.clear_header("Content-Type")
        handler.set_status(101)
        handler.set_header("Upgrade", "websocket")
        handler.set_header("Connection", "Upgrade")
        handler.set_header("Sec-WebSocket-Accept", self._challenge_response(handler))
        handler.finish()

        self.stream = handler._detach_stream()

        self.start_pinging()
        try:
            open_result = handler.open(*handler.open_args, **handler.open_kwargs)
            if open_result is not None:
                await open_result
        except Exception:
            handler.log_exception(*sys.exc_info())
            self._abort()
            return

        await self._receive_frame_loop()

    def _parse_extensions_header(
        self, headers: httputil.HTTPHeaders
    ) -> list[tuple[str, dict[str, str]]]:
        extensions = headers.get("Sec-WebSocket-Extensions", "")
        if extensions:
            return [httputil._parse_header(e.strip()) for e in extensions.split(",")]
        return []

    def _process_server_headers(
        self, key: str | bytes, headers: httputil.HTTPHeaders
    ) -> None:
        """Process the headers sent by the server to this client connection.

        'key' is the websocket handshake challenge/response key.
        """
        assert headers["Upgrade"].lower() == "websocket"
        assert headers["Connection"].lower() == "upgrade"
        accept = self.compute_accept_value(key)
        assert headers["Sec-Websocket-Accept"] == accept

        extensions = self._parse_extensions_header(headers)
        for ext in extensions:
            if ext[0] == "permessage-deflate" and self._compression_options is not None:
                self._create_compressors("client", ext[1])
            else:
                raise ValueError("unsupported extension %r" % (ext,))

        self.selected_subprotocol = headers.get("Sec-WebSocket-Protocol", None)

    def _get_compressor_options(
        self,
        side: str,
        agreed_parameters: dict[str, Any],
        compression_options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Converts a websocket agreed_parameters set to keyword arguments
        for our compressor objects.
        """
        options: dict[str, Any] = dict(
            persistent=(side + "_no_context_takeover") not in agreed_parameters
        )
        wbits_header = agreed_parameters.get(side + "_max_window_bits", None)
        if wbits_header is None:
            options["max_wbits"] = zlib.MAX_WBITS
        else:
            options["max_wbits"] = int(wbits_header)
        options["compression_options"] = compression_options
        return options

    def _create_compressors(
        self,
        side: str,
        agreed_parameters: dict[str, Any],
        compression_options: dict[str, Any] | None = None,
    ) -> None:
        # TODO: handle invalid parameters gracefully
        allowed_keys = {
            "server_no_context_takeover",
            "client_no_context_takeover",
            "server_max_window_bits",
            "client_max_window_bits",
        }
        for key in agreed_parameters:
            if key not in allowed_keys:
                raise ValueError("unsupported compression parameter %r" % key)
        other_side = "client" if (side == "server") else "server"
        self._compressor = _PerMessageDeflateCompressor(
            **self._get_compressor_options(side, agreed_parameters, compression_options)
        )
        self._decompressor = _PerMessageDeflateDecompressor(
            max_message_size=self.params.max_message_size,
            **self._get_compressor_options(
                other_side, agreed_parameters, compression_options
            ),
        )

    def _write_frame(
        self, fin: bool, opcode: int, data: bytes, flags: int = 0
    ) -> "Future[None]":
        data_len = len(data)
        if opcode & 0x8:
            # All control frames MUST have a payload length of 125
            # bytes or less and MUST NOT be fragmented.
            if not fin:
                raise ValueError("control frames may not be fragmented")
            if data_len > 125:
                raise ValueError("control frame payloads may not exceed 125 bytes")
        if fin:
            finbit = self.FIN
        else:
            finbit = 0
        frame = struct.pack("B", finbit | opcode | flags)
        if self.mask_outgoing:
            mask_bit = 0x80
        else:
            mask_bit = 0
        if data_len < 126:
            frame += struct.pack("B", data_len | mask_bit)
        elif data_len <= 0xFFFF:
            frame += struct.pack("!BH", 126 | mask_bit, data_len)
        else:
            frame += struct.pack("!BQ", 127 | mask_bit, data_len)
        if self.mask_outgoing:
            mask = os.urandom(4)
            data = mask + _websocket_mask(mask, data)
        frame += data
        self._wire_bytes_out += len(frame)
        return self.stream.write(frame)

    def write_message(
        self, message: str | bytes | dict[str, Any], binary: bool = False
    ) -> "Future[None]":
        """Sends the given message to the client of this Web Socket."""
        if binary:
            opcode = 0x2
        else:
            opcode = 0x1
        if isinstance(message, dict):
            message = tornado.escape.json_encode(message)
        message = tornado.escape.utf8(message)
        assert isinstance(message, bytes)
        self._message_bytes_out += len(message)
        flags = 0
        if self._compressor:
            message = self._compressor.compress(message)
            flags |= self.RSV1
        # For historical reasons, write methods in Tornado operate in a semi-synchronous
        # mode in which awaiting the Future they return is optional (But errors can
        # still be raised). This requires us to go through an awkward dance here
        # to transform the errors that may be returned while presenting the same
        # semi-synchronous interface.
        try:
            fut = self._write_frame(True, opcode, message, flags=flags)
        except StreamClosedError:
            raise WebSocketClosedError()

        async def wrapper() -> None:
            try:
                await fut
            except StreamClosedError:
                raise WebSocketClosedError()

        return asyncio.ensure_future(wrapper())

    def write_ping(self, data: bytes) -> None:
        """Send ping frame."""
        assert isinstance(data, bytes)
        self._write_frame(True, 0x9, data)

    async def _receive_frame_loop(self) -> None:
        try:
            work = 0  # See _YIELD_THRESHOLD
            while not self.client_terminated:
                start = self._wire_bytes_in
                await self._receive_frame()
                work += _FRAME_YIELD_COST + self._wire_bytes_in - start
                if work >= _YIELD_THRESHOLD:
                    work = 0
                    await asyncio.sleep(0)
        except (StreamClosedError, _WebSocketAborted):
            self._abort()
        finally:
            self.handler.on_ws_connection_close(
                self.close_code,
                self.close_reason,
                message_in_progress=self._message_opcode is not None,
            )

    async def _read_bytes(self, n: int, partial: bool = False) -> bytes:
        data = await self.stream.read_bytes(n, partial=partial)
        self._wire_bytes_in += len(data)
        return data

    async def _receive_frame(self) -> None:
        # Read the frame header.
        data = await self._read_bytes(2)
        header, mask_payloadlen = struct.unpack("BB", data)
        is_final_frame = bool(header & self.FIN)
        reserved_bits = header & self.RSV_MASK
        opcode = header & self.OPCODE_MASK
        opcode_is_control = bool(opcode & 0x8)
        if self._decompressor is not None and opcode != 0:
            # The compression flag is only present on the first frame's
            # header, so remember it for the whole message.
            self._message_compressed = bool(reserved_bits & self.RSV1)
            reserved_bits &= ~self.RSV1
        if reserved_bits:
            # client is using as-yet-undefined extensions; abort
            self._abort()
            return
        is_masked = bool(mask_payloadlen & 0x80)
        payloadlen = mask_payloadlen & 0x7F

        # Parse and validate the length.
        if opcode_is_control and payloadlen >= 126:
            # control frames must have payload < 126
            self._abort()
            return
        if payloadlen < 126:
            pass
        elif payloadlen == 126:
            data = await self._read_bytes(2)
            payloadlen = struct.unpack("!H", data)[0]
        elif payloadlen == 127:
            data = await self._read_bytes(8)
            payloadlen = struct.unpack("!Q", data)[0]

        if opcode_is_control:
            if not is_final_frame:
                # control frames must not be fragmented
                self._abort()
                return
        elif opcode == 0:  # continuation frame
            if self._message_opcode is None:
                # nothing to continue
                self._abort()
                return
            self._message_wire_size += payloadlen
            if self._message_wire_size > self.params.max_message_size:
                self.close(1009, "message too big")
                self._abort()
                return
        else:  # start of a new data message
            if self._message_opcode is not None:
                # can't start new message until the old one is finished
                self._abort()
                return
            if opcode not in (0x1, 0x2):
                # Opcode 3-7 are reserved for future non-control frames;
                # receiving one is a protocol error (RFC 6455).
                self._abort()
                return
            if payloadlen > self.params.max_message_size:
                self.close(1009, "message too big")
                self._abort()
                return

        # Read the mask, if any.
        frame_mask: bytes | None = None
        if is_masked:
            frame_mask = await self._read_bytes(4)

        if opcode_is_control:
            # Control frames are small and fully buffered. They may be
            # interleaved between the fragments of a data message, so
            # they are handled independently of the per-message state.
            data = await self._read_bytes(payloadlen)
            if frame_mask is not None:
                data = _websocket_mask(frame_mask, data)
            handled_future = self._handle_message(opcode, data)
            if handled_future is not None:
                await handled_future
            return

        if opcode != 0:
            await self._begin_message(opcode, payloadlen)

        # Stream the payload in bounded chunks. Each read returns as soon
        # as some bytes are available (partial=True), but never more than
        # read_chunk_size at a time, and the next chunk is not read until
        # the delegate has processed this one, giving slow consumers
        # end-to-end backpressure.
        frame_mask_offset = 0
        remaining = payloadlen
        while remaining > 0:
            n = min(remaining, self.params.read_chunk_size)
            chunk = await self._read_bytes(n, partial=True)
            if frame_mask is not None:
                # The mask cycles over the frame's payload starting at
                # offset zero, so rotate it for each chunk.
                shift = frame_mask_offset % 4
                mask = (
                    frame_mask
                    if shift == 0
                    else (frame_mask[shift:] + frame_mask[:shift])
                )
                chunk = _websocket_mask(mask, chunk)
            frame_mask_offset += len(chunk)
            remaining -= len(chunk)
            await self._message_payload_chunk(chunk, is_final_frame and remaining == 0)
        if is_final_frame and payloadlen == 0:
            # A zero-length final frame still has to flush the
            # decompressor/text decoder (e.g. an empty text message).
            await self._message_payload_chunk(b"", True)

        if is_final_frame:
            await self._end_message()

    async def _begin_message(self, opcode: int, payloadlen: int) -> None:
        self._message_opcode = opcode
        self._message_wire_size = payloadlen
        self._message_size = 0
        if opcode == 0x1:
            self._message_decoder = codecs.getincrementaldecoder("utf-8")(
                errors="strict"
            )
        else:
            self._message_decoder = None
        if self._message_compressed:
            assert self._decompressor is not None
            self._decompressor.begin_message()
        callback = self._run_callback(self.handler.on_message_begin, opcode)
        if callback is not None:
            await callback

    async def _message_payload_chunk(self, data: bytes, final: bool) -> None:
        opcode = self._message_opcode
        assert opcode is not None
        if self._message_compressed:
            assert self._decompressor is not None
            if final:
                data += b"\x00\x00\xff\xff"
            # Inflate one bounded step at a time so a compression bomb
            # is cut off as soon as max_message_size is exceeded,
            # instead of after decompressing the whole message.
            pending = data
            try:
                while pending:
                    output, pending = self._decompressor.inflate_step(
                        pending, self.params.read_chunk_size
                    )
                    if output:
                        await self._deliver_message_chunk(output, opcode)
                if final:
                    output = self._decompressor.flush()
                    if output:
                        await self._deliver_message_chunk(output, opcode)
            except zlib.error:
                self._abort()
                raise _WebSocketAborted()
        elif data:
            await self._deliver_message_chunk(data, opcode)
        if final and opcode == 0x1:
            # Flush any buffered multibyte tail and validate the stream.
            assert self._message_decoder is not None
            try:
                payload = self._message_decoder.decode(b"", final=True)
            except UnicodeDecodeError:
                self._abort()
                raise _WebSocketAborted()
            if payload:
                callback = self._run_callback(self.handler.on_message_chunk, payload)
                if callback is not None:
                    await callback

    async def _deliver_message_chunk(self, chunk: bytes, opcode: int) -> None:
        self._message_size += len(chunk)
        self._message_bytes_in += len(chunk)
        if self._message_size > self.params.max_message_size:
            self.close(1009, "message too big after decompression")
            self._abort()
            raise _WebSocketAborted()
        payload: str | bytes
        if opcode == 0x1:
            # Decode incrementally so a multibyte character split
            # across chunks is never corrupted.
            assert self._message_decoder is not None
            try:
                payload = self._message_decoder.decode(chunk)
            except UnicodeDecodeError:
                self._abort()
                raise _WebSocketAborted()
        else:
            payload = chunk
        if payload:
            callback = self._run_callback(self.handler.on_message_chunk, payload)
            if callback is not None:
                await callback

    async def _end_message(self) -> None:
        if self._message_compressed:
            assert self._decompressor is not None
            self._decompressor.end_message()
        self._message_opcode = None
        self._message_wire_size = 0
        self._message_size = 0
        self._message_compressed = False
        self._message_decoder = None
        callback = self._run_callback(self.handler.on_message_end)
        if callback is not None:
            await callback

    def _handle_message(self, opcode: int, data: bytes) -> "Optional[Future[None]]":
        """Execute a control-frame callback, returning its Future.

        Data messages are streamed through on_message_begin/chunk/end and
        never reach this method; it handles close, ping and pong frames,
        which may be interleaved between fragments of a data message.
        """
        if self.client_terminated:
            return None

        if opcode == 0x8:
            # Close
            self.client_terminated = True
            if len(data) >= 2:
                self.close_code = struct.unpack(">H", data[:2])[0]
            if len(data) > 2:
                self.close_reason = to_unicode(data[2:])
            # Echo the received close code, if any (RFC 6455 section 5.5.1).
            self.close(self.close_code)
        elif opcode == 0x9:
            # Ping
            self._send_pong(data)
            self._run_callback(self.handler.on_ping, data)
        elif opcode == 0xA:
            # Pong
            self._received_pong = True
            return self._run_callback(self.handler.on_pong, data)
        else:
            self._abort()
        return None

    def _send_pong(self, data: bytes) -> None:
        # If a peer sends pings faster than it reads our pongs, the pongs
        # would accumulate in our write buffer without limit. RFC 6455
        # section 5.5.3 allows us to respond only to the most recent ping
        # when we have not yet sent the pongs for previous ones, so we have
        # at most one pong outstanding and remember the latest ping to answer
        # when it has been sent.
        if self._pong_future is not None and not self._pong_future.done():
            self._pending_pong = data
            return
        # This ping supersedes any that were waiting for an answer.
        self._pending_pong = None
        if self.server_terminated:
            # No frames may be sent after a close frame.
            return
        try:
            self._pong_future = self._write_frame(True, 0xA, data)
        except StreamClosedError:
            self._abort()
            return
        self._pong_future.add_done_callback(self._on_pong_sent)

    def _on_pong_sent(self, future: "Future[None]") -> None:
        if self._pong_future is future:
            self._pong_future = None
        if future.cancelled() or future.exception() is not None:
            # The stream was closed.
            self._pending_pong = None
            return
        if self._pending_pong is not None:
            data = self._pending_pong
            self._pending_pong = None
            self._send_pong(data)

    def close(self, code: int | None = None, reason: str | None = None) -> None:
        """Closes the WebSocket connection."""
        if not self.server_terminated:
            if not self.stream.closed():
                if code is None and reason is not None:
                    code = 1000  # "normal closure" status code
                if code is None:
                    close_data = b""
                else:
                    close_data = struct.pack(">H", code)
                if reason is not None:
                    close_data += utf8(reason)
                try:
                    self._write_frame(True, 0x8, close_data)
                except StreamClosedError:
                    self._abort()
            self.server_terminated = True
        if self.client_terminated:
            if self._waiting is not None:
                self.stream.io_loop.remove_timeout(self._waiting)
                self._waiting = None
            self.stream.close()
        elif self._waiting is None:
            # Give the client a few seconds to complete a clean shutdown,
            # otherwise just close the connection.
            self._waiting = self.stream.io_loop.add_timeout(
                self.stream.io_loop.time() + 5, self._abort
            )
        if self._ping_coroutine:
            self._ping_coroutine.cancel()
            self._ping_coroutine = None

    def is_closing(self) -> bool:
        """Return ``True`` if this connection is closing.

        The connection is considered closing if either side has
        initiated its closing handshake or if the stream has been
        shut down uncleanly.
        """
        return self.stream.closed() or self.client_terminated or self.server_terminated

    def set_nodelay(self, x: bool) -> None:
        self.stream.set_nodelay(x)

    @property
    def ping_interval(self) -> float:
        interval = self.params.ping_interval
        if interval is not None:
            return interval
        return 0

    @property
    def ping_timeout(self) -> float:
        timeout = self.params.ping_timeout
        if timeout is not None:
            if self.ping_interval and timeout > self.ping_interval:
                de_dupe_gen_log(
                    # Note: using de_dupe_gen_log to prevent this message from
                    # being duplicated for each connection
                    logging.WARNING,
                    f"The websocket_ping_timeout ({timeout}) cannot be longer"
                    f" than the websocket_ping_interval ({self.ping_interval})."
                    f"\nSetting websocket_ping_timeout={self.ping_interval}",
                )
                return self.ping_interval
            return timeout
        return self.ping_interval

    def start_pinging(self) -> None:
        """Start sending periodic pings to keep the connection alive"""
        if (
            # prevent multiple ping coroutines being run in parallel
            not self._ping_coroutine
            # only run the ping coroutine if a ping interval is configured
            and self.ping_interval > 0
        ):
            self._ping_coroutine = asyncio.create_task(self.periodic_ping())

    @staticmethod
    def ping_sleep_time(*, last_ping_time: float, interval: float, now: float) -> float:
        """Calculate the sleep time until the next ping should be sent."""
        return max(0, last_ping_time + interval - now)

    async def periodic_ping(self) -> None:
        """Send a ping and wait for a pong if ping_timeout is configured.

        Called periodically if the websocket_ping_interval is set and non-zero.
        """
        interval = self.ping_interval
        timeout = self.ping_timeout

        await asyncio.sleep(interval)

        while True:
            # send a ping
            self._received_pong = False
            ping_time = IOLoop.current().time()
            self.write_ping(b"")

            # wait until the ping timeout
            await asyncio.sleep(timeout)

            # make sure we received a pong within the timeout
            if timeout > 0 and not self._received_pong:
                self.close(reason="ping timed out")
                return

            # wait until the next scheduled ping
            await asyncio.sleep(
                self.ping_sleep_time(
                    last_ping_time=ping_time,
                    interval=interval,
                    now=IOLoop.current().time(),
                )
            )


class _MessageBeginEvent:
    """Internal marker placed on the streaming queue at message start."""

    def __init__(self, opcode: int) -> None:
        self.binary = opcode == 0x2


_MESSAGE_END = object()


class WebSocketClientConnection(simple_httpclient._HTTPConnection):
    """WebSocket client connection.

    This class should not be instantiated directly; use the
    `websocket_connect` function instead.
    """

    protocol: WebSocketProtocol | None = None

    def __init__(
        self,
        request: httpclient.HTTPRequest,
        on_message_callback: Callable[[None | str | bytes], None] | None = None,
        compression_options: dict[str, Any] | None = None,
        ping_interval: float | None = None,
        ping_timeout: float | None = None,
        max_message_size: int = _default_max_message_size,
        subprotocols: list[str] | None = None,
        resolver: Resolver | None = None,
        read_chunk_size: int = _default_read_chunk_size,
    ) -> None:
        self.connect_future: Future[WebSocketClientConnection] = Future()
        self.read_queue: Queue[None | str | bytes] = Queue(1)
        # Queue used by read_message_stream. Lazily created when that
        # method is first called, so connections that use read_message or
        # on_message_callback keep their original behavior.
        self._streaming_queue: Queue[Any] | None = None
        # Set once the connection is gone; makes read_message_stream
        # return None after the close sentinel was consumed (e.g. by a
        # WebSocketMessageStream that was interrupted mid-message).
        self._streaming_closed = False
        # Buffer used to reassemble chunks for read_message /
        # on_message_callback when streaming reads are not in use.
        self._message_chunks: list[str] | bytearray | None = None
        self.key = base64.b64encode(os.urandom(16))
        self._on_message_callback = on_message_callback
        self.close_code: int | None = None
        self.close_reason: str | None = None
        self.params = _WebSocketParams(
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
            max_message_size=max_message_size,
            compression_options=compression_options,
            read_chunk_size=read_chunk_size,
        )

        scheme, sep, rest = request.url.partition(":")
        scheme = {"ws": "http", "wss": "https"}[scheme]
        request.url = scheme + sep + rest
        request.headers.update(
            {
                "Upgrade": "websocket",
                "Connection": "Upgrade",
                "Sec-WebSocket-Key": to_unicode(self.key),
                "Sec-WebSocket-Version": "13",
            }
        )
        if subprotocols is not None:
            request.headers["Sec-WebSocket-Protocol"] = ",".join(subprotocols)
        if compression_options is not None:
            # Always offer to let the server set our max_wbits (and even though
            # we don't offer it, we will accept a client_no_context_takeover
            # from the server).
            # TODO: set server parameters for deflate extension
            # if requested in self.compression_options.
            request.headers["Sec-WebSocket-Extensions"] = (
                "permessage-deflate; client_max_window_bits"
            )

        # Websocket connection is currently unable to follow redirects
        request.follow_redirects = False

        self.tcp_client = TCPClient(resolver=resolver)
        super().__init__(
            None,
            request,
            lambda: None,
            self._on_http_response,
            104857600,
            self.tcp_client,
            65536,
            104857600,
        )

    def __del__(self) -> None:
        if self.protocol is not None:
            # Unclosed client connections can sometimes log "task was destroyed but
            # was pending" warnings if shutdown strikes at the wrong time (such as
            # while a ping is being processed due to ping_interval). Log our own
            # warning to make it a little more deterministic (although it's still
            # dependent on GC timing).
            warnings.warn("Unclosed WebSocketClientConnection", ResourceWarning)

    def close(self, code: int | None = None, reason: str | None = None) -> None:
        """Closes the websocket connection.

        ``code`` and ``reason`` are documented under
        `WebSocketHandler.close`.

        .. versionadded:: 3.2

        .. versionchanged:: 4.0

           Added the ``code`` and ``reason`` arguments.
        """
        if self.protocol is not None:
            self.protocol.close(code, reason)
            self.protocol = None

    def on_connection_close(self) -> None:
        if not self.connect_future.done():
            self.connect_future.set_exception(StreamClosedError())
        if self._streaming_queue is not None:
            # The queue may be full of an unconsumed event; enqueue the
            # close sentinel as a separate task so it always lands after
            # the last chunk without blocking the frame loop.
            if not self._streaming_closed:
                self._streaming_closed = True
                asyncio.ensure_future(self._streaming_queue.put(None))
        else:
            self._on_message(None)
        self.tcp_client.close()
        super().on_connection_close()

    def on_ws_connection_close(
        self,
        close_code: int | None = None,
        close_reason: str | None = None,
        message_in_progress: bool = False,
    ) -> None:
        self.close_code = close_code
        self.close_reason = close_reason
        self.on_connection_close()

    def _on_http_response(self, response: httpclient.HTTPResponse) -> None:
        if not self.connect_future.done():
            if response.error:
                self.connect_future.set_exception(response.error)
            else:
                self.connect_future.set_exception(
                    WebSocketError("Non-websocket response")
                )

    async def headers_received(
        self,
        start_line: httputil.RequestStartLine | httputil.ResponseStartLine,
        headers: httputil.HTTPHeaders,
    ) -> None:
        assert isinstance(start_line, httputil.ResponseStartLine)
        if start_line.code != 101:
            await super().headers_received(start_line, headers)
            return

        if self._timeout is not None:
            self.io_loop.remove_timeout(self._timeout)
            self._timeout = None

        self.headers = headers
        self.protocol = self.get_websocket_protocol()
        self.protocol._process_server_headers(self.key, self.headers)
        self.protocol.stream = self.connection.detach()

        IOLoop.current().add_callback(self.protocol._receive_frame_loop)
        self.protocol.start_pinging()

        # Once we've taken over the connection, clear the final callback
        # we set on the http request.  This deactivates the error handling
        # in simple_httpclient that would otherwise interfere with our
        # ability to see exceptions.
        self.final_callback = None  # type: ignore

        future_set_result_unless_cancelled(self.connect_future, self)

    def write_message(
        self, message: str | bytes | dict[str, Any], binary: bool = False
    ) -> "Future[None]":
        """Sends a message to the WebSocket server.

        If the stream is closed, raises `WebSocketClosedError`.
        Returns a `.Future` which can be used for flow control.

        .. versionchanged:: 5.0
           Exception raised on a closed stream changed from `.StreamClosedError`
           to `WebSocketClosedError`.
        """
        if self.protocol is None:
            raise WebSocketClosedError("Client connection has been closed")
        return self.protocol.write_message(message, binary=binary)

    def read_message(
        self,
        callback: Callable[["Future[Union[None, str, bytes]]"], None] | None = None,
    ) -> Awaitable[None | str | bytes]:
        """Reads a message from the WebSocket server.

        If on_message_callback was specified at WebSocket
        initialization, this function will never return messages

        Returns a future whose result is the message, or None
        if the connection is closed.  If a callback argument
        is given it will be called with the future when it is
        ready.
        """

        awaitable = self.read_queue.get()
        if callback is not None:
            self.io_loop.add_future(asyncio.ensure_future(awaitable), callback)
        return awaitable

    def on_message(self, message: str | bytes) -> Awaitable[None] | None:
        return self._on_message(message)

    def _on_message(self, message: None | str | bytes) -> Awaitable[None] | None:
        if self._on_message_callback:
            self._on_message_callback(message)
            return None
        else:
            return self.read_queue.put(message)

    def on_message_begin(self, opcode: int) -> Awaitable[None] | None:
        queue = self._streaming_queue
        if queue is not None:
            return queue.put(_MessageBeginEvent(opcode))
        self._message_chunks = [] if opcode == 0x1 else bytearray()
        return None

    def on_message_chunk(self, data: str | bytes) -> Awaitable[None] | None:
        queue = self._streaming_queue
        if queue is not None:
            # Backpressure: the frame loop awaits the put, so at most one
            # chunk is buffered here while the application is slow.
            return queue.put(data)
        chunks = self._message_chunks
        assert chunks is not None
        if isinstance(data, str):
            assert isinstance(chunks, list)
            chunks.append(data)
        else:
            assert isinstance(chunks, bytearray)
            chunks.extend(data)
        return None

    def on_message_end(self) -> Awaitable[None] | None:
        queue = self._streaming_queue
        if queue is not None:
            return queue.put(_MESSAGE_END)
        chunks = self._message_chunks
        self._message_chunks = None
        if isinstance(chunks, bytearray):
            message: None | str | bytes = bytes(chunks)
        else:
            assert chunks is not None
            message = "".join(chunks)
        return self._on_message(message)

    async def read_message_stream(
        self,
    ) -> Optional["WebSocketMessageStream"]:
        """Wait for the next message and return it as a stream of chunks.

        Like `read_message`, but instead of buffering the whole message
        the returned `WebSocketMessageStream` yields its contents one
        chunk at a time, so large messages can be processed without
        holding them all in memory. Text messages yield ``str`` chunks
        and binary messages yield ``bytes`` chunks; single-frame and
        fragmented messages are handled identically (control frames such
        as pings may still be interleaved between fragments).

        Returns ``None`` if the connection is closed before the next
        message starts. If the connection is lost in the middle of a
        message, reading from the returned stream raises
        `WebSocketClosedError`, which distinguishes an interrupted
        message from one that was received completely.

        This method must be used instead of `read_message` from the
        start of the connection; the two cannot be mixed. It is also
        incompatible with ``on_message_callback``.

        .. versionadded:: 6.7
        """
        if self._on_message_callback is not None:
            raise RuntimeError(
                "read_message_stream cannot be used with on_message_callback"
            )
        if self._streaming_queue is None:
            if self.read_queue.qsize() > 0:
                raise RuntimeError(
                    "cannot use read_message_stream after read_message has "
                    "buffered messages; choose one style per connection"
                )
            self._streaming_queue = Queue(1)
        elif self._streaming_closed and self._streaming_queue.qsize() == 0:
            # The close sentinel has already been consumed (for example
            # by a stream that was interrupted mid-message), so there will
            # never be another message.
            return None
        event = await self._streaming_queue.get()
        if event is None:
            return None
        if isinstance(event, _MessageBeginEvent):
            return WebSocketMessageStream(self, event.binary)
        raise WebSocketError("protocol error: expected the start of a message")

    def ping(self, data: bytes = b"") -> None:
        """Send ping frame to the remote end.

        The data argument allows a small amount of data (up to 125
        bytes) to be sent as a part of the ping message. Note that not
        all websocket implementations expose this data to
        applications.

        Consider using the ``ping_interval`` argument to
        `websocket_connect` instead of sending pings manually.

        .. versionadded:: 5.1

        """
        data = utf8(data)
        if self.protocol is None:
            raise WebSocketClosedError()
        self.protocol.write_ping(data)

    def on_pong(self, data: bytes) -> None:
        pass

    def on_ping(self, data: bytes) -> None:
        pass

    def get_websocket_protocol(self) -> WebSocketProtocol:
        return WebSocketProtocol13(self, mask_outgoing=True, params=self.params)

    @property
    def selected_subprotocol(self) -> str | None:
        """The subprotocol selected by the server.

        .. versionadded:: 5.1
        """
        assert self.protocol is not None
        return self.protocol.selected_subprotocol

    def log_exception(
        self,
        typ: "Optional[Type[BaseException]]",
        value: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        assert typ is not None
        assert value is not None
        app_log.error("Uncaught exception %s", value, exc_info=(typ, value, tb))


class WebSocketMessageStream:
    """A single incoming WebSocket message, read one chunk at a time.

    Returned by `WebSocketClientConnection.read_message_stream`. Each
    call to `read_chunk` returns the next chunk (``str`` for a text
    message, ``bytes`` for a binary one) and applies backpressure to the
    connection: the next chunk is not read off the socket until the
    previous one has been consumed, so a large message only needs one
    chunk of memory at a time.

    The class also implements the async iterator protocol, so messages
    can be consumed with ``async for``.

    .. versionadded:: 6.7
    """

    def __init__(self, conn: "WebSocketClientConnection", binary: bool) -> None:
        self._conn = conn
        #: True for binary messages, False for text messages.
        self.binary = binary
        self._finished = False

    @property
    def message_type(self) -> int:
        """The WebSocket opcode of this message: 1 for text, 2 for binary."""
        return 0x2 if self.binary else 0x1

    async def read_chunk(self) -> Union[str, bytes, None]:
        """Return the next chunk of the message, or None when complete.

        Raises `WebSocketClosedError` if the connection is lost before
        the message has been received completely. After that (or after
        a previous call returned None), subsequent calls return None.
        """
        if self._finished:
            return None
        queue = self._conn._streaming_queue
        assert queue is not None
        event = await queue.get()
        if event is None:
            self._finished = True
            raise WebSocketClosedError(
                "connection closed before the message was received completely"
            )
        if event is _MESSAGE_END:
            self._finished = True
            return None
        return cast("Union[str, bytes]", event)

    def __aiter__(self) -> "WebSocketMessageStream":
        return self

    async def __anext__(self) -> Union[str, bytes]:
        chunk = await self.read_chunk()
        if chunk is None:
            raise StopAsyncIteration
        return chunk


def websocket_connect(
    url: str | httpclient.HTTPRequest,
    callback: Callable[["Future[WebSocketClientConnection]"], None] | None = None,
    connect_timeout: float | None = None,
    on_message_callback: Callable[[None | str | bytes], None] | None = None,
    compression_options: dict[str, Any] | None = None,
    ping_interval: float | None = None,
    ping_timeout: float | None = None,
    max_message_size: int = _default_max_message_size,
    subprotocols: list[str] | None = None,
    resolver: Resolver | None = None,
    read_chunk_size: int = _default_read_chunk_size,
) -> "Awaitable[WebSocketClientConnection]":
    """Client-side websocket support.

    Takes a url and returns a Future whose result is a
    `WebSocketClientConnection`.

    ``compression_options`` is interpreted in the same way as the
    return value of `.WebSocketHandler.get_compression_options`.

    The connection supports two styles of operation. In the coroutine
    style, the application typically calls
    `~.WebSocketClientConnection.read_message` in a loop::

        conn = yield websocket_connect(url)
        while True:
            msg = yield conn.read_message()
            if msg is None: break
            # Do something with msg

    Large messages can instead be streamed one chunk at a time with
    `~.WebSocketClientConnection.read_message_stream`::

        conn = yield websocket_connect(url, read_chunk_size=64 * 1024)
        while True:
            stream = yield conn.read_message_stream()
            if stream is None: break
            chunks = []
            async for chunk in stream:
                chunks.append(chunk)
                # Slow consumers apply backpressure automatically.

    In the callback style, pass an ``on_message_callback`` to
    ``websocket_connect``. In both styles, a message of ``None``
    indicates that the connection has been closed.

    ``subprotocols`` may be a list of strings specifying proposed
    subprotocols. The selected protocol may be found on the
    ``selected_subprotocol`` attribute of the connection object
    when the connection is complete.

    ``read_chunk_size`` bounds the amount of incoming data buffered
    while a message is being streamed (the default is 64KiB); it also
    controls how much data is read at once internally, so a single
    unfragmented frame is delivered in several chunks just like a
    fragmented one.

    .. versionchanged:: 3.2
       Also accepts ``HTTPRequest`` objects in place of urls.

    .. versionchanged:: 4.1
       Added ``compression_options`` and ``on_message_callback``.

    .. versionchanged:: 4.5
       Added the ``ping_interval``, ``ping_timeout``, and ``max_message_size``
       arguments, which have the same meaning as in `WebSocketHandler`.

    .. versionchanged:: 5.0
       The ``io_loop`` argument (deprecated since version 4.1) has been removed.

    .. versionchanged:: 5.1
       Added the ``subprotocols`` argument.

    .. versionchanged:: 6.3
       Added the ``resolver`` argument.

    .. versionchanged:: 6.7
       Added the ``read_chunk_size`` argument and support for streaming
       reads through ``read_message_stream``.

    .. deprecated:: 6.5
       The ``callback`` argument is deprecated and will be removed in Tornado 7.0.
       Use the returned Future instead. Note that ``on_message_callback`` is not
       deprecated and may still be used.
    """
    if isinstance(url, httpclient.HTTPRequest):
        assert connect_timeout is None
        request = url
        # Copy and convert the headers dict/object (see comments in
        # AsyncHTTPClient.fetch)
        request.headers = httputil.HTTPHeaders(request.headers)
    else:
        request = httpclient.HTTPRequest(url, connect_timeout=connect_timeout)
    request = cast(
        httpclient.HTTPRequest,
        httpclient._RequestProxy(request, httpclient.HTTPRequest._DEFAULTS),
    )
    conn = WebSocketClientConnection(
        request,
        on_message_callback=on_message_callback,
        compression_options=compression_options,
        ping_interval=ping_interval,
        ping_timeout=ping_timeout,
        max_message_size=max_message_size,
        subprotocols=subprotocols,
        resolver=resolver,
        read_chunk_size=read_chunk_size,
    )
    if callback is not None:
        warnings.warn(
            "The callback argument to websocket_connect is deprecated. "
            "Use the returned Future instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        IOLoop.current().add_future(conn.connect_future, callback)
    return conn.connect_future
