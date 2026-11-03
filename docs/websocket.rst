``tornado.websocket`` --- Bidirectional communication to the browser
====================================================================

.. testsetup::

   import tornado

.. automodule:: tornado.websocket

   .. autoclass:: WebSocketHandler

   Event handlers
   --------------

   .. automethod:: WebSocketHandler.open
   .. automethod:: WebSocketHandler.on_message

   By default, incoming messages are reassembled and delivered to
   ``on_message`` all at once. Overriding `WebSocketHandler.on_message_chunk`
   switches to streaming delivery instead: `~WebSocketHandler.on_message_begin`
   is called when a message starts arriving, ``on_message_chunk`` with each
   chunk (text chunks are ``str`` decoded incrementally as UTF-8, binary chunks
   are ``bytes``), and `~WebSocketHandler.on_message_end` when the message is
   complete; ``on_message`` is then no longer called. Both single-frame and
   fragmented messages are delivered the same way, and control frames such as
   pings may still be interleaved between fragments. The frame loop waits for
   each (possibly asynchronous) chunk callback before reading more data, so a
   slow consumer never has to buffer an entire large message. If the
   connection is lost in the middle of a message, ``on_message_end`` is not
   called; check `~WebSocketHandler.streaming_message_in_progress` in
   `~WebSocketHandler.on_close` to distinguish that case from a complete
   message.

   .. automethod:: WebSocketHandler.on_message_begin
   .. automethod:: WebSocketHandler.on_message_chunk
   .. automethod:: WebSocketHandler.on_message_end
   .. autoattribute:: WebSocketHandler.streaming_message_in_progress
   .. automethod:: WebSocketHandler.on_close
   .. automethod:: WebSocketHandler.select_subprotocol
   .. autoattribute:: WebSocketHandler.selected_subprotocol
   .. automethod:: WebSocketHandler.on_ping

   Output
   ------

   .. automethod:: WebSocketHandler.write_message
   .. automethod:: WebSocketHandler.close

   Configuration
   -------------

   .. automethod:: WebSocketHandler.check_origin
   .. automethod:: WebSocketHandler.get_compression_options
   .. automethod:: WebSocketHandler.set_nodelay
   .. autoattribute:: WebSocketHandler.max_message_size
   .. autoattribute:: WebSocketHandler.read_chunk_size

   Other
   -----

   .. automethod:: WebSocketHandler.ping
   .. automethod:: WebSocketHandler.on_pong
   .. autoexception:: WebSocketClosedError


   Client-side support
   -------------------

   .. autofunction:: websocket_connect
   .. autoclass:: WebSocketClientConnection
       :members:
   .. autoclass:: WebSocketMessageStream
       :members:
