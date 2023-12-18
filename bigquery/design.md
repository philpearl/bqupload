
- server receives connections
- first data on a connection describes table and messages
- server gets a buffer that's associated with that info
- server just pushes messages into the buffer

- buffer buffers messages. 
- flushes when full
- also flushes if can send upstream
- flush either goes upstream or to disk.

- sender pulls data to send from buffer
- or from disk (prioritize memory)
- attempts to send it. 
- if send fails, backs off and tries again.

- mechanism to write in-memory data to disk when exit is signalled.