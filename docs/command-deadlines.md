# One-shot command deadlines

`command.run` accepts `timeoutMs` from 1 through 3,600,000 milliseconds. The
default is five minutes. `artifact.build` uses the same bounded runner with a
one-hour default; callers can supply a smaller deadline. Use `process.start`
and its process ID to supervise persistent or longer work.

For a read-only diagnostic with a 30-second client observation budget, set a
shorter execution deadline, for example:

```json
{"cwd":"/allowed/root","argv":["diagnostic-command"],"timeoutMs":10000}
```

The execution deadline belongs to the Executor. Closing the client connection
does not cancel a command, and an observation timeout does not establish whether
the command changed anything. Keep the original request/process identity when
observing an uncertain submission.

At the deadline the runner terminates its Unix process group or Windows Job
Object. It reports `COMMAND_TIMED_OUT`, bounded partial stdout/stderr, whether
termination was requested and whether the root stopped. The error is not
retryable and its outcome is unknown: side effects may already have occurred.
Do not automatically rerun a timed-out mutation.

Output is collected in private temporary files and only the last 64 KiB per
stream is loaded into memory. Completion does not wait for pipe EOF held by a
descendant. Temporary files consume disk while a command runs; this is not a
disk quota or a sandbox against a command deliberately escaping its process
group. Desktop session, HWND, stale-reference and input guards are independent
and remain enforced.
