# Cloud Backed SQLite patches

These numbered patches apply only to the checksum-pinned CBS source staged by
`build.rs`. They are kept separate from the pristine fetched checkout. The
Google storage module accepts an HTTP or HTTPS base URL through
`endpoint=<base-url>` while retaining the default
`https://storage.googleapis.com` endpoint and the `maxresults` option.
