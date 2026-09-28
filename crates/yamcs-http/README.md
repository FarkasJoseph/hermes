# yamcs-http

Rust client library for the [YAMCS](https://yamcs.org) (Yet Another Mission Control System) HTTP/REST API.

## Features

- Complete HTTP/REST API client for YAMCS
- Support for multiple authentication methods (JWT, client certificates, none)
- WebSocket support for real-time subscriptions (with `websocket` feature)
- Comprehensive type definitions for all YAMCS API objects
- Async/await using `tokio` and `reqwest`
- Request interceptors for custom authentication flows

## Code Generation

`proto/yamcs/` holds 13 `.proto` files vendored from the `yamcs-api-5.13.5.jar` that YAMCS 5.13.5
ships, covering the mdb, processing, events, pvalue and server packages plus the websocket
envelope. The `build.rs` script generates Rust types from them with
[prost](https://github.com/tokio-rs/prost), into Cargo's `OUT_DIR`, exposed as `yamcs_http::pb`.

Building therefore needs `protoc` on the PATH, as `hermes-pb` already does.

To refresh the vendored protos for a newer YAMCS, copy them out of that version's
`yamcs-api-*.jar`, keeping the directory layout, since the import paths inside the files depend on
it. Re-record the test fixtures in the same pass; `tests/offline.rs` carries the commands.

## Contributing

This crate is part of the Hermes ground data system project. Contributions are welcome!

## License

See the main Hermes repository for license information.

## Resources

- [YAMCS Documentation](https://docs.yamcs.org/)
- [YAMCS REST API Reference](https://docs.yamcs.org/yamcs-http-api/)
- [Hermes Project](https://github.com/nasa/hermes)
