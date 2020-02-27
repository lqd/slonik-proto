Quick way to build and test:
cargo build --release -q && cp target/release/libslonik_proto.so ./slonik_proto.so && python test.py ; rm slonik_proto.so
cargo build --release -q && (cd python && cp ../target/release/libslonik_proto.so ./slonik_proto.so && python test.py ; rm slonik_proto.so)