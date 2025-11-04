ARG GIT_BRANCH=unknown
ARG IMAGE_TAG=unknown
ARG GIT_SHA=unknown

FROM rust:1.85-slim AS builder

ARG GIT_BRANCH
ARG IMAGE_TAG
ARG GIT_SHA

# Create a new empty shell project
WORKDIR /usr/src/zumblezay

# Install dependencies required for building
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libsqlite3-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy over manifests and build script
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./

# Create dummy source files to build dependencies
RUN mkdir -p src && \
    echo "fn main() {}" > src/server_main.rs && \
    echo "pub fn dummy() {}" > src/lib.rs

# Build dependencies - this will be cached if dependencies don't change
RUN cargo build --release --bin zumblezay_server

# Remove the dummy source files
RUN rm -rf src

# Copy the actual source code
COPY src/ src/
COPY testdata/ testdata/

# Build the actual application
RUN touch src/lib.rs src/server_main.rs && cargo build --release --bin zumblezay_server

# Runtime stage
FROM debian:bookworm-slim
ARG GIT_BRANCH
ARG IMAGE_TAG
ARG GIT_SHA
RUN apt-get update && apt-get install -y \
    ca-certificates \
    ffmpeg \
    libsqlite3-0 \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user to run the application
RUN groupadd -r zumblezay && useradd -r -g zumblezay zumblezay
# Create necessary directories with proper permissions
RUN mkdir -p /data && chown -R zumblezay:zumblezay /data
WORKDIR /app
COPY --from=builder /usr/src/zumblezay/target/release/zumblezay_server .

ENV RUST_LOG=info
ENV APP_BUILD_BRANCH=${GIT_BRANCH} \
    APP_BUILD_TAG=${IMAGE_TAG} \
    APP_BUILD_COMMIT=${GIT_SHA}

# Switch to non-root user
USER zumblezay

EXPOSE 3010
VOLUME ["/data"]
ENTRYPOINT ["/app/zumblezay_server"]

# Default command line arguments
CMD ["--events-db", "/data/events.db", "--zumblezay-db", "/data/zumblezay.db", "--cache-db", "/data/cache.db", "--video-path-replacement-prefix", "/data"] 
