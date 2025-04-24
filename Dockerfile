# Build the manager binary
FROM golang:1.21.4-bookworm as builder

RUN apt update  && apt install -y golang git

WORKDIR /workspace
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    go mod download

# Copy the go source
COPY pkg/ pkg/
COPY ori/ ori/
COPY hack/ hack/

ARG TARGETOS
ARG TARGETARCH

FROM builder as cephlet-bucket-builder
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GO111MODULE=on go build -ldflags="-s -w" -a -o bin/cephlet-bucket ./ori/bucket/cmd/bucket/main.go


# Start from Kubernetes Debian base.
FROM builder as cephlet-volume-builder
# Install necessary dependencies

RUN apt update  && apt install -y libcephfs-dev librbd-dev librados-dev libc-bin

# Build
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    CGO_ENABLED=1 GOOS=$TARGETOS GOARCH=$TARGETARCH GO111MODULE=on go build -ldflags="-s -w" -a -o bin/cephlet-volume ./ori/volume/cmd/volume/main.go


# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot as manager
WORKDIR /
COPY --from=controller /workspace/bin/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]



# Since we're leveraging apt to pull in dependencies, we use `gcr.io/distroless/base` because it includes glibc.
FROM gcr.io/distroless/base-debian11 as distroless-base

# The distroless amd64 image has a target triplet of x86_64
FROM distroless-base AS distroless-amd64
ENV LIB_DIR_PREFIX x86_64
ENV LIB_DIR_PREFIX_MINUS x86-64

# The distroless arm64 image has a target triplet of aarch64
FROM distroless-base AS distroless-arm64
ENV LIB_DIR_PREFIX aarch64
ENV LIB_DIR_PREFIX_MINUS aarch64


FROM busybox:1.36.1-uclibc as busybox
FROM distroless-$TARGETARCH  as cephlet-volume
WORKDIR /
COPY --from=cephlet-volume-builder /workspace/bin/cephlet-volume /cephlet-volume
USER 65532:65532
ENTRYPOINT ["/cephlet-volume"]


FROM debian:trixie AS cephlet-bucket
RUN apt update  && apt install -y --no-install-recommends libcephfs-dev librbd-dev librados-dev libc-bin ca-certificates
WORKDIR /
COPY --from=cephlet-bucket-builder /workspace/bin/cephlet-bucket /cephlet-bucket
USER 65532:65532
ENTRYPOINT ["/cephlet-bucket"]

# Build stage used for validation of the output-image
# See validate-container-linux-* targets in Makefile
FROM cephlet-volume as validation-image

COPY --from=busybox /usr/bin/ldd /usr/bin/find /usr/bin/xargs /usr/bin/
COPY --from=builder /workspace/hack/print-missing-deps.sh /print-missing-deps.sh
SHELL ["/bin/bash", "-c"]
RUN /print-missing-deps.sh


# Final build stage, create the real Docker image with ENTRYPOINT
FROM cephlet-volume
USER 65532:65532

ENTRYPOINT ["/cephlet-volume"]



FROM distroless-base  as cephlet-bucket
COPY --from=cephlet-bucket-builder /workspace/bin/cephlet-bucket /cephlet-bucket
USER 65532:65532
ENTRYPOINT ["/cephlet-bucket"]
