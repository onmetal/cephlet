podman build --platform=linux/amd64 -t cephlet-dev-env -f local.Dockerfile .

podman run --rm -it \
-v "$(pwd):/workspace" \
-w /workspace \
cephlet-dev-env

go test -v ./pkg/omap/...


podman run --rm \
-v "$(pwd):/workspace" \
-w /workspace \
cephlet-dev-env \
go test -v ./pkg/omap/...

