# Two-stage build: compile a static Go binary, drop it onto ubuntu:22.04 so the
# in-container userspace matches the host (glibc 2.35, same as kernel 5.15).
# CRIU compatibility is best when the in-container libc matches the host's
# kernel headers, so we avoid musl/alpine here.
FROM docker.io/library/golang:1.22-bullseye AS build
WORKDIR /src
COPY main.go .
RUN CGO_ENABLED=0 go build -o /counter-app main.go

FROM docker.io/library/ubuntu:22.04
COPY --from=build /counter-app /counter-app
EXPOSE 8080
ENTRYPOINT ["/counter-app"]
