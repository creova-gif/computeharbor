FROM golang:1.23-alpine AS builder

WORKDIR /src

COPY go.mod ./
COPY pkg/ ./pkg/
COPY control-plane/ ./control-plane/

RUN CGO_ENABLED=0 go build -o /computeharbor ./control-plane/cmd/computeharbor

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /computeharbor /computeharbor

EXPOSE 8080

ENTRYPOINT ["/computeharbor"]
CMD ["serve", "-addr", ":8080"]
