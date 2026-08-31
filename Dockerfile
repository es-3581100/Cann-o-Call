FROM golang:1.23 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /out/flatten-workspace .

FROM gcr.io/distroless/static:nonroot

COPY --from=build /out/flatten-workspace /flatten-workspace

ENV ADDR=:8080
EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT ["/flatten-workspace"]
