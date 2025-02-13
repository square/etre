# Etre

[![GoDoc](https://godoc.org/github.com/square/etre?status.svg)](https://pkg.go.dev/github.com/square/etre?tab=doc)

Etre is an entity API for managing primitive entities with labels.

**This project is still under development and should not be used for anything in production yet. We are not seeking external contributors at this time**

# License

[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0)

# Local

## Build and Run

To run a local instance, first compile the binary:

```
cd bin/etre
go build
```

If you don't have MongoDB running already, start the Docker container:

```
cd test/docker
docker compose up
```

This is a single-node replica set, so currently the default Etre config won't connect properly with the Docker container.
Instead, put this config in `bin/etre/config.yaml`:

```yaml
datasource:
  url: "mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true"
cdc:
  datasource:
    url: "mongodb://127.0.0.1:27017/?replicaSet=rs0&directConnection=true"
entity:
  types:
    - host
```

You can modify the `entity` list if you want; this is just an example.

Then start Etre:

```
./etre -config config.yaml
```

If successful, Etre log output should end with:

```
2025/02/13 14:01:45.046796 server.go:152: Connected to main database
2025/02/13 14:01:45.046880 server.go:158: Connected to CDC database
2025/02/13 14:01:45.046908 api_gomux.go:169: Listening on 127.0.0.1:32084
```

## Testing

Start MongoDB in Docker if not already started:

```
cd test/docker
docker compose up
```

Note: this Docker container uses the default MonogDB port: 27017.
If your machine is already running MongoDB, be sure to stop it (or modify the port mappings in `test/docker/docker-compose.yml`).

Then run `go test ./...` from the repo root directory.

## Basic CLI Usage
```shell
> es --insert host hostname=app-001 env=production region=us-west-2
OK, inserted host 67ae6b15e93e456b6ae2d7e4
> es --insert host hostname=app-002 env=production region=us-west-2
OK, inserted host 67ae6b19e93e456b6ae2d7e6
> es --insert host hostname=app-003 env=production region=us-west-2
OK, inserted host 67ae6b1ee93e456b6ae2d7e8
> es --insert host hostname=app-004 env=production region=us-east-1
OK, inserted host 67ae6b25e93e456b6ae2d7ea
> es host region=us-west-2
_id:67ae6b15e93e456b6ae2d7e4,_rev:0,_type:host,env:production,hostname:app-001,region:us-west-2
_id:67ae6b19e93e456b6ae2d7e6,_rev:0,_type:host,env:production,hostname:app-002,region:us-west-2
_id:67ae6b1ee93e456b6ae2d7e8,_rev:0,_type:host,env:production,hostname:app-003,region:us-west-2
> es host region=us-east-1
_id:67ae6b25e93e456b6ae2d7ea,_rev:0,_type:host,env:production,hostname:app-004,region:us-east-1
> es --update host 67ae6b1ee93e456b6ae2d7e8 region=us-east-1
OK, updated host 67ae6b1ee93e456b6ae2d7e8
> es host region=us-west-2
_id:67ae6b15e93e456b6ae2d7e4,_rev:0,_type:host,env:production,hostname:app-001,region:us-west-2
_id:67ae6b19e93e456b6ae2d7e6,_rev:0,_type:host,env:production,hostname:app-002,region:us-west-2
> es host region=us-east-1
_id:67ae6b1ee93e456b6ae2d7e8,_rev:1,_type:host,env:production,hostname:app-003,region:us-east-1
_id:67ae6b25e93e456b6ae2d7ea,_rev:0,_type:host,env:production,hostname:app-004,region:us-east-1
>
```