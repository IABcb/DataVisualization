
CREATE USER compa WITH PASSWORD 'compa';
CREATE DATABASE inventory;
GRANT ALL PRIVILEGES ON DATABASE inventory to compa;
\c inventory
CREATE TABLE online
(
    "subscriberId" text,
    "priorityBW" double precision,
	"priorityLatency" double precision,
    "gatewayId" text,
    "providedBW" double precision,
	"providedLatency" double precision
);

CREATE TABLE gateways
(
    "gatewayId" text,
    "MaxNumChannels" int,
    "MaxBW" double precision,
    "UsersConnected" int,
	"Latency" double precision,
	"Threshold" double precision
);

