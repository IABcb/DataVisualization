
CREATE USER subscribers WITH PASSWORD 'subscribers';
CREATE DATABASE subscribers;
GRANT ALL PRIVILEGES ON DATABASE subscribers to subscribers;
\c subscribers
CREATE TABLE "subscribers" (
    "subscriberId" text,
    "Profile" integer,
    "Soccer" boolean,
    "Gaming" boolean,
    "Bandwith" integer,
    "Consumed" integer,
    "downHTTP" integer,
    "upHTTP" integer,
    "downYoutube" integer,
    "upYoutube" integer,
    "downVoIP" integer,
    "upVoIP" integer
);

CREATE TABLE contracts
(
    "user_id" text,
    "contract" integer
);

INSERT INTO contracts VALUES ('192.168.6.30', 3);
INSERT INTO contracts VALUES ('192.168.6.31', 1);
INSERT INTO contracts VALUES ('192.168.6.32', 3);
INSERT INTO contracts VALUES ('192.168.6.33', 2);
INSERT INTO contracts VALUES ('192.168.6.34', 2);
INSERT INTO contracts VALUES ('192.168.6.35', 3);
INSERT INTO contracts VALUES ('192.168.6.36', 3);
INSERT INTO contracts VALUES ('192.168.6.37', 3);
INSERT INTO contracts VALUES ('192.168.6.38', 1);
INSERT INTO contracts VALUES ('192.168.6.39', 1);
INSERT INTO contracts VALUES ('192.168.6.40', 3);
INSERT INTO contracts VALUES ('192.168.6.41', 3);
