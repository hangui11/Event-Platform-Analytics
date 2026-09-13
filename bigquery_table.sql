CREATE TABLE `project-bd5f1c88-1252-4a0a-a34.analytics.events` (
  event_id     STRING,
  app          STRING,
  event_name   STRING,
  event_type   STRING,
  user_id      STRING,
  event_time   TIMESTAMP,
  ingested_at  TIMESTAMP,
  version      STRING,
  region       STRING,
  properties   JSON
)
PARTITION BY DATE(event_time)
CLUSTER BY app, event_name
OPTIONS (
  description = "Event analytics platform - shared event envelope from all apps"
);
