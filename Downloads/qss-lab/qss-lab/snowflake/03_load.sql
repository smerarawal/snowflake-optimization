-- Run in SnowSQL (PUT is not supported in the Snowsight worksheet UI).
-- Replace <path> with the absolute path to out/M (or out/S). Load S first to validate.
USE SCHEMA QSS_SNOWFLAKE_LAB.RAW;

CREATE OR REPLACE FILE FORMAT CSV_GZ TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('') EMPTY_FIELD_AS_NULL = TRUE COMPRESSION = GZIP;
CREATE OR REPLACE STAGE LAB_STAGE FILE_FORMAT = CSV_GZ;

PUT file://<path>/dim_tenant/part_*.csv.gz     @LAB_STAGE/dim_tenant/     AUTO_COMPRESS=FALSE;
PUT file://<path>/dim_service/part_*.csv.gz    @LAB_STAGE/dim_service/    AUTO_COMPRESS=FALSE;
PUT file://<path>/appointments/part_*.csv.gz   @LAB_STAGE/appointments/   AUTO_COMPRESS=FALSE;
PUT file://<path>/queue_events/part_*.csv.gz   @LAB_STAGE/queue_events/   AUTO_COMPRESS=FALSE;
PUT file://<path>/notifications/part_*.csv.gz  @LAB_STAGE/notifications/  AUTO_COMPRESS=FALSE;
PUT file://<path>/audit_events/part_*.csv.gz   @LAB_STAGE/audit_events/   AUTO_COMPRESS=FALSE;

COPY INTO DIM_TENANT    FROM @LAB_STAGE/dim_tenant/;
COPY INTO DIM_SERVICE   FROM @LAB_STAGE/dim_service/;
COPY INTO APPOINTMENTS  FROM @LAB_STAGE/appointments/;
COPY INTO QUEUE_EVENTS  FROM @LAB_STAGE/queue_events/;
COPY INTO NOTIFICATIONS FROM @LAB_STAGE/notifications/;
COPY INTO AUDIT_EVENTS  FROM @LAB_STAGE/audit_events/;

-- Validate against out/<scale>/manifest.json row_counts
SELECT 'DIM_TENANT', COUNT(*) FROM DIM_TENANT UNION ALL SELECT 'DIM_SERVICE', COUNT(*) FROM DIM_SERVICE
UNION ALL SELECT 'APPOINTMENTS', COUNT(*) FROM APPOINTMENTS UNION ALL SELECT 'QUEUE_EVENTS', COUNT(*) FROM QUEUE_EVENTS
UNION ALL SELECT 'NOTIFICATIONS', COUNT(*) FROM NOTIFICATIONS UNION ALL SELECT 'AUDIT_EVENTS', COUNT(*) FROM AUDIT_EVENTS;

-- Always suspend when done:
ALTER WAREHOUSE QSS_LAB_WH SUSPEND;
