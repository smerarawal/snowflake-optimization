-- Synthetic benchmark schema (modeled on an appointment/queue platform; NOT the QSS schema).
CREATE DATABASE IF NOT EXISTS qss_lab;
USE qss_lab;

CREATE TABLE dim_tenant (
  tenant_id VARCHAR(8) PRIMARY KEY, tenant_name VARCHAR(64), region VARCHAR(16),
  timezone VARCHAR(32), queue_enabled TINYINT, staff_capacity INT, created_at DATETIME
) ENGINE=InnoDB;

CREATE TABLE dim_service (
  service_id INT PRIMARY KEY, tenant_id VARCHAR(8), category VARCHAR(32), avg_duration_min INT,
  KEY idx_service_tenant (tenant_id)
) ENGINE=InnoDB;

CREATE TABLE appointments (
  appointment_id BIGINT PRIMARY KEY, tenant_id VARCHAR(8) NOT NULL, service_id INT NOT NULL,
  customer_id BIGINT NOT NULL, status VARCHAR(16) NOT NULL,
  created_at DATETIME NOT NULL, scheduled_at DATETIME NOT NULL, checked_in_at DATETIME NULL,
  completed_at DATETIME NULL, cancelled_at DATETIME NULL, updated_at DATETIME NOT NULL,
  ingested_at DATETIME NOT NULL,
  KEY idx_appt_tenant_sched (tenant_id, scheduled_at),
  KEY idx_appt_created (created_at),
  KEY idx_appt_customer (customer_id)
) ENGINE=InnoDB;

CREATE TABLE queue_events (
  event_id BIGINT PRIMARY KEY, appointment_id BIGINT NOT NULL, tenant_id VARCHAR(8) NOT NULL,
  event_type VARCHAR(24) NOT NULL, event_ts DATETIME NOT NULL, counter_id INT, ingested_at DATETIME NOT NULL,
  KEY idx_q_appt (appointment_id), KEY idx_q_tenant_ts (tenant_id, event_ts)
) ENGINE=InnoDB;

CREATE TABLE notifications (
  notification_id BIGINT PRIMARY KEY, appointment_id BIGINT NOT NULL, tenant_id VARCHAR(8) NOT NULL,
  message_type VARCHAR(16), channel VARCHAR(8), attempt_no INT, retry_count INT,
  delivery_status VARCHAR(12), created_at DATETIME NOT NULL, ingested_at DATETIME NOT NULL,
  KEY idx_n_appt (appointment_id), KEY idx_n_tenant_ts (tenant_id, created_at)
) ENGINE=InnoDB;

CREATE TABLE audit_events (
  audit_id BIGINT PRIMARY KEY, tenant_id VARCHAR(8) NOT NULL, entity_type VARCHAR(16),
  entity_id BIGINT, event_type VARCHAR(24), actor_type VARCHAR(12),
  event_ts DATETIME NOT NULL, ingested_at DATETIME NOT NULL,
  KEY idx_a_tenant_ts (tenant_id, event_ts), KEY idx_a_entity (entity_id)
) ENGINE=InnoDB;
