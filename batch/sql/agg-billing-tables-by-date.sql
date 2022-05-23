CREATE TABLE IF NOT EXISTS `attempt_resources_tmp` (
  `batch_id` BIGINT NOT NULL,
  `job_id` INT NOT NULL,
  `attempt_id` VARCHAR(40) NOT NULL,
  `resource` VARCHAR(100) NOT NULL,
  `quantity` BIGINT NOT NULL,
  PRIMARY KEY (`batch_id`, `job_id`, `attempt_id`, `resource`),
  FOREIGN KEY (`batch_id`) REFERENCES batches(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`batch_id`, `job_id`) REFERENCES jobs(`batch_id`, `job_id`) ON DELETE CASCADE,
  FOREIGN KEY (`batch_id`, `job_id`, `attempt_id`) REFERENCES attempts(`batch_id`, `job_id`, `attempt_id`) ON DELETE CASCADE,
  FOREIGN KEY (`resource`) REFERENCES resources(`resource`) ON DELETE CASCADE
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `aggregated_billing_project_resources_tmp` (
  `billing_project` VARCHAR(100) NOT NULL,
  `start_time` BIGINT NOT NULL,
  `end_time` BIGINT NOT NULL,
  `resource` VARCHAR(100) NOT NULL,
  `token` INT NOT NULL DEFAULT 0,
  `usage` BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`billing_project`, `start_time`, `end_time`, `resource`, `token`),
  FOREIGN KEY (`billing_project`) REFERENCES billing_projects(name) ON DELETE CASCADE,
  FOREIGN KEY (`resource`) REFERENCES resources(`resource`) ON DELETE CASCADE
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `aggregated_batch_resources_tmp` (
  `batch_id` BIGINT NOT NULL,
  `start_time` BIGINT NOT NULL,
  `end_time` BIGINT NOT NULL,
  `resource` VARCHAR(100) NOT NULL,
  `token` INT NOT NULL DEFAULT 0,
  `usage` BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`batch_id`, `start_time`, `end_time`, `resource`, `token`),
  FOREIGN KEY (`batch_id`) REFERENCES batches(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`resource`) REFERENCES resources(`resource`) ON DELETE CASCADE
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `aggregated_job_resources_tmp` (
  `batch_id` BIGINT NOT NULL,
  `job_id` INT NOT NULL,
  `start_time` BIGINT NOT NULL,
  `end_time` BIGINT NOT NULL,
  `resource` VARCHAR(100) NOT NULL,
  `token` INT NOT NULL DEFAULT 0,
  `usage` BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`batch_id`, `job_id`, `start_time`, `end_time`, `resource`, `token`),
  FOREIGN KEY (`batch_id`) REFERENCES batches(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`batch_id`, `job_id`) REFERENCES jobs(`batch_id`, `job_id`) ON DELETE CASCADE,
  FOREIGN KEY (`resource`) REFERENCES resources(`resource`) ON DELETE CASCADE
) ENGINE = InnoDB;
