DROP TABLE IF EXISTS `aggregated_billing_project_resources_by_date`;
CREATE TABLE IF NOT EXISTS `aggregated_billing_project_resources_by_date` (
  `billing_project` VARCHAR(100) NOT NULL,
  `start_time` BIGINT NOT NULL,
  `end_time` BIGINT NOT NULL,
  `resource` VARCHAR(100) NOT NULL,
  `token` INT NOT NULL,
  `usage` BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`billing_project`, `start_time`, `end_time`, `resource`, `token`),
  FOREIGN KEY (`billing_project`) REFERENCES billing_projects(name) ON DELETE CASCADE,
  FOREIGN KEY (`resource`) REFERENCES resources(`resource`) ON DELETE CASCADE
) ENGINE = InnoDB;
CREATE INDEX aggregated_billing_project_resources_by_date_start_time ON `aggregated_billing_project_resources_by_date` (`start_time`);
CREATE INDEX aggregated_billing_project_resources_by_date_end_time ON `aggregated_billing_project_resources_by_date` (`end_time`);

DROP TABLE IF EXISTS `aggregated_batch_resources_by_date`;
CREATE TABLE IF NOT EXISTS `aggregated_batch_resources_by_date` (
  `batch_id` BIGINT NOT NULL,
  `start_time` BIGINT NOT NULL,
  `end_time` BIGINT NOT NULL,
  `resource` VARCHAR(100) NOT NULL,
  `token` INT NOT NULL,
  `usage` BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`batch_id`, `start_time`, `end_time`, `resource`, `token`),
  FOREIGN KEY (`batch_id`) REFERENCES batches(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`resource`) REFERENCES resources(`resource`) ON DELETE CASCADE
) ENGINE = InnoDB;
CREATE INDEX aggregated_batch_resources_by_date_start_time ON `aggregated_batch_resources_by_date` (`start_time`);
CREATE INDEX aggregated_batch_resources_by_date_end_time ON `aggregated_batch_resources_by_date` (`end_time`);

DROP TABLE IF EXISTS `aggregated_job_resources_by_date`;
CREATE TABLE IF NOT EXISTS `aggregated_job_resources_by_date` (
  `batch_id` BIGINT NOT NULL,
  `job_id` INT NOT NULL,
  `start_time` BIGINT NOT NULL,
  `end_time` BIGINT NOT NULL,
  `resource` VARCHAR(100) NOT NULL,
  `usage` BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`batch_id`, `job_id`, `start_time`, `end_time`, `resource`),
  FOREIGN KEY (`batch_id`) REFERENCES batches(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`batch_id`, `job_id`) REFERENCES jobs(`batch_id`, `job_id`) ON DELETE CASCADE,
  FOREIGN KEY (`resource`) REFERENCES resources(`resource`) ON DELETE CASCADE
) ENGINE = InnoDB;
CREATE INDEX aggregated_job_resources_by_date_start_time ON `aggregated_job_resources_by_date` (`start_time`);
CREATE INDEX aggregated_job_resources_by_date_end_time ON `aggregated_job_resources_by_date` (`end_time`);

DROP TABLE IF EXISTS `attempts_time_msecs_diff`;
CREATE TABLE IF NOT EXISTS `attempts_time_msecs_diff` (
  `counter` BIGINT NOT NULL AUTO_INCREMENT,
  `batch_id` BIGINT NOT NULL,
  `job_id` INT NOT NULL,
  `attempt_id` VARCHAR(40) NOT NULL,
  `end_time` BIGINT,
  `msecs_diff` BIGINT NOT NULL,
  PRIMARY KEY (`counter`),
  FOREIGN KEY (`batch_id`, `job_id`) REFERENCES jobs(`batch_id`, `job_id`) ON DELETE CASCADE
) ENGINE = InnoDB;
CREATE INDEX attempts_time_msecs_diff_attempt ON `attempts_time_msecs_diff` (`batch_id`, `job_id`, `attempt_id`);

DELIMITER $$

DROP TRIGGER IF EXISTS attempts_after_update $$
CREATE TRIGGER attempts_after_update AFTER UPDATE ON attempts
FOR EACH ROW
BEGIN
  DECLARE job_cores_mcpu INT;
  DECLARE cur_billing_project VARCHAR(100);
  DECLARE msec_diff BIGINT;
  DECLARE cur_n_tokens INT;
  DECLARE rand_token INT;

  SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
  SET rand_token = FLOOR(RAND() * cur_n_tokens);

  SELECT cores_mcpu INTO job_cores_mcpu FROM jobs
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id;

  SELECT billing_project INTO cur_billing_project FROM batches WHERE id = NEW.batch_id;

  SET msec_diff = (GREATEST(COALESCE(NEW.end_time - NEW.start_time, 0), 0) -
                   GREATEST(COALESCE(OLD.end_time - OLD.start_time, 0), 0));

  INSERT INTO aggregated_billing_project_resources (billing_project, resource, token, `usage`)
  SELECT billing_project, resource, rand_token, msec_diff * quantity
  FROM attempt_resources
  JOIN batches ON batches.id = attempt_resources.batch_id
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

  INSERT INTO aggregated_batch_resources (batch_id, resource, token, `usage`)
  SELECT batch_id, resource, rand_token, msec_diff * quantity
  FROM attempt_resources
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

  INSERT INTO aggregated_job_resources (batch_id, job_id, resource, `usage`)
  SELECT batch_id, job_id, resource, msec_diff * quantity
  FROM attempt_resources
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

  INSERT INTO attempts_time_msecs_diff (batch_id, job_id, attempt_id, msecs_diff)
  VALUES (NEW.batch_id, NEW.job_id, NEW.attempt_id, msec_diff);
END $$

DROP TRIGGER IF EXISTS attempts_after_insert $$
CREATE TRIGGER attempts_after_insert AFTER INSERT ON attempts
FOR EACH ROW
BEGIN
  DECLARE msec_diff BIGINT;

  SET msec_diff = GREATEST(COALESCE(NEW.end_time - NEW.start_time, 0), 0);

  INSERT INTO attempts_time_msecs_diff (batch_id, job_id, attempt_id, msecs_diff)
  VALUES (NEW.batch_id, NEW.job_id, NEW.attempt_id, msec_diff);
END $$

DELIMITER ;
