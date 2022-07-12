ALTER TABLE `resources` ADD COLUMN resource_id INT AUTO_INCREMENT UNIQUE NOT NULL;

ALTER TABLE `attempt_resources` ADD COLUMN resource_id INT, ALGORITHM=INSTANT;
ALTER TABLE `aggregated_billing_project_resources` ADD COLUMN resource_id INT, ALGORITHM=INSTANT;
ALTER TABLE `aggregated_batch_resources` ADD COLUMN resource_id INT, ALGORITHM=INSTANT;
ALTER TABLE `aggregated_job_resources` ADD COLUMN resource_id INT, ALGORITHM=INSTANT;

SET foreign_key_checks = 0;
ALTER TABLE `attempt_resources` ADD FOREIGN KEY (resource_id) REFERENCES `resources` (resource_id) ON DELETE CASCADE, ALGORITHM=INPLACE;
ALTER TABLE `aggregated_billing_project_resources` ADD FOREIGN KEY (resource_id) REFERENCES `resources` (resource_id) ON DELETE CASCADE, ALGORITHM=INPLACE;
ALTER TABLE `aggregated_batch_resources` ADD FOREIGN KEY (resource_id) REFERENCES `resources` (resource_id) ON DELETE CASCADE, ALGORITHM=INPLACE;
ALTER TABLE `aggregated_job_resources` ADD FOREIGN KEY (resource_id) REFERENCES `resources` (resource_id) ON DELETE CASCADE, ALGORITHM=INPLACE;
SET foreign_key_checks = 1;

DELIMITER $$

DROP TRIGGER IF EXISTS attempt_resources_before_insert $$
CREATE TRIGGER attempt_resources_before_insert BEFORE INSERT ON attempt_resources
FOR EACH ROW
BEGIN
  DECLARE cur_resource_id INT;
  SELECT resource_id INTO cur_resource_id FROM resources WHERE resource = NEW.resource;
  SET NEW.resource_id = cur_resource_id;
END $$

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

  INSERT INTO aggregated_billing_project_resources (billing_project, resource, resource_id, token, `usage`)
  SELECT billing_project, attempt_resources.resource, resource_id, rand_token, msec_diff * quantity
  FROM attempt_resources
  JOIN batches ON batches.id = attempt_resources.batch_id
  LEFT JOIN resources ON resources.resource = attempt_resources.resource
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

  INSERT INTO aggregated_batch_resources (batch_id, resource, resource_id, token, `usage`)
  SELECT batch_id, attempt_resources.resource, resource_id, rand_token, msec_diff * quantity
  FROM attempt_resources
  LEFT JOIN resources ON resources.resource = attempt_resources.resource
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

  INSERT INTO aggregated_job_resources (batch_id, job_id, resource, resource_id, `usage`)
  SELECT batch_id, job_id, attempt_resources.resource, resource_id, msec_diff * quantity
  FROM attempt_resources
  LEFT JOIN resources ON resources.resource = attempt_resources.resource
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;
END $$

DROP TRIGGER IF EXISTS attempt_resources_after_insert $$
CREATE TRIGGER attempt_resources_after_insert AFTER INSERT ON attempt_resources
FOR EACH ROW
BEGIN
  DECLARE cur_start_time BIGINT;
  DECLARE cur_end_time BIGINT;
  DECLARE cur_billing_project VARCHAR(100);
  DECLARE msec_diff BIGINT;
  DECLARE cur_n_tokens INT;
  DECLARE rand_token INT;
  DECLARE cur_resource_id INT;

  SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
  SET rand_token = FLOOR(RAND() * cur_n_tokens);

  SELECT billing_project INTO cur_billing_project FROM batches WHERE id = NEW.batch_id;

  SELECT resource_id INTO cur_resource_id FROM resources WHERE resource = NEW.resource;

  SELECT start_time, end_time INTO cur_start_time, cur_end_time
  FROM attempts
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  LOCK IN SHARE MODE;

  SET msec_diff = GREATEST(COALESCE(cur_end_time - cur_start_time, 0), 0);

  INSERT INTO aggregated_billing_project_resources (billing_project, resource, resource_id, token, `usage`)
  VALUES (cur_billing_project, NEW.resource, cur_resource_id, rand_token, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_batch_resources (batch_id, resource, resource_id, token, `usage`)
  VALUES (NEW.batch_id, NEW.resource, cur_resource_id, rand_token, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_job_resources (batch_id, job_id, resource, resource_id, `usage`)
  VALUES (NEW.batch_id, NEW.job_id, NEW.resource, cur_resource_id, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;
END $$

-- DROP TRIGGER IF EXISTS attempt_resources_after_update $$
-- CREATE TRIGGER attempt_resources_after_update AFTER UPDATE ON attempt_resources
-- FOR EACH ROW
-- BEGIN
--   DECLARE cur_billing_project VARCHAR(100);
--   DECLARE cur_resource_id INT;
--
--   SELECT billing_project INTO cur_billing_project FROM batches WHERE id = NEW.batch_id;
--   SELECT resource_id INTO cur_resource_id FROM resources WHERE resource = NEW.resource;
--
--   UPDATE aggregated_billing_project_resources
--   SET resource_id = cur_resource_id
--   WHERE billing_project = cur_billing_project AND resource = NEW.resource;
--
--   UPDATE aggregated_batch_resources
--   SET resource_id = cur_resource_id
--   WHERE batch_id = NEW.batch_id AND resource = NEW.resource;
--
--   UPDATE aggregated_job_resources
--   SET resource_id = cur_resource_id
--   WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND resource = NEW.resource;
-- END $$

DELIMITER ;
