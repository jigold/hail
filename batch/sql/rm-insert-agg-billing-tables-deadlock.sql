DELIMITER $$

-- MYSQL takes out locks for any rows that are checked with the foreign
-- key constraints when inserting rows. This can cause deadlocks. To
-- avoid this scenario, we first check the keys exist without locking
-- those rows and then turn off foreign key constraint checking when
-- inserting new rows into the desired table.

-- https://bugs.mysql.com/bug.php?id=48652

DROP TRIGGER IF EXISTS attempts_after_update $$
CREATE TRIGGER attempts_after_update AFTER UPDATE ON attempts
FOR EACH ROW
BEGIN
  DECLARE billing_project_exists INT;
  DECLARE job_exists INT;
  DECLARE resource_exists INT;

  DECLARE job_cores_mcpu INT;
  DECLARE cur_billing_project VARCHAR(100);
  DECLARE msec_diff BIGINT;
  DECLARE cur_n_tokens INT;
  DECLARE rand_token INT;

  SELECT COUNT(*) INTO resource_exists
  FROM resources
  WHERE `resource` = NEW.resource
  LOCK IN SHARE MODE;

  SELECT COUNT(*) INTO job_exists
  FROM jobs
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id
  LOCK IN SHARE MODE;

  SELECT billing_project
  INTO cur_billing_project
  FROM batches WHERE id = NEW.batch_id
  LOCK IN SHARE MODE;

  SELECT COUNT(*) INTO billing_project_exists
  FROM billing_projects
  WHERE `name` = cur_billing_project
  LOCK IN SHARE MODE;

  -- batch exists is already checked for
  IF resource_exists != 1 OR billing_project_exists != 1 OR job_exists != 1 THEN
    SET @message_text = "Foreign key constraints for inserting into aggregated resources tables were violated";
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @message_text;
  END IF;

  SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
  SET rand_token = FLOOR(RAND() * cur_n_tokens);

  SELECT cores_mcpu INTO job_cores_mcpu FROM jobs
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id;

  SET msec_diff = (GREATEST(COALESCE(NEW.end_time - NEW.start_time, 0), 0) -
                   GREATEST(COALESCE(OLD.end_time - OLD.start_time, 0), 0));

  SET FOREIGN_KEY_CHECKS = 0;

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

  SET FOREIGN_KEY_CHECKS = 1;
END $$

DROP TRIGGER IF EXISTS attempt_resources_after_insert $$
CREATE TRIGGER attempt_resources_after_insert AFTER INSERT ON attempt_resources
FOR EACH ROW
BEGIN
  DECLARE billing_project_exists INT;
  DECLARE job_exists INT;
  DECLARE resource_exists INT;

  DECLARE cur_start_time BIGINT;
  DECLARE cur_end_time BIGINT;
  DECLARE cur_billing_project VARCHAR(100);
  DECLARE msec_diff BIGINT;
  DECLARE cur_n_tokens INT;
  DECLARE rand_token INT;

  SELECT COUNT(*) INTO resource_exists
  FROM resources
  WHERE `resource` = NEW.resource
  LOCK IN SHARE MODE;

  SELECT COUNT(*) INTO job_exists
  FROM jobs
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id
  LOCK IN SHARE MODE;

  SELECT billing_project
  INTO cur_billing_project
  FROM batches WHERE id = NEW.batch_id
  LOCK IN SHARE MODE;

  SELECT COUNT(*) INTO billing_project_exists
  FROM billing_projects
  WHERE `name` = cur_billing_project
  LOCK IN SHARE MODE;

  -- batch exists is already checked for
  IF resource_exists != 1 OR billing_project_exists != 1 OR job_exists != 1 THEN
    SET @message_text = "Foreign key constraints for inserting into aggregated resources tables were violated";
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = @message_text;
  END IF;

  SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
  SET rand_token = FLOOR(RAND() * cur_n_tokens);

  SELECT start_time, end_time INTO cur_start_time, cur_end_time
  FROM attempts
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  LOCK IN SHARE MODE;

  SET msec_diff = GREATEST(COALESCE(cur_end_time - cur_start_time, 0), 0);

  SET FOREIGN_KEY_CHECKS = 0;

  INSERT INTO aggregated_job_resources (batch_id, job_id, resource, `usage`)
  VALUES (NEW.batch_id, NEW.job_id, NEW.resource, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_batch_resources (batch_id, resource, token, `usage`)
  VALUES (NEW.batch_id, NEW.resource, rand_token, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_billing_project_resources (billing_project, resource, token, `usage`)
  VALUES (cur_billing_project, NEW.resource, rand_token, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  SET FOREIGN_KEY_CHECKS = 1;
END $$

DELIMITER ;
