DELIMITER $$

DROP TRIGGER IF EXISTS attempt_resources_tmp_after_insert $$
CREATE TRIGGER attempt_resources_tmp_after_insert AFTER INSERT ON attempt_resources_tmp
FOR EACH ROW
BEGIN
  DECLARE cur_start_time BIGINT;
  DECLARE cur_end_time BIGINT;
  DECLARE cur_billing_project VARCHAR(100);
  DECLARE msec_diff BIGINT;
  DECLARE cur_n_tokens INT;
  DECLARE rand_token INT;
  DECLARE start_billing_period BIGINT;
  DECLARE end_billing_period BIGINT;

  SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
  SET rand_token = FLOOR(RAND() * cur_n_tokens);

  SELECT billing_project INTO cur_billing_project FROM batches WHERE id = NEW.batch_id;

  SELECT start_time, end_time INTO cur_start_time, cur_end_time
  FROM attempts
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  LOCK IN SHARE MODE;

  SET msec_diff = GREATEST(COALESCE(cur_end_time - cur_start_time, 0), 0);

  SET start_billing_period = UNIX_TIMESTAMP(CAST(FROM_UNIXTIME(cur_end_time / 1000) AS DATE)) * 1000;
  SET end_billing_period = UNIX_TIMESTAMP(ADDDATE(CAST(FROM_UNIXTIME(cur_end_time / 1000) AS DATE), INTERVAL 1 DAY)) * 1000;

  INSERT INTO aggregated_job_resources_tmp (batch_id, job_id, start_time, end_time, resource, `usage`)
  VALUES (NEW.batch_id, NEW.job_id, start_billing_period, end_billing_period, NEW.resource, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_batch_resources_tmp (batch_id, start_time, end_time, resource, token, `usage`)
  VALUES (NEW.batch_id, start_billing_period, end_billing_period, NEW.resource, rand_token, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_billing_project_resources_tmp (billing_project, start_time, end_time, resource, token, `usage`)
  VALUES (cur_billing_project, start_billing_period, end_billing_period, NEW.resource, rand_token, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;
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
  DECLARE start_billing_period BIGINT;
  DECLARE end_billing_period BIGINT;

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

  IF msec_diff > 0 THEN
    SET start_billing_period = UNIX_TIMESTAMP(CAST(FROM_UNIXTIME(NEW.end_time / 1000) AS DATE)) * 1000;
    SET end_billing_period = UNIX_TIMESTAMP(ADDDATE(CAST(FROM_UNIXTIME(NEW.end_time / 1000) AS DATE), INTERVAL 1 DAY)) * 1000;

    INSERT INTO aggregated_billing_project_resources_tmp (billing_project, start_time, end_time, resource, token, `usage`)
    SELECT billing_project, start_billing_period, end_billing_period, resource, rand_token, msec_diff * quantity
    FROM attempt_resources
    JOIN batches ON batches.id = attempt_resources.batch_id
    WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
    ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

    INSERT INTO aggregated_batch_resources_tmp (batch_id, start_time, end_time, resource, token, `usage`)
    SELECT batch_id, start_billing_period, end_billing_period, resource, rand_token, msec_diff * quantity
    FROM attempt_resources
    WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
    ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

    INSERT INTO aggregated_job_resources_tmp (batch_id, job_id, start_time, end_time, resource, `usage`)
    SELECT batch_id, job_id, start_billing_period, end_billing_period, resource, msec_diff * quantity
    FROM attempt_resources
    WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
    ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;
  END IF;
END $$

DELIMITER ;
