ALTER TABLE attempt_resources MODIFY resource_id INT NOT NULL;
ALTER TABLE aggregated_billing_project_resources MODIFY resource_id INT NOT NULL;
ALTER TABLE aggregated_batch_resources MODIFY resource_id INT NOT NULL;
ALTER TABLE aggregated_job_resources MODIFY resource_id INT NOT NULL;

ALTER TABLE attempt_resources DROP PRIMARY KEY, ADD PRIMARY KEY (batch_id, job_id, attempt_id, resource_id);
ALTER TABLE aggregated_billing_project_resources DROP PRIMARY KEY, ADD PRIMARY KEY (billing_project, resource_id, token);
ALTER TABLE aggregated_batch_resources DROP PRIMARY KEY, ADD PRIMARY KEY (batch_id, resource_id, token);
ALTER TABLE aggregated_job_resources DROP PRIMARY KEY, ADD PRIMARY KEY (batch_id, job_id, resource_id);

ALTER TABLE attempt_resources MODIFY resource VARCHAR(100);
ALTER TABLE aggregated_billing_project_resources MODIFY resource VARCHAR(100);
ALTER TABLE aggregated_batch_resources MODIFY resource VARCHAR(100);
ALTER TABLE aggregated_job_resources MODIFY resource VARCHAR(100);

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

  INSERT INTO aggregated_billing_project_resources (billing_project, resource_id, token, `usage`)
  SELECT billing_project, resource_id, rand_token, msec_diff * quantity
  FROM attempt_resources
  JOIN batches ON batches.id = attempt_resources.batch_id
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

  INSERT INTO aggregated_batch_resources (batch_id, resource_id, token, `usage`)
  SELECT batch_id, resource_id, rand_token, msec_diff * quantity
  FROM attempt_resources
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

  INSERT INTO aggregated_job_resources (batch_id, job_id, resource_id, `usage`)
  SELECT batch_id, job_id, resource_id, msec_diff * quantity
  FROM attempt_resources
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

  SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
  SET rand_token = FLOOR(RAND() * cur_n_tokens);

  SELECT billing_project INTO cur_billing_project FROM batches WHERE id = NEW.batch_id;

  SELECT start_time, end_time INTO cur_start_time, cur_end_time
  FROM attempts
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  LOCK IN SHARE MODE;

  SET msec_diff = GREATEST(COALESCE(cur_end_time - cur_start_time, 0), 0);

  INSERT INTO aggregated_billing_project_resources (billing_project, resource_id, token, `usage`)
  VALUES (cur_billing_project, NEW.resource_id, rand_token, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_batch_resources (batch_id, resource_id, token, `usage`)
  VALUES (NEW.batch_id, NEW.resource_id, rand_token, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_job_resources (batch_id, job_id, resource_id, `usage`)
  VALUES (NEW.batch_id, NEW.job_id, NEW.resource_id, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;
END $$

DROP TRIGGER IF EXISTS attempt_resources_before_insert $$

DELIMITER ;
