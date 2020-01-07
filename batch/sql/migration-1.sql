ALTER TABLE `attempts` ADD CONSTRAINT fk_instance_name
  FOREIGN KEY (instance_name) REFERENCES instances(name) ON DELETE CASCADE;

ALTER TABLE instances ADD time_activated BIGINT;
ALTER TABLE instances ADD time_deactivated BIGINT;
ALTER TABLE instances ADD removed BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX `instances_removed` ON `instances` (`removed`);

DELIMITER $$

DROP TRIGGER IF EXISTS instances_before_update;
CREATE TRIGGER instances_before_update BEFORE UPDATE on instances
FOR EACH ROW
BEGIN
  IF OLD.time_deactivated IS NOT NULL AND (NEW.time_deactivated IS NULL OR NEW.time_deactivated > OLD.time_deactivated) THEN
    SET NEW.time_deactivated = OLD.time_deactivated;
  END IF;
END $$

DROP TRIGGER IF EXISTS jobs_after_insert;
CREATE TRIGGER jobs_after_insert AFTER INSERT ON jobs
FOR EACH ROW
BEGIN
  DECLARE in_user VARCHAR(100);

  SELECT user INTO in_user from batches
  WHERE id = NEW.batch_id;

  IF NEW.state = 'Ready' THEN
    UPDATE user_resources
      SET n_ready_jobs = n_ready_jobs + 1, ready_cores_mcpu = ready_cores_mcpu + NEW.cores_mcpu
      WHERE user = in_user;
    UPDATE ready_cores SET ready_cores_mcpu = ready_cores_mcpu + NEW.cores_mcpu;
  END IF;

  IF NEW.state = 'Running' THEN
    UPDATE user_resources
    SET n_running_jobs = n_running_jobs + 1, running_cores_mcpu = running_cores_mcpu + NEW.cores_mcpu
    WHERE user = in_user;
  END IF;
END $$

DROP TRIGGER IF EXISTS jobs_after_update;
CREATE TRIGGER jobs_after_update AFTER UPDATE ON jobs
FOR EACH ROW
BEGIN
  DECLARE in_user VARCHAR(100);

  SELECT user INTO in_user from batches
  WHERE id = NEW.batch_id;

  IF OLD.state = 'Ready' THEN
    UPDATE user_resources
      SET n_ready_jobs = n_ready_jobs - 1, ready_cores_mcpu = ready_cores_mcpu - OLD.cores_mcpu
      WHERE user = in_user;
    UPDATE ready_cores SET ready_cores_mcpu = ready_cores_mcpu - OLD.cores_mcpu;
  END IF;

  IF NEW.state = 'Ready' THEN
    UPDATE user_resources
      SET n_ready_jobs = n_ready_jobs + 1, ready_cores_mcpu = ready_cores_mcpu + NEW.cores_mcpu
      WHERE user = in_user;
    UPDATE ready_cores SET ready_cores_mcpu = ready_cores_mcpu + NEW.cores_mcpu;
  END IF;

  IF OLD.state = 'Running' THEN
    UPDATE user_resources
    SET n_running_jobs = n_running_jobs - 1, running_cores_mcpu = running_cores_mcpu - OLD.cores_mcpu
    WHERE user = in_user;
  END IF;

  IF NEW.state = 'Running' THEN
    UPDATE user_resources
    SET n_running_jobs = n_running_jobs + 1, running_cores_mcpu = running_cores_mcpu + NEW.cores_mcpu
    WHERE user = in_user;
  END IF;
END $$

DROP PROCEDURE IF EXISTS activate_instance;
CREATE PROCEDURE activate_instance(
  IN in_instance_name VARCHAR(100),
  IN in_ip_address VARCHAR(100),
  IN in_activation_time BIGINT
)
BEGIN
  DECLARE cur_state VARCHAR(40);
  DECLARE cur_token VARCHAR(100);

  START TRANSACTION;

  SELECT state, token INTO cur_state, cur_token FROM instances
  WHERE name = in_instance_name;

  IF cur_state = 'pending' THEN
    UPDATE instances
    SET state = 'active',
      activation_token = NULL,
      ip_address = in_ip_address,
      time_activated = in_activation_time WHERE name = in_instance_name;
    COMMIT;
    SELECT 0 as rc, cur_token as token;
  ELSE
    ROLLBACK;
    SELECT 1 as rc, cur_state, 'state not pending' as message;
  END IF;
END $$

DROP PROCEDURE IF EXISTS deactivate_instance;
CREATE PROCEDURE deactivate_instance(
  IN in_instance_name VARCHAR(100),
  IN in_reason VARCHAR(40),
  IN in_timestamp BIGINT
)
BEGIN
  DECLARE cur_state VARCHAR(40);

  START TRANSACTION;

  SELECT state INTO cur_state FROM instances WHERE name = in_instance_name;

  UPDATE instances
  SET time_deactivated = in_timestamp
  WHERE name = in_instance_name;

  UPDATE attempts
  SET end_time = in_timestamp, reason = in_reason
  WHERE instance_name = in_instance_name;

  IF cur_state = 'pending' or cur_state = 'active' THEN
    UPDATE jobs
    INNER JOIN attempts ON jobs.batch_id = attempts.batch_id AND jobs.job_id = attempts.job_id AND jobs.attempt_id = attempts.attempt_id
    SET state = 'Ready',
        jobs.attempt_id = NULL
    WHERE instance_name = in_instance_name;

    UPDATE instances SET state = 'inactive', free_cores_mcpu = cores_mcpu WHERE name = in_instance_name;

    COMMIT;
    SELECT 0 as rc;
  ELSE
    ROLLBACK;
    SELECT 1 as rc, cur_state, 'state not live (active or pending)' as message;
  END IF;
END $$

DROP PROCEDURE IF EXISTS add_attempt;
CREATE PROCEDURE add_attempt(
  IN in_batch_id BIGINT,
  IN in_job_id INT,
  IN in_attempt_id VARCHAR(40),
  IN in_instance_name VARCHAR(100),
  IN in_cores_mcpu INT,
  OUT delta_cores_mcpu INT DEFAULT 0
)
BEGIN
  DECLARE attempt_exists BOOLEAN;
  DECLARE cur_instance_state VARCHAR(40);

  START TRANSACTION;

  SET attempt_exists = EXISTS (SELECT * FROM attempts
                               WHERE batch_id = in_batch_id AND
                                 job_id = in_job_id AND attempt_id = in_attempt_id);

  IF NOT attempt_exists AND in_attempt_id IS NOT NULL THEN
    INSERT INTO attempts (batch_id, job_id, attempt_id, instance_name) VALUES (in_batch_id, in_job_id, in_attempt_id, in_instance_name);
    SELECT state INTO cur_instance_state FROM instances WHERE name = in_instance_name;
    IF cur_instance_state = 'active' THEN
      UPDATE instances SET free_cores_mcpu = free_cores_mcpu - in_cores_mcpu WHERE name = in_instance_name;
      SET delta_cores_mcpu = -1 * in_cores_mcpu;
    END IF;
  END IF;
  COMMIT;
END $$

DROP PROCEDURE IF EXISTS schedule_job;
CREATE PROCEDURE schedule_job(
  IN in_batch_id BIGINT,
  IN in_job_id INT,
  IN in_attempt_id VARCHAR(40),
  IN in_instance_name VARCHAR(100)
)
BEGIN
  DECLARE cur_job_state VARCHAR(40);
  DECLARE cur_cores_mcpu INT;
  DECLARE cur_job_cancel BOOLEAN;
  DECLARE cur_instance_state VARCHAR(40);
  DECLARE delta_cores_mcpu INT;

  START TRANSACTION;

  SELECT state, cores_mcpu
    (jobs.cancelled OR batches.cancelled) AND NOT always_run
  INTO cur_job_state, cur_cores_mcpu, cur_job_cancel
  FROM jobs
  INNER JOIN batches ON batches.id = jobs.batch_id
  WHERE batch_id = in_batch_id AND batches.closed
    AND job_id = in_job_id;

  CALL add_attempt(in_batch_id, in_job_id, in_attempt_id, in_instance_name, cur_cores_mcpu, delta_cores_mcpu);

  IF delta_cores_mcpu = 0 THEN
    SET delta_cores_mcpu = cur_cores_mcpu;
  ELSE
    SET delta_cores_mcpu = 0;
  END IF;

  IF cur_job_state = 'Ready' AND NOT cur_job_cancel AND cur_instance_state = 'active' THEN
    UPDATE jobs SET state = 'Running', attempt_id = in_attempt_id WHERE batch_id = in_batch_id AND job_id = in_job_id;
    COMMIT;
    SELECT 0 as rc, in_instance_name, delta_cores_mcpu;
  ELSE
    COMMIT;
    SELECT 1 as rc,
      cur_job_state,
      cur_job_cancel,
      cur_instance_state,
      in_instance_name,
      delta_cores_mcpu,
      'job not Ready or cancelled or instance not active' as message;
  END IF;
END $$

DROP PROCEDURE IF EXISTS unschedule_job;
CREATE PROCEDURE unschedule_job(
  IN in_batch_id BIGINT,
  IN in_job_id INT,
  IN in_attempt_id VARCHAR(40),
  IN in_instance_name VARCHAR(100),
  IN new_end_time BIGINT,
  IN new_reason VARCHAR(40)
)
BEGIN
  DECLARE cur_job_state VARCHAR(40);
  DECLARE cur_instance_state VARCHAR(40);
  DECLARE cur_attempt_id VARCHAR(40);
  DECLARE cur_cores_mcpu INT;
  DECLARE cur_end_time BIGINT;
  DECLARE delta_cores_mcpu INT DEFAULT 0;

  START TRANSACTION;

  SELECT state, cores_mcpu, attempt_id
  INTO cur_job_state, cur_cores_mcpu, cur_attempt_id
  FROM jobs WHERE batch_id = in_batch_id AND job_id = in_job_id;

  SELECT end_time INTO cur_end_time FROM attempts
    WHERE batch_id = in_batch_id AND job_id = in_job_id AND attempt_id = in_attempt_id

  UPDATE attempts
    SET end_time = new_end_time, reason = new_reason
    WHERE batch_id = in_batch_id AND job_id = in_job_id AND attempt_id = in_attempt_id;

  SELECT state INTO cur_instance_state FROM instances WHERE name = in_instance_name;
  IF cur_instance_state = 'active' AND cur_end_time IS NULL THEN
    UPDATE instances
    SET free_cores_mcpu = free_cores_mcpu + cur_cores_mcpu
    WHERE name = in_instance_name;

    SET delta_cores_mcpu = cur_cores_mcpu;
  END IF;

  IF cur_job_state = 'Running' AND cur_attempt_id = in_attempt_id THEN
    UPDATE jobs SET state = 'Ready', attempt_id = NULL WHERE batch_id = in_batch_id AND job_id = in_job_id;
    COMMIT;
    SELECT 0 as rc, delta_cores_mcpu;
  ELSE
    COMMIT;
    SELECT 1 as rc, cur_job_state, delta_cores_mcpu,
      'job state not Running or wrong attempt id' as message;
  END IF;
END $$

DROP PROCEDURE IF EXISTS mark_job_started;
CREATE PROCEDURE mark_job_started(
  IN in_batch_id BIGINT,
  IN in_job_id INT,
  IN in_attempt_id VARCHAR(40),
  IN in_instance_name VARCHAR(100),
  IN new_start_time BIGINT
)
BEGIN
  DECLARE cur_cores_mcpu INT;
  DECLARE cur_instance_state VARCHAR(40);
  DECLARE delta_cores_mcpu INT;

  START TRANSACTION;

  SELECT cores_mcpu INTO cur_cores_mcpu
  FROM jobs
  INNER JOIN batches ON batches.id = jobs.batch_id
  WHERE batch_id = in_batch_id AND batches.closed
    AND job_id = in_job_id;

  CALL add_attempt(in_batch_id, in_job_id, in_attempt_id, in_instance_name, cur_cores_mcpu, delta_cores_mcpu);

  UPDATE attempts SET start_time = new_start_time
  WHERE batch_id = in_batch_id AND job_id = in_job_id AND attempt_id = in_attempt_id;

  COMMIT;
  SELECT 0 as rc, delta_cores_mcpu;

END $$

DROP PROCEDURE IF EXISTS mark_job_complete;
CREATE PROCEDURE mark_job_complete(
  IN in_batch_id BIGINT,
  IN in_job_id INT,
  IN in_attempt_id VARCHAR(40),
  IN in_instance_name VARCHAR(40),
  IN new_state VARCHAR(40),
  IN new_status VARCHAR(65535),
  IN new_start_time BIGINT,
  IN new_end_time BIGINT,
  IN new_reason VARCHAR(40),
  IN new_timestamp BIGINT
)
BEGIN
  DECLARE cur_job_state VARCHAR(40);
  DECLARE cur_instance_state VARCHAR(40);
  DECLARE cur_cores_mcpu INT;
  DECLARE cur_end_time BIGINT;
  DECLARE delta_cores_mcpu INT DEFAULT 0;

  START TRANSACTION;

  SELECT state, cores_mcpu
  INTO cur_job_state, cur_cores_mcpu
  FROM jobs
  WHERE batch_id = in_batch_id AND job_id = in_job_id;

  SELECT end_time INTO cur_end_time FROM attempts
    WHERE batch_id = in_batch_id AND job_id = in_job_id AND attempt_id = in_attempt_id

  CALL add_attempt(in_batch_id, in_job_id, in_attempt_id, in_instance_name, cur_cores_mcpu, delta_cores_mcpu);

  UPDATE attempts
  SET start_time = new_start_time, end_time = new_end_time, reason = new_reason
  WHERE batch_id = in_batch_id AND job_id = in_job_id AND attempt_id = in_attempt_id;

  SELECT state INTO cur_instance_state FROM instances WHERE name = in_instance_name;
  IF cur_instance_state = 'active' AND cur_end_time IS NULL THEN
    UPDATE instances
    SET free_cores_mcpu = free_cores_mcpu + cur_cores_mcpu
    WHERE name = in_instance_name;

    SET delta_cores_mcpu = delta_cores_mcpu + cur_cores_mcpu;
  END IF;

  IF cur_job_state = 'Ready' OR cur_job_state = 'Running' THEN  
    UPDATE jobs
    SET state = new_state, status = new_status, attempt_id = NULL
    WHERE batch_id = in_batch_id AND job_id = in_job_id;

    UPDATE batches SET n_completed = n_completed + 1 WHERE id = in_batch_id;
    UPDATE batches SET time_completed = new_timestamp
      WHERE id = in_batch_id AND n_completed = batches.n_jobs;

    IF new_state = 'Cancelled' THEN
      UPDATE batches SET n_cancelled = n_cancelled + 1 WHERE id = in_batch_id;
    ELSEIF new_state = 'Error' OR new_state = 'Failed' THEN
      UPDATE batches SET n_failed = n_failed + 1 WHERE id = in_batch_id;
    ELSE
      UPDATE batches SET n_succeeded = n_succeeded + 1 WHERE id = in_batch_id;
    END IF;

    UPDATE jobs
      INNER JOIN `job_parents`
        ON jobs.batch_id = `job_parents`.batch_id AND
           jobs.job_id = `job_parents`.job_id
      SET jobs.state = IF(jobs.n_pending_parents = 1, 'Ready', 'Pending'),
          jobs.n_pending_parents = jobs.n_pending_parents - 1,
          jobs.cancelled = IF(new_state = 'Success', jobs.cancelled, 1)
      WHERE jobs.batch_id = in_batch_id AND
            `job_parents`.batch_id = in_batch_id AND
            `job_parents`.parent_id = in_job_id;

    COMMIT;
    SELECT 0 as rc,
      cur_job_state as old_state,
      delta_cores_mcpu;
  ELSEIF cur_job_state = 'Cancelled' OR cur_job_state = 'Error' OR
         cur_job_state = 'Failed' OR cur_job_state = 'Success' THEN
    COMMIT;
    SELECT 0 as rc,
      cur_job_state as old_state,
      delta_cores_mcpu;
  ELSE
    COMMIT;
    SELECT 1 as rc,
      cur_job_state,
      delta_cores_mcpu,
      'job state not Ready, Running or complete' as message;
  END IF;
END $$

DELIMITER ;
