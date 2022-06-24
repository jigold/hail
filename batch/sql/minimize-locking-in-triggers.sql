ALTER TABLE instances_free_cores_mcpu ADD COLUMN token INT DEFAULT 0;
ALTER TABLE instances_free_cores_mcpu DROP PRIMARY KEY, ADD PRIMARY KEY(name, token);

DELIMITER $$

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

  INSERT INTO aggregated_billing_project_resources (billing_project, resource, token, `usage`)
  VALUES (cur_billing_project, NEW.resource, rand_token, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_batch_resources (batch_id, resource, token, `usage`)
  VALUES (NEW.batch_id, NEW.resource, rand_token, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_job_resources (batch_id, job_id, resource, `usage`)
  VALUES (NEW.batch_id, NEW.job_id, NEW.resource, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;
END $$


DROP PROCEDURE IF EXISTS add_attempt $$
CREATE PROCEDURE add_attempt(
  IN in_batch_id BIGINT,
  IN in_job_id INT,
  IN in_attempt_id VARCHAR(40),
  IN in_instance_name VARCHAR(100),
  IN in_cores_mcpu INT,
  OUT delta_cores_mcpu INT
)
BEGIN
  DECLARE cur_instance_state VARCHAR(40);
--   DECLARE dummy_lock INT;
  DECLARE rand_token INT;
  DECLARE cur_n_tokens INT;

  SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
  SET rand_token = FLOOR(RAND() * cur_n_tokens);

  SET delta_cores_mcpu = IFNULL(delta_cores_mcpu, 0);

  IF in_attempt_id IS NOT NULL THEN
    # dummy lock to avoid deadlocks
    INSERT INTO instances_free_cores_mcpu (name, token, free_cores_mcpu)
    VALUES (in_instance_name, token, free_cores_mcpu)
    ON DUPLICATE KEY UPDATE free_cores_mcpu = free_cores_mcpu + 0;

--     SELECT 1 INTO dummy_lock FROM instances_free_cores_mcpu
--     WHERE name = in_instance_name AND token = rand_token
--     FOR UPDATE;

    INSERT INTO attempts (batch_id, job_id, attempt_id, instance_name)
    VALUES (in_batch_id, in_job_id, in_attempt_id, in_instance_name)
    ON DUPLICATE KEY UPDATE batch_id = batch_id;

    IF ROW_COUNT() != 0 THEN
      SELECT state INTO cur_instance_state
      FROM instances
      WHERE name = in_instance_name
      LOCK IN SHARE MODE;

      IF cur_instance_state = 'pending' OR cur_instance_state = 'active' THEN
        INSERT INTO instances_free_cores_mcpu (name, token, free_cores_mcpu)
        VALUES (in_instance_name, rand_token, -in_cores_mcpu)
        ON DUPLICATE KEY UPDATE free_cores_mcpu = free_cores_mcpu - in_cores_mcpu;

--         UPDATE instances_free_cores_mcpu
--         SET free_cores_mcpu = free_cores_mcpu - in_cores_mcpu
--         WHERE instances_free_cores_mcpu.name = in_instance_name AND token = rand_token;
      END IF;

      SET delta_cores_mcpu = -1 * in_cores_mcpu;
    END IF;
  END IF;
END $$

DROP PROCEDURE IF EXISTS deactivate_instance $$
CREATE PROCEDURE deactivate_instance(
  IN in_instance_name VARCHAR(100),
  IN in_reason VARCHAR(40),
  IN in_timestamp BIGINT
)
BEGIN
  DECLARE cur_state VARCHAR(40);
  DECLARE cur_free_cores INT;
  DECLARE cur_cores_mcpu INT;

  START TRANSACTION;

  SELECT state INTO cur_state FROM instances WHERE name = in_instance_name FOR UPDATE;

  SELECT cores_mcpu INTO cur_cores_mcpu
  FROM instances
  WHERE name = in_instance_name
  FOR UPDATE;

  SELECT SUM(free_cores_mcpu) INTO cur_free_cores
  FROM instances_free_cores_mcpu
  WHERE name = in_instance_name
  GROUP BY name
  FOR UPDATE;

  UPDATE instances
  SET time_deactivated = in_timestamp
  WHERE name = in_instance_name;

  UPDATE attempts
  SET end_time = in_timestamp, reason = in_reason
  WHERE instance_name = in_instance_name;

  IF cur_state = 'pending' or cur_state = 'active' THEN
    UPDATE instances
    SET state = 'inactive'
    WHERE instances.name = in_instance_name;

    INSERT INTO instances_free_cores_mcpu (name, token, free_cores_mcpu)
    VALUES (in_instance_name, 0, cur_cores_mcpu - cur_free_cores)
    ON DUPLICATE KEY UPDATE free_cores_mcpu = free_cores_mcpu + (cur_cores_mcpu - cur_free_cores);

--     UPDATE instances_free_cores_mcpu
--     SET free_cores_mcpu = cores_mcpu
--     WHERE name = in_instance_name;

    UPDATE jobs
    INNER JOIN attempts ON jobs.batch_id = attempts.batch_id AND jobs.job_id = attempts.job_id AND jobs.attempt_id = attempts.attempt_id
    SET state = 'Ready',
        jobs.attempt_id = NULL
    WHERE instance_name = in_instance_name AND (state = 'Running' OR state = 'Creating');

    COMMIT;
    SELECT 0 as rc;
  ELSE
    ROLLBACK;
    SELECT 1 as rc, cur_state, 'state not live (active or pending)' as message;
  END IF;
END $$

DROP PROCEDURE IF EXISTS unschedule_job $$
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
  DECLARE rand_token INT;
  DECLARE cur_n_tokens INT;

  START TRANSACTION;

  SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
  SET rand_token = FLOOR(RAND() * cur_n_tokens);

  SELECT state, cores_mcpu, attempt_id
  INTO cur_job_state, cur_cores_mcpu, cur_attempt_id
  FROM jobs
  WHERE batch_id = in_batch_id AND job_id = in_job_id
  FOR UPDATE;

  SELECT end_time INTO cur_end_time
  FROM attempts
  WHERE batch_id = in_batch_id AND job_id = in_job_id AND attempt_id = in_attempt_id
  FOR UPDATE;

  UPDATE attempts
  SET end_time = new_end_time, reason = new_reason
  WHERE batch_id = in_batch_id AND job_id = in_job_id AND attempt_id = in_attempt_id;

  SELECT state INTO cur_instance_state FROM instances WHERE name = in_instance_name LOCK IN SHARE MODE;

  IF cur_instance_state = 'active' AND cur_end_time IS NULL THEN
    INSERT INTO instances_free_cores_mcpu (name, token, free_cores_mcpu)
    VALUES (in_instance_name, rand_token, cur_cores_mcpu)
    ON DUPLICATE KEY UPDATE free_cores_mcpu = free_cores_mcpu + cur_cores_mcpu;

--     UPDATE instances_free_cores_mcpu
--     SET free_cores_mcpu = free_cores_mcpu + cur_cores_mcpu
--     WHERE instances_free_cores_mcpu.name = in_instance_name;

    SET delta_cores_mcpu = cur_cores_mcpu;
  END IF;

  IF (cur_job_state = 'Creating' OR cur_job_state = 'Running') AND cur_attempt_id = in_attempt_id THEN
    UPDATE jobs SET state = 'Ready', attempt_id = NULL WHERE batch_id = in_batch_id AND job_id = in_job_id;
    COMMIT;
    SELECT 0 as rc, delta_cores_mcpu;
  ELSE
    COMMIT;
    SELECT 1 as rc, cur_job_state, delta_cores_mcpu,
      'job state not Running or Creating or wrong attempt id' as message;
  END IF;
END $$

DROP PROCEDURE IF EXISTS mark_job_complete $$
CREATE PROCEDURE mark_job_complete(
  IN in_batch_id BIGINT,
  IN in_job_id INT,
  IN in_attempt_id VARCHAR(40),
  IN in_instance_name VARCHAR(100),
  IN new_state VARCHAR(40),
  IN new_status TEXT,
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
  DECLARE total_jobs_in_batch INT;
  DECLARE expected_attempt_id VARCHAR(40);
  DECLARE rand_token INT;
  DECLARE cur_n_tokens INT;

  START TRANSACTION;

  SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
  SET rand_token = FLOOR(RAND() * cur_n_tokens);

  SELECT n_jobs INTO total_jobs_in_batch FROM batches WHERE id = in_batch_id;

  SELECT state, cores_mcpu
  INTO cur_job_state, cur_cores_mcpu
  FROM jobs
  WHERE batch_id = in_batch_id AND job_id = in_job_id
  FOR UPDATE;

  CALL add_attempt(in_batch_id, in_job_id, in_attempt_id, in_instance_name, cur_cores_mcpu, delta_cores_mcpu);

  SELECT end_time INTO cur_end_time FROM attempts
  WHERE batch_id = in_batch_id AND job_id = in_job_id AND attempt_id = in_attempt_id
  FOR UPDATE;

  UPDATE attempts
  SET start_time = new_start_time, end_time = new_end_time, reason = new_reason
  WHERE batch_id = in_batch_id AND job_id = in_job_id AND attempt_id = in_attempt_id;

  SELECT state INTO cur_instance_state FROM instances WHERE name = in_instance_name LOCK IN SHARE MODE;
  IF cur_instance_state = 'active' AND cur_end_time IS NULL THEN
    INSERT INTO instances_free_cores_mcpu (name, token, free_cores_mcpu)
    VALUES (in_instance_name, rand_token, cur_cores_mcpu)
    ON DUPLICATE KEY UPDATE free_cores_mcpu = free_cores_mcpu + cur_cores_mcpu;

--     UPDATE instances_free_cores_mcpu
--     SET free_cores_mcpu = free_cores_mcpu + cur_cores_mcpu
--     WHERE instances_free_cores_mcpu.name = in_instance_name;

    SET delta_cores_mcpu = delta_cores_mcpu + cur_cores_mcpu;
  END IF;

  SELECT attempt_id INTO expected_attempt_id FROM jobs
  WHERE batch_id = in_batch_id AND job_id = in_job_id
  FOR UPDATE;

  IF expected_attempt_id IS NOT NULL AND expected_attempt_id != in_attempt_id THEN
    COMMIT;
    SELECT 2 as rc,
      expected_attempt_id,
      delta_cores_mcpu,
      'input attempt id does not match expected attempt id' as message;
  ELSEIF cur_job_state = 'Ready' OR cur_job_state = 'Creating' OR cur_job_state = 'Running' THEN
    UPDATE jobs
    SET state = new_state, status = new_status, attempt_id = in_attempt_id
    WHERE batch_id = in_batch_id AND job_id = in_job_id;

    UPDATE batches_n_jobs_in_complete_states
      SET n_completed = (@new_n_completed := n_completed + 1),
          n_cancelled = n_cancelled + (new_state = 'Cancelled'),
          n_failed    = n_failed + (new_state = 'Error' OR new_state = 'Failed'),
          n_succeeded = n_succeeded + (new_state != 'Cancelled' AND new_state != 'Error' AND new_state != 'Failed')
      WHERE id = in_batch_id;

    # Grabbing an exclusive lock on batches here could deadlock,
    # but this IF should only execute for the last job
    IF @new_n_completed = total_jobs_in_batch THEN
      UPDATE batches
      SET time_completed = new_timestamp,
          `state` = 'complete'
      WHERE id = in_batch_id;
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
      'job state not Ready, Creating, Running or complete' as message;
  END IF;
END $$

DELIMITER ;
