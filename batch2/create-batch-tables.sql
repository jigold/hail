CREATE TABLE IF NOT EXISTS `tokens` (
  `name` VARCHAR(100) NOT NULL,
  `token` VARCHAR(100) NOT NULL,
  PRIMARY KEY (`name`)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `instances` (
  `name` VARCHAR(100) NOT NULL,
  `state` VARCHAR(40) NOT NULL,
  `activation_token` VARCHAR(100),
  `token` VARCHAR(100) NOT NULL,
  `cores_mcpu` INT NOT NULL,
  `free_cores_mcpu` INT NOT NULL,
  `time_created` DOUBLE NOT NULL,
  `failed_request_count` INT NOT NULL DEFAULT 0,
  `last_updated` DOUBLE NOT NULL,
  `ip_address` VARCHAR(100),
  PRIMARY KEY (`name`)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `batches` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `userdata` VARCHAR(65535) NOT NULL,
  `user` VARCHAR(100) NOT NULL,
  `attributes` VARCHAR(65535),
  `callback` VARCHAR(65535),
  `deleted` BOOLEAN NOT NULL DEFAULT FALSE,
  `cancelled` BOOLEAN NOT NULL DEFAULT FALSE,
  `closed` BOOLEAN NOT NULL DEFAULT FALSE,
  `n_jobs` INT NOT NULL,
  `n_completed` INT NOT NULL DEFAULT 0,
  `n_succeeded` INT NOT NULL DEFAULT 0,
  `n_failed` INT NOT NULL DEFAULT 0,
  `n_cancelled` INT NOT NULL DEFAULT 0,
  `time_created` DOUBLE NOT NULL,
  `time_completed` DOUBLE,
  `msec_mcpu` BIGINT NOT NULL DEFAULT 0,
  `cost` DOUBLE NOT NULL DEFAULT 0
  PRIMARY KEY (`id`)
) ENGINE = InnoDB;
CREATE INDEX `batches_user` ON `batches` (`user`);
CREATE INDEX `batches_deleted` ON `batches` (`deleted`);

CREATE TABLE IF NOT EXISTS `jobs` (
  `batch_id` BIGINT NOT NULL,
  `job_id` INT NOT NULL,
  `state` VARCHAR(40) NOT NULL,
  `spec` VARCHAR(65535) NOT NULL,
  `always_run` BOOLEAN NOT NULL,
  `cores_mcpu` INT NOT NULL,
  `instance_name` VARCHAR(100),
  `status` VARCHAR(65535),
  `n_pending_parents` INT NOT NULL,
  `cancelled` BOOLEAN NOT NULL DEFAULT FALSE,
  `msec_mcpu` BIGINT NOT NULL DEFAULT 0,
  `cost` DOUBLE NOT NULL DEFAULT 0,
  PRIMARY KEY (`batch_id`, `job_id`),
  FOREIGN KEY (`batch_id`) REFERENCES batches(id) ON DELETE CASCADE,
  FOREIGN KEY (`instance_name`) REFERENCES instances(name)
) ENGINE = InnoDB;
CREATE INDEX `jobs_state` ON `jobs` (`state`);
CREATE INDEX `jobs_instance_name` ON `jobs` (`instance_name`);

CREATE TABLE IF NOT EXISTS `attempts` (
  `batch_id` BIGINT NOT NULL,
  `job_id` INT NOT NULL,
  `attempt_id` VARCHAR(40) NOT NULL,
  `start_time` DOUBLE,
  `end_time` DOUBLE,
  `reason` VARCHAR(40)
  PRIMARY KEY (`batch_id`, `job_id`, `attempt_id`),
  FOREIGN KEY (`batch_id`) REFERENCES batches(id) ON DELETE CASCADE,
  FOREIGN KEY (`batch_id`, `job_id`) REFERENCES jobs(batch_id, job_id) ON DELETE CASCADE
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `ready_cores` (
  ready_cores_mcpu INT NOT NULL
) ENGINE = InnoDB;

INSERT INTO ready_cores (ready_cores_mcpu) VALUES (0);

CREATE TABLE IF NOT EXISTS `gevents_mark` (
  mark VARCHAR(40)
) ENGINE = InnoDB;

INSERT INTO `gevents_mark` (mark) VALUES (NULL);

CREATE TABLE IF NOT EXISTS `job_parents` (
  `batch_id` BIGINT NOT NULL,
  `job_id` INT NOT NULL,
  `parent_id` INT NOT NULL,
  PRIMARY KEY (`batch_id`, `job_id`, `parent_id`),
  FOREIGN KEY (`batch_id`) REFERENCES batches(id) ON DELETE CASCADE,
  FOREIGN KEY (`batch_id`, `job_id`) REFERENCES jobs(batch_id, job_id) ON DELETE CASCADE
) ENGINE = InnoDB;
CREATE INDEX job_parents_parent_id ON `job_parents` (batch_id, parent_id);

CREATE TABLE IF NOT EXISTS `job_attributes` (
  `batch_id` BIGINT NOT NULL,
  `job_id` INT NOT NULL,
  `key` VARCHAR(100) NOT NULL,
  `value` VARCHAR(65535),
  PRIMARY KEY (`batch_id`, `job_id`, `key`),
  FOREIGN KEY (`batch_id`) REFERENCES batches(id) ON DELETE CASCADE,
  FOREIGN KEY (`batch_id`, `job_id`) REFERENCES jobs(batch_id, job_id) ON DELETE CASCADE
) ENGINE = InnoDB;
CREATE INDEX job_attributes_key_value ON `job_attributes` (`key`, `value`(256));

CREATE TABLE IF NOT EXISTS `batch_attributes` (
  `batch_id` BIGINT NOT NULL,
  `key` VARCHAR(100) NOT NULL,
  `value` VARCHAR(65535),
  PRIMARY KEY (`batch_id`, `key`),
  FOREIGN KEY (`batch_id`) REFERENCES batches(id) ON DELETE CASCADE  
) ENGINE = InnoDB;
CREATE INDEX batch_attributes_key_value ON `batch_attributes` (`key`, `value`(256));

DELIMITER $$

CREATE TRIGGER attempts_before_update BEFORE UPDATE ON attempts
FOR EACH ROW
BEGIN
  IF OLD.start_time IS NOT NULL AND (NEW.start_time IS NULL OR NEW.start_time > OLD.start_time) THEN
    SET NEW.start_time = OLD.start_time;
  END IF;

  IF OLD.end_time IS NOT NULL AND (NEW.end_time IS NULL OR NEW.end_time > OLD.end_time) THEN
    SET NEW.end_time = OLD.end_time;
    SET NEW.reason = OLD.reason;
  END IF;
END $$

CREATE TRIGGER attempts_after_update AFTER UPDATE ON attempts
FOR EACH ROW
BEGIN
  DECLARE cores_mcpu INT;
  DECLARE time_diff DOUBLE;
  DECLARE cost_per_core_sec DOUBLE;
  DECLARE new_msec_mcpu BIGINT;

  SET cost_per_core_sec = 0.01 / 3600;

  SELECT cores_mcpu INTO cores_mcpu FROM jobs
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id;

  SET time_diff = COALESCE(NEW.end_time - NEW.start_time, 0) -
                  COALESCE(OLD.end_time - OLD.start_time, 0);

  SET new_msec_mcpu = (time_diff * 1000) * cores_mcpu;

  UPDATE batches
  SET msec_mcpu = batches.msec_mcpu + new_msec_mcpu
  WHERE id = NEW.batch_id;

  UPDATE batches
  SET cost = batches.msec_mcpu * cost_per_core_sec * 0.001 * 0.001
  WHERE id = NEW.batch_id;

  UPDATE jobs
  SET msec_mcpu = jobs.msec_mcpu + new_msec_mcpu
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id;

  UPDATE jobs
  SET cost = jobs.msec_mcpu * cost_per_core_sec * 0.001 * 0.001
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id;
END $$

CREATE PROCEDURE activate_instance(
  IN in_instance_name VARCHAR(100),
  IN in_ip_address VARCHAR(100)
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
      ip_address = in_ip_address WHERE name = in_instance_name;
    COMMIT;
    SELECT 0 as rc, cur_token as token;
  ELSE
    ROLLBACK;
    SELECT 1 as rc, cur_state, 'state not pending' as message;
  END IF;
END $$

CREATE PROCEDURE deactivate_instance(
  IN in_instance_name VARCHAR(100)
)
BEGIN
  DECLARE cur_state VARCHAR(40);

  START TRANSACTION;

  SELECT state INTO cur_state FROM instances WHERE name = in_instance_name;

  IF cur_state = 'pending' or cur_state = 'active' THEN
    UPDATE ready_cores
    SET ready_cores_mcpu = ready_cores_mcpu +
      COALESCE(
        (SELECT SUM(cores_mcpu)
         FROM jobs
         WHERE instance_name = in_instance_name),
	0);

    UPDATE jobs
    SET state = 'Ready',
        instance_name = NULL
    WHERE instance_name = in_instance_name;

    UPDATE instances SET state = 'inactive', free_cores_mcpu = cores_mcpu WHERE name = in_instance_name;

    COMMIT;
    SELECT 0 as rc;
  ELSE
    ROLLBACK;
    SELECT 1 as rc, cur_state, 'state not live (active or pending)' as message;
  END IF;
END $$

CREATE PROCEDURE mark_instance_deleted(
  IN in_instance_name VARCHAR(100)
)
BEGIN
  DECLARE cur_state VARCHAR(40);

  START TRANSACTION;

  SELECT state INTO cur_state FROM instances WHERE name = in_instance_name;

  IF cur_state = 'inactive' THEN
    UPDATE instances SET state = 'deleted' WHERE name = in_instance_name;
    COMMIT;
    SELECT 0 as rc;
  ELSE
    ROLLBACK;
    SELECT 1 as rc, cur_state, 'state not inactive' as message;
  END IF;
END $$

CREATE PROCEDURE close_batch(
  IN in_batch_id BIGINT
)
BEGIN
  DECLARE cur_batch_closed BOOLEAN;
  DECLARE expected_n_jobs INT;
  DECLARE actual_n_jobs INT;

  START TRANSACTION;

  SELECT n_jobs, closed INTO expected_n_jobs, cur_batch_closed FROM batches
  WHERE id = in_batch_id AND NOT deleted;

  IF cur_batch_closed = 1 THEN
    COMMIT;
    SELECT 0 as rc;
  ELSEIF cur_batch_closed = 0 THEN
    SELECT COUNT(*) INTO actual_n_jobs FROM jobs
    WHERE batch_id = in_batch_id;

    IF actual_n_jobs = expected_n_jobs THEN
      UPDATE batches SET closed = 1 WHERE id = in_batch_id;
      UPDATE batches SET time_completed = UNIX_TIMESTAMP(NOW(3))
        WHERE id = in_batch_id AND n_completed = batches.n_jobs;
      UPDATE ready_cores
	SET ready_cores_mcpu = ready_cores_mcpu +
	  COALESCE(
	    (SELECT SUM(cores_mcpu) FROM jobs
	     WHERE jobs.state = 'Ready' AND jobs.batch_id = in_batch_id),
	    0);
      COMMIT;
      SELECT 0 as rc;
    ELSE
      ROLLBACK;
      SELECT 2 as rc, expected_n_jobs, actual_n_jobs, 'wrong number of jobs' as message;
    END IF;
  ELSE
    ROLLBACK;
    SELECT 1 as rc, cur_batch_closed, 'batch closed is not 0 or 1' as message;
  END IF;
END $$

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

  START TRANSACTION;

  SELECT state, cores_mcpu,
    (jobs.cancelled OR batches.cancelled) AND NOT always_run
  INTO cur_job_state, cur_cores_mcpu, cur_job_cancel
  FROM jobs
  INNER JOIN batches ON batches.id = jobs.batch_id
  WHERE batch_id = in_batch_id AND batches.closed
    AND job_id = in_job_id;

  SELECT state INTO cur_instance_state FROM instances WHERE name = in_instance_name;

  IF cur_job_state = 'Ready' AND NOT cur_job_cancel AND cur_instance_state = 'active' THEN
    UPDATE jobs SET state = 'Running', instance_name = in_instance_name WHERE batch_id = in_batch_id AND job_id = in_job_id;
    INSERT INTO attempts (batch_id, job_id, attempt_id) VALUES (in_batch_id, in_job_id, in_attempt_id);
    UPDATE ready_cores SET ready_cores_mcpu = ready_cores_mcpu - cur_cores_mcpu;
    UPDATE instances SET free_cores_mcpu = free_cores_mcpu - cur_cores_mcpu WHERE name = in_instance_name;
    COMMIT;
    SELECT 0 as rc, in_instance_name;
  ELSE
    ROLLBACK;
    SELECT 1 as rc,
      cur_job_state,
      cur_job_cancel,
      cur_instance_state,
      in_instance_name,
      'job not Ready or cancelled or instance not active' as message;
  END IF;
END $$

CREATE PROCEDURE unschedule_job(
  IN in_batch_id BIGINT,
  IN in_job_id INT,
  IN expected_instance_name VARCHAR(100),
  IN new_end_time DOUBLE,
  IN new_reason VARCHAR(40)
)
BEGIN
  DECLARE cur_job_state VARCHAR(40);
  DECLARE cur_job_instance_name VARCHAR(100);
  DECLARE cur_cores_mcpu INT;

  START TRANSACTION;

  SELECT state, cores_mcpu, instance_name
  INTO cur_job_state, cur_cores_mcpu, cur_job_instance_name
  FROM jobs WHERE batch_id = in_batch_id AND job_id = in_job_id;

  IF cur_job_state = 'Running' AND cur_job_instance_name = expected_instance_name THEN
    UPDATE jobs SET state = 'Ready', instance_name = NULL WHERE batch_id = in_batch_id AND job_id = in_job_id;
    UPDATE ready_cores SET ready_cores_mcpu = ready_cores_mcpu + cur_cores_mcpu;
    UPDATE instances SET free_cores_mcpu = free_cores_mcpu + cur_cores_mcpu WHERE name = cur_job_instance_name;
    UPDATE attempts SET end_time = new_end_time, reason = new_reason WHERE batch_id = in_batch_id AND job_id = in_job_id;
    COMMIT;
    SELECT 0 as rc;
  ELSE
    ROLLBACK;
    SELECT 1 as rc, cur_job_state, cur_job_instance_name, expected_instance_name,
      'job state not Running or wrong instance' as message;
  END IF;
END $$

CREATE PROCEDURE mark_job_complete(
  IN in_batch_id BIGINT,
  IN in_job_id INT,
  IN in_attempt_id VARCHAR(40),
  IN new_state VARCHAR(40),
  IN new_status VARCHAR(65535),
  IN new_start_time DOUBLE,
  IN new_end_time DOUBLE,
  IN new_reason VARCHAR(40)
)
BEGIN
  DECLARE cur_job_state VARCHAR(40);
  DECLARE cur_job_instance_name VARCHAR(100);
  DECLARE cur_cores_mcpu INT;

  START TRANSACTION;

  SELECT state, cores_mcpu, instance_name
  INTO cur_job_state, cur_cores_mcpu, cur_job_instance_name
  FROM jobs
  WHERE batch_id = in_batch_id AND job_id = in_job_id;

  IF cur_job_state = 'Ready' OR cur_job_state = 'Running' THEN
    UPDATE jobs
    SET state = new_state, status = new_status, instance_name = NULL
    WHERE batch_id = in_batch_id AND job_id = in_job_id;

    UPDATE batches SET n_completed = n_completed + 1 WHERE id = in_batch_id;
    UPDATE batches SET time_completed = UNIX_TIMESTAMP(NOW(3))
      WHERE id = in_batch_id AND n_completed = batches.n_jobs;

    IF new_state = 'Cancelled' THEN
      UPDATE batches SET n_cancelled = n_cancelled + 1 WHERE id = in_batch_id;
    ELSEIF new_state = 'Error' OR new_state = 'Failed' THEN
      UPDATE batches SET n_failed = n_failed + 1 WHERE id = in_batch_id;
    ELSE
      UPDATE batches SET n_succeeded = n_succeeded + 1 WHERE id = in_batch_id;
    END IF;

    IF cur_job_instance_name IS NOT NULL THEN
      UPDATE instances
      SET free_cores_mcpu = free_cores_mcpu + cur_cores_mcpu
      WHERE name = cur_job_instance_name;
    END IF;

    IF cur_job_state = 'Ready' THEN
      UPDATE ready_cores SET ready_cores_mcpu = ready_cores_mcpu - cur_cores_mcpu;
    END IF;
    UPDATE ready_cores
      SET ready_cores_mcpu = ready_cores_mcpu +
        COALESCE(
	  (SELECT SUM(jobs.cores_mcpu) FROM jobs
	   INNER JOIN `job_parents`
	     ON jobs.batch_id = `job_parents`.batch_id AND
		jobs.job_id = `job_parents`.job_id
	   WHERE jobs.batch_id = in_batch_id AND
		 `job_parents`.batch_id = in_batch_id AND
		 `job_parents`.parent_id = in_job_id AND
		 jobs.n_pending_parents = 1),
          0);

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

    IF in_attempt_id IS NOT NULL THEN
      UPDATE attempts
      SET start_time = new_start_time, end_time = new_end_time, reason = new_reason
      WHERE batch_id = in_batch_id AND job_id = in_job_id AND attempt_id = in_attempt_id;
    END IF;

    COMMIT;
    SELECT 0 as rc,
      cur_job_state as old_state,
      cur_cores_mcpu as cores_mcpu,
      cur_job_instance_name as instance_name;
  ELSEIF cur_job_state = 'Cancelled' OR cur_job_state = 'Error' OR
         cur_job_state = 'Failed' OR cur_job_state = 'Success' THEN
    COMMIT;
    SELECT 0 as rc,
      cur_job_state as old_state;
  ELSE
    ROLLBACK;
    SELECT 1 as rc, cur_job_state, 'job state not Ready, Running or complete' as message;
  END IF;
END $$

DELIMITER ;
