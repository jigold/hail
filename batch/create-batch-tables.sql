CREATE TABLE IF NOT EXISTS `batch` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `userdata` TEXT(65535) NOT NULL,
  `user` VARCHAR(100) NOT NULL,
  `attributes` TEXT(65535),
  `callback` TEXT(65535),
  `ttl` INT,
  `is_open` BOOLEAN NOT NULL,
  `deleted` BOOLEAN NOT NULL default false,
  `n_jobs` INT NOT NULL default 0,
  `n_completed` INT NOT NULL default 0,
  `n_succeeded` INT NOT NULL default 0,
  `n_failed` INT NOT NULL default 0,
  `n_cancelled` INT NOT NULL default 0,
  `time_created` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE = InnoDB;
CREATE INDEX batch_user ON batch (user);

CREATE TABLE IF NOT EXISTS `jobs` (
  `batch_id` BIGINT NOT NULL,
  `job_id` INT NOT NULL,
  `state` VARCHAR(40) NOT NULL,
  `input_exit_code` INT,
  `main_exit_code` INT,
  `output_exit_code` INT,
  `batch_id` BIGINT NOT NULL,
  `pod_name` VARCHAR(1024),
  `pvc_name` TEXT(65535),
  `callback` TEXT(65535),
  `task_idx` INT NOT NULL,
  `always_run` BOOLEAN NOT NULL,
  `cancelled` BOOLEAN NOT NULL,
  `time_created` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  `duration` BIGINT,
  `userdata` TEXT(65535) NOT NULL,
  `user` VARCHAR(100) NOT NULL,
  `attributes` TEXT(65535),
  `tasks` TEXT(65535) NOT NULL,
  `input_log_uri` VARCHAR(1024),
  `main_log_uri` VARCHAR(1024),
  `output_log_uri` VARCHAR(1024),
  PRIMARY KEY (`batch_id`, `job_id`),
  FOREIGN KEY (`batch_id`) REFERENCES batch(id) ON DELETE CASCADE
) ENGINE = InnoDB;
CREATE INDEX jobs_user ON jobs (user);
CREATE INDEX jobs_batch ON jobs (batch_id);

CREATE TABLE IF NOT EXISTS `jobs-parents` (
  `batch_id` BIGINT NOT NULL,
  `job_id` INT NOT NULL,
  `parent_id` INT NOT NULL,
  PRIMARY KEY (`batch_id`, `job_id`, `parent_id`),
  FOREIGN KEY (`batch_id`) REFERENCES batch(id) ON DELETE CASCADE
) ENGINE = InnoDB;
CREATE INDEX jobs_parents_job_id ON `jobs-parents` (batch_id, job_id);
CREATE INDEX jobs_parents_parent_id ON `jobs-parents` (batch_id, parent_id);

DELIMITER $$

CREATE TRIGGER trigger_jobs_insert AFTER INSERT ON jobs
    FOR EACH ROW BEGIN
        UPDATE batch SET n_jobs = n_jobs + 1 WHERE id = new.batch_id;
        IF (NEW.state LIKE 'Complete' OR NEW.state LIKE 'Cancelled') THEN
            UPDATE batch SET n_completed = n_completed + 1 WHERE id = NEW.batch_id;
            IF (NEW.state LIKE 'Complete' AND (NEW.input_exit_code > 0 OR NEW.main_exit_code > 0 OR NEW.output_exit_code > 0)) THEN
	        UPDATE batch SET n_failed = n_failed + 1 WHERE id = NEW.batch_id;
            ELSEIF (NEW.state LIKE 'Complete') THEN
                UPDATE batch SET n_succeeded = n_succeeded + 1 WHERE id = NEW.batch_id;
	    ELSEIF (NEW.state LIKE 'Cancelled') THEN
                UPDATE batch SET n_cancelled = n_cancelled + 1 WHERE id = NEW.batch_id;
	    END IF;
        END IF;
    END;
$$

CREATE TRIGGER trigger_jobs_update AFTER UPDATE ON jobs
    FOR EACH ROW BEGIN
        IF (OLD.state NOT LIKE NEW.state) AND (NEW.state LIKE 'Complete' OR NEW.state LIKE 'Cancelled') THEN
            UPDATE batch SET n_completed = n_completed + 1 WHERE id = NEW.batch_id;
            IF (NEW.state LIKE 'Complete' AND (NEW.input_exit_code > 0 OR NEW.main_exit_code > 0 OR NEW.output_exit_code > 0)) THEN
	        UPDATE batch SET n_failed = n_failed + 1 WHERE id = NEW.batch_id;
            ELSEIF (NEW.state LIKE 'Complete') THEN
                UPDATE batch SET n_succeeded = n_succeeded + 1 WHERE id = NEW.batch_id;
	    ELSEIF (NEW.state LIKE 'Cancelled') THEN
                UPDATE batch SET n_cancelled = n_cancelled + 1 WHERE id = NEW.batch_id;
	    END IF;
        END IF;
    END;
$$

DELIMITER ;
