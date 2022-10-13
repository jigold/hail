ALTER TABLE `billing_projects` ADD COLUMN billing_project_id INT AUTO_INCREMENT UNIQUE NOT NULL, ALGORITHM=INSTANT;
ALTER TABLE `billing_projects` ADD COLUMN name_cs VARCHAR(100) COLLATE utf8mb4_0900_as_cs, ALGORITHM=INSTANT;

# Add billing_project id to everywhere billing_project is used

ALTER TABLE `billing_project_users` ADD COLUMN billing_project_users INT DEFAULT NULL, ALGORITHM=INSTANT;
ALTER TABLE `batches` ADD COLUMN billing_project_id INT DEFAULT NULL, ALGORITHM=INSTANT;

# Fill in billing project id to billing project users table with select
# This table is small so okay to update in one transaction
UPDATE billing_projects
SET name_cs = `name`;

UPDATE billing_project_users
INNER JOIN billing_projects ON billing_project_users.billing_project = billing_projects.name
SET billing_project_id = billing_projects.billing_project_id;

# Create new aggregated_billing_tables

DROP TABLE IF EXISTS `aggregated_billing_project_id_user_resources_v3`;
CREATE TABLE IF NOT EXISTS `aggregated_billing_project_id_user_resources_v3` (
  `billing_project_id` INT NOT NULL,
  `user` VARCHAR(100) NOT NULL,
  `resource_id` INT NOT NULL,
  `token` INT NOT NULL,
  `usage` BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`billing_project_id`, `user`, `resource_id`, `token`),
  FOREIGN KEY (`billing_project_id`) REFERENCES billing_projects(billing_project_id) ON DELETE CASCADE,
  FOREIGN KEY (`resource_id`) REFERENCES resources(`resource_id`) ON DELETE CASCADE
) ENGINE = InnoDB;
CREATE INDEX aggregated_billing_project_id_user_resources_v3_user ON `aggregated_billing_project_id_user_resources_v3` (`user`);

DROP TABLE IF EXISTS `aggregated_billing_project_id_user_resources_by_date_v3`;
CREATE TABLE IF NOT EXISTS `aggregated_billing_project_id_user_resources_by_date_v3` (
  `billing_date` DATE NOT NULL,
  `billing_project_id` INT NOT NULL,
  `user` VARCHAR(100) NOT NULL,
  `resource_id` INT NOT NULL,
  `token` INT NOT NULL,
  `usage` BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`billing_date`, `billing_project_id`, `user`, `resource_id`, `token`),
  FOREIGN KEY (`billing_project_id`) REFERENCES billing_projects(billing_project_id) ON DELETE CASCADE,
  FOREIGN KEY (`resource_id`) REFERENCES resources(`resource_id`) ON DELETE CASCADE
) ENGINE = InnoDB;
CREATE INDEX aggregated_billing_project_id_user_resources_by_date_v3_billing_date_user ON `aggregated_billing_project_id_user_resources_by_date_v3` (`billing_date`, `user`);

# Set up updating the batches and aggregated billing tables in parallel
ALTER TABLE `batches` ADD COLUMN billing_project_id_updated BOOLEAN DEFAULT FALSE, ALGORITHM=INSTANT;
ALTER TABLE `attempts` ADD COLUMN aggregated_in_v3 BOOLEAN DEFAULT FALSE, ALGORITHM=INSTANT;

# Add triggers to do automatic updates

DELIMITER $$

DROP TRIGGER IF EXISTS batches_before_insert $$
CREATE TRIGGER batches_before_insert BEFORE INSERT ON batches
FOR EACH ROW
BEGIN
  DECLARE cur_billing_project_id INT;

  SELECT billing_project_id INTO cur_billing_project_id
  FROM billing_projects
  WHERE billing_project = NEW.billing_project;

  IF NEW.billing_project_id IS NULL THEN
    SET NEW.billing_project_id = cur_billing_project_id;
  END IF;
END $$

DROP TRIGGER IF EXISTS batches_before_update $$
CREATE TRIGGER batches_before_update BEFORE UPDATE ON batches
FOR EACH ROW
BEGIN
  DECLARE cur_billing_project_id INT;

  SELECT billing_project_id INTO cur_billing_project_id
  FROM billing_projects
  WHERE billing_project = NEW.billing_project;

  IF NEW.billing_project_id IS NULL THEN
    SET NEW.billing_project_id = cur_billing_project_id;
  END IF;

  SET NEW.billing_project_updated = TRUE;
END $$

DROP TRIGGER IF EXISTS attempts_before_update $$
CREATE TRIGGER attempts_before_update BEFORE UPDATE ON attempts
FOR EACH ROW
BEGIN
  IF OLD.start_time IS NOT NULL AND (NEW.start_time IS NULL OR OLD.start_time < NEW.start_time) THEN
    SET NEW.start_time = OLD.start_time;
  END IF;

  # for job private instances that do not finish creating
  IF NEW.reason = 'activation_timeout' THEN
    SET NEW.start_time = NULL;
  END IF;

  IF OLD.reason IS NOT NULL AND (OLD.end_time IS NULL OR NEW.end_time IS NULL OR NEW.end_time >= OLD.end_time) THEN
    SET NEW.end_time = OLD.end_time;
    SET NEW.reason = OLD.reason;
  END IF;

  # rollup_time should not go backward in time
  # this could happen if MJS happens after the billing update is received
  IF NEW.rollup_time IS NOT NULL AND OLD.rollup_time IS NOT NULL AND NEW.rollup_time < OLD.rollup_time THEN
    SET NEW.rollup_time = OLD.rollup_time;
  END IF;

  # rollup_time should never be less than the start time
  IF NEW.rollup_time IS NOT NULL AND NEW.start_time IS NOT NULL AND NEW.rollup_time < NEW.start_time THEN
    SET NEW.rollup_time = OLD.rollup_time;
  END IF;

  # rollup_time should never be greater than the end time
  IF NEW.rollup_time IS NOT NULL AND NEW.end_time IS NOT NULL AND NEW.rollup_time > NEW.end_time THEN
    SET NEW.rollup_time = NEW.end_time;
  END IF;

  SET NEW.aggregated_in_v3 = TRUE;
END $$

DROP TRIGGER IF EXISTS attempts_after_update $$
CREATE TRIGGER attempts_after_update AFTER UPDATE ON attempts
FOR EACH ROW
BEGIN
  DECLARE job_cores_mcpu INT;
  DECLARE cur_billing_project VARCHAR(100);
  DECLARE cur_billing_project_id INT;
  DECLARE msec_diff BIGINT;
  DECLARE msec_diff_rollup BIGINT;
  DECLARE msec_diff_migration BIGINT;
  DECLARE cur_n_tokens INT;
  DECLARE rand_token INT;
  DECLARE cur_billing_date DATE;

  SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
  SET rand_token = FLOOR(RAND() * cur_n_tokens);

  SELECT cores_mcpu INTO job_cores_mcpu FROM jobs
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id;

  SELECT billing_project INTO cur_billing_project FROM batches WHERE id = NEW.batch_id;

  SELECT billing_project_id INTO cur_billing_project_id
  FROM billing_projects
  WHERE name = cur_billing_project;

  SET msec_diff = (GREATEST(COALESCE(NEW.end_time - NEW.start_time, 0), 0) -
                   GREATEST(COALESCE(OLD.end_time - OLD.start_time, 0), 0));

  SET msec_diff_rollup = (GREATEST(COALESCE(NEW.rollup_time - NEW.start_time, 0), 0) -
                          GREATEST(COALESCE(OLD.rollup_time - OLD.start_time, 0), 0));

  IF NOT OLD.aggregated_in_v3 THEN
    SET msec_diff_migration = GREATEST(COALESCE(NEW.end_time - NEW.start_time, 0), 0);
    SET rand_token_migration = NEW.batch_id DIV 100000 + NEW.job_id DIV 100000;
  ELSE
    SET msec_diff_migration = msec_diff;
    SET rand_token_migration = rand_token;
  END IF;

  SET cur_billing_date = CAST(UTC_DATE() AS DATE);

  IF msec_diff != 0 THEN
    INSERT INTO aggregated_billing_project_resources (billing_project, resource, token, `usage`)
    SELECT billing_project, resources.resource, rand_token, msec_diff * quantity
    FROM attempt_resources
    JOIN batches ON batches.id = attempt_resources.batch_id
    LEFT JOIN resources ON attempt_resources.resource_id = resources.resource_id
    WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
    ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

    INSERT INTO aggregated_batch_resources (batch_id, resource, token, `usage`)
    SELECT batch_id, resources.resource, rand_token, msec_diff * quantity
    FROM attempt_resources
    LEFT JOIN resources ON attempt_resources.resource_id = resources.resource_id
    WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
    ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

    INSERT INTO aggregated_job_resources (batch_id, job_id, resource, `usage`)
    SELECT batch_id, job_id, resources.resource, msec_diff * quantity
    FROM attempt_resources
    LEFT JOIN resources ON attempt_resources.resource_id = resources.resource_id
    WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
    ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;
  END IF;

  IF msec_diff_rollup != 0 THEN
    INSERT INTO aggregated_billing_project_user_resources_v2 (billing_project, user, resource_id, token, `usage`)
    SELECT billing_project, `user`,
      resource_id,
      rand_token,
      msec_diff_rollup * quantity
    FROM attempt_resources
    JOIN batches ON batches.id = attempt_resources.batch_id
    WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
    ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff_rollup * quantity;

    INSERT INTO aggregated_batch_resources_v2 (batch_id, resource_id, token, `usage`)
    SELECT attempt_resources.batch_id,
      resource_id,
      rand_token,
      msec_diff_rollup * quantity
    FROM attempt_resources
    WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
    ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff_rollup * quantity;

    INSERT INTO aggregated_job_resources_v2 (batch_id, job_id, resource_id, `usage`)
    SELECT attempt_resources.batch_id, attempt_resources.job_id,
      resource_id,
      msec_diff_rollup * quantity
    FROM attempt_resources
    WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
    ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff_rollup * quantity;

    INSERT INTO aggregated_billing_project_user_resources_by_date_v2 (billing_date, billing_project, user, resource_id, token, `usage`)
    SELECT cur_billing_date,
      billing_project,
      `user`,
      resource_id,
      rand_token,
      msec_diff_rollup * quantity
    FROM attempt_resources
    JOIN batches ON batches.id = attempt_resources.batch_id
    WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
    ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff_rollup * quantity;
  END IF;

  IF msec_diff_migration != 0 THEN
    INSERT INTO aggregated_billing_project_id_user_resources_v3 (billing_project_id, user, resource_id, token, `usage`)
    SELECT cur_billing_project_id, `user`,
      resource_id,
      rand_token_migration,
      msec_diff_migration * quantity
    FROM attempt_resources
    JOIN batches ON batches.id = attempt_resources.batch_id
    WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
    ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff_migration * quantity;

    INSERT INTO aggregated_billing_project_user_resources_by_date_v2 (billing_date, billing_project_id, user, resource_id, token, `usage`)
    SELECT cur_billing_date,
      cur_billing_project_id,
      `user`,
      resource_id,
      rand_token_migration,
      msec_diff_migration * quantity
    FROM attempt_resources
    JOIN batches ON batches.id = attempt_resources.batch_id
    WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
    ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff_migration * quantity;
  END IF;
END $$

DROP TRIGGER IF EXISTS attempt_resources_after_insert $$
CREATE TRIGGER attempt_resources_after_insert AFTER INSERT ON attempt_resources
FOR EACH ROW
BEGIN
  DECLARE cur_start_time BIGINT;
  DECLARE cur_rollup_time BIGINT;
  DECLARE cur_end_time BIGINT;
  DECLARE cur_billing_project VARCHAR(100);
  DECLARE cur_user VARCHAR(100);
  DECLARE msec_diff BIGINT;
  DECLARE msec_diff_rollup BIGINT;
  DECLARE cur_n_tokens INT;
  DECLARE rand_token INT;
  DECLARE cur_resource VARCHAR(100);
  DECLARE cur_billing_date DATE;

  SELECT billing_project, user INTO cur_billing_project, cur_user
  FROM batches WHERE id = NEW.batch_id;

  SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
  SET rand_token = FLOOR(RAND() * cur_n_tokens);

  SELECT resource INTO cur_resource FROM resources WHERE resource_id = NEW.resource_id;

  SELECT start_time, end_time, rollup_time INTO cur_start_time, cur_end_time, cur_rollup_time
  FROM attempts
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  LOCK IN SHARE MODE;

  SET msec_diff = GREATEST(COALESCE(cur_end_time - cur_start_time, 0), 0);
  SET msec_diff_rollup = GREATEST(COALESCE(cur_rollup_time - cur_start_time, 0), 0);

  SET cur_billing_date = CAST(UTC_DATE() AS DATE);

  IF msec_diff != 0 THEN
    INSERT INTO aggregated_billing_project_resources (billing_project, resource, token, `usage`)
    VALUES (cur_billing_project, cur_resource, rand_token, NEW.quantity * msec_diff)
    ON DUPLICATE KEY UPDATE
      `usage` = `usage` + NEW.quantity * msec_diff;

    INSERT INTO aggregated_batch_resources (batch_id, resource, token, `usage`)
    VALUES (NEW.batch_id, cur_resource, rand_token, NEW.quantity * msec_diff)
    ON DUPLICATE KEY UPDATE
      `usage` = `usage` + NEW.quantity * msec_diff;

    INSERT INTO aggregated_job_resources (batch_id, job_id, resource, `usage`)
    VALUES (NEW.batch_id, NEW.job_id, cur_resource, NEW.quantity * msec_diff)
    ON DUPLICATE KEY UPDATE
      `usage` = `usage` + NEW.quantity * msec_diff;
  END IF;

  IF msec_diff_rollup != 0 THEN
    INSERT INTO aggregated_billing_project_user_resources_v2 (billing_project, user, resource_id, token, `usage`)
    VALUES (cur_billing_project, cur_user, NEW.resource_id, rand_token, NEW.quantity * msec_diff_rollup)
    ON DUPLICATE KEY UPDATE
      `usage` = `usage` + NEW.quantity * msec_diff_rollup;

    INSERT INTO aggregated_batch_resources_v2 (batch_id, resource_id, token, `usage`)
    VALUES (NEW.batch_id, NEW.resource_id, rand_token, NEW.quantity * msec_diff_rollup)
    ON DUPLICATE KEY UPDATE
      `usage` = `usage` + NEW.quantity * msec_diff_rollup;

    INSERT INTO aggregated_job_resources_v2 (batch_id, job_id, resource_id, `usage`)
    VALUES (NEW.batch_id, NEW.job_id, NEW.resource_id, NEW.quantity * msec_diff_rollup)
    ON DUPLICATE KEY UPDATE
      `usage` = `usage` + NEW.quantity * msec_diff_rollup;

    IF cur_billing_date IS NOT NULL THEN
      INSERT INTO aggregated_billing_project_user_resources_by_date_v2 (billing_date, billing_project, user, resource_id, token, `usage`)
      VALUES (cur_billing_date, cur_billing_project, cur_user, NEW.resource_id, rand_token, NEW.quantity * msec_diff_rollup)
      ON DUPLICATE KEY UPDATE
        `usage` = `usage` + NEW.quantity * msec_diff_rollup;
    END IF;

    INSERT INTO aggregated_billing_project_id_user_resources_v3 (billing_project_id, user, resource_id, token, `usage`)
    VALUES (cur_billing_project_id, cur_user, NEW.resource_id, rand_token, NEW.quantity * msec_diff_rollup)
    ON DUPLICATE KEY UPDATE
      `usage` = `usage` + NEW.quantity * msec_diff_rollup;

    IF cur_billing_date IS NOT NULL THEN
      INSERT INTO aggregated_billing_project_id_user_resources_by_date_v3 (billing_date, billing_project_id, user, resource_id, token, `usage`)
      VALUES (cur_billing_date, cur_billing_project_id, cur_user, NEW.resource_id, rand_token, NEW.quantity * msec_diff_rollup)
      ON DUPLICATE KEY UPDATE
        `usage` = `usage` + NEW.quantity * msec_diff_rollup;
    END IF;
  END IF;
END $$

DELIMITER ;
