DELIMITER $$

DROP TRIGGER IF EXISTS jobs_after_update $$
CREATE TRIGGER jobs_after_update AFTER UPDATE ON jobs
FOR EACH ROW
BEGIN
  DECLARE cur_user VARCHAR(100);
  DECLARE cur_batch_cancelled BOOLEAN;
  DECLARE cur_n_tokens INT;
  DECLARE rand_token INT;

  SELECT user INTO cur_user FROM batches WHERE id = NEW.batch_id;

  SET cur_batch_cancelled = EXISTS (SELECT TRUE
                                    FROM batches_cancelled
                                    WHERE id = NEW.batch_id
                                    LOCK IN SHARE MODE);

  SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
  SET rand_token = FLOOR(RAND() * cur_n_tokens);

  IF OLD.state = 'Ready' THEN
    IF NOT (OLD.always_run OR OLD.cancelled OR cur_batch_cancelled) THEN
      # cancellable
      INSERT INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_ready_cancellable_jobs, ready_cancellable_cores_mcpu)
      VALUES (OLD.batch_id, OLD.inst_coll, rand_token, -1, -OLD.cores_mcpu)
      ON DUPLICATE KEY UPDATE
        n_ready_cancellable_jobs = n_ready_cancellable_jobs - 1,
        ready_cancellable_cores_mcpu = ready_cancellable_cores_mcpu - OLD.cores_mcpu;
    END IF;

    IF NOT OLD.always_run AND (OLD.cancelled OR cur_batch_cancelled) THEN
      # cancelled
      INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_ready_jobs)
      VALUES (cur_user, OLD.inst_coll, rand_token, -1)
      ON DUPLICATE KEY UPDATE
        n_cancelled_ready_jobs = n_cancelled_ready_jobs - 1;
    ELSE
      # runnable
      INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_ready_jobs, ready_cores_mcpu)
      VALUES (cur_user, OLD.inst_coll, rand_token, -1, -OLD.cores_mcpu)
      ON DUPLICATE KEY UPDATE
        n_ready_jobs = n_ready_jobs - 1,
        ready_cores_mcpu = ready_cores_mcpu - OLD.cores_mcpu;
    END IF;
  ELSEIF OLD.state = 'Running' THEN
    IF NOT (OLD.always_run OR cur_batch_cancelled) THEN
      # cancellable
      INSERT INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_running_cancellable_jobs, running_cancellable_cores_mcpu)
      VALUES (OLD.batch_id, OLD.inst_coll, rand_token, -1, -OLD.cores_mcpu)
      ON DUPLICATE KEY UPDATE
        n_running_cancellable_jobs = n_running_cancellable_jobs - 1,
        running_cancellable_cores_mcpu = running_cancellable_cores_mcpu - OLD.cores_mcpu;
    END IF;

    # state = 'Running' jobs cannot be cancelled at the job level
    IF NOT OLD.always_run AND cur_batch_cancelled THEN
      # cancelled
      INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_running_jobs)
      VALUES (cur_user, OLD.inst_coll, rand_token, -1)
      ON DUPLICATE KEY UPDATE
        n_cancelled_running_jobs = n_cancelled_running_jobs - 1;
    ELSE
      # running
      INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_running_jobs, running_cores_mcpu)
      VALUES (cur_user, OLD.inst_coll, rand_token, -1, -OLD.cores_mcpu)
      ON DUPLICATE KEY UPDATE
        n_running_jobs = n_running_jobs - 1,
        running_cores_mcpu = running_cores_mcpu - OLD.cores_mcpu;
    END IF;
  ELSEIF OLD.state = 'Creating' THEN
    IF NOT (OLD.always_run OR cur_batch_cancelled) THEN
      # cancellable
      INSERT INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_creating_cancellable_jobs)
      VALUES (OLD.batch_id, OLD.inst_coll, rand_token, -1)
      ON DUPLICATE KEY UPDATE
        n_creating_cancellable_jobs = n_creating_cancellable_jobs - 1;
    END IF;

    # state = 'Creating' jobs cannot be cancelled at the job level
    IF NOT OLD.always_run AND cur_batch_cancelled THEN
      # cancelled
      INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_creating_jobs)
      VALUES (cur_user, OLD.inst_coll, rand_token, -1)
      ON DUPLICATE KEY UPDATE
        n_cancelled_creating_jobs = n_cancelled_creating_jobs - 1;
    ELSE
      # creating
      INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_creating_jobs)
      VALUES (cur_user, OLD.inst_coll, rand_token, -1)
      ON DUPLICATE KEY UPDATE
        n_creating_jobs = n_creating_jobs - 1;
    END IF;

  END IF;

  IF NEW.state = 'Ready' THEN
    IF NOT (NEW.always_run OR NEW.cancelled OR cur_batch_cancelled) THEN
      # cancellable
      INSERT INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_ready_cancellable_jobs, ready_cancellable_cores_mcpu)
      VALUES (NEW.batch_id, NEW.inst_coll, rand_token, 1, NEW.cores_mcpu)
      ON DUPLICATE KEY UPDATE
        n_ready_cancellable_jobs = n_ready_cancellable_jobs + 1,
        ready_cancellable_cores_mcpu = ready_cancellable_cores_mcpu + NEW.cores_mcpu;
    END IF;

    IF NOT NEW.always_run AND (NEW.cancelled OR cur_batch_cancelled) THEN
      # cancelled
      INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_ready_jobs)
      VALUES (cur_user, NEW.inst_coll, rand_token, 1)
      ON DUPLICATE KEY UPDATE
        n_cancelled_ready_jobs = n_cancelled_ready_jobs + 1;
    ELSE
      # runnable
      INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_ready_jobs, ready_cores_mcpu)
      VALUES (cur_user, NEW.inst_coll, rand_token, 1, NEW.cores_mcpu)
      ON DUPLICATE KEY UPDATE
        n_ready_jobs = n_ready_jobs + 1,
        ready_cores_mcpu = ready_cores_mcpu + NEW.cores_mcpu;
    END IF;
  ELSEIF NEW.state = 'Running' THEN
    IF NOT (NEW.always_run OR cur_batch_cancelled) THEN
      # cancellable
      INSERT INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_running_cancellable_jobs, running_cancellable_cores_mcpu)
      VALUES (NEW.batch_id, NEW.inst_coll, rand_token, 1, NEW.cores_mcpu)
      ON DUPLICATE KEY UPDATE
        n_running_cancellable_jobs = n_running_cancellable_jobs + 1,
        running_cancellable_cores_mcpu = running_cancellable_cores_mcpu + NEW.cores_mcpu;
    END IF;

    # state = 'Running' jobs cannot be cancelled at the job level
    IF NOT NEW.always_run AND cur_batch_cancelled THEN
      # cancelled
      INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_running_jobs)
      VALUES (cur_user, NEW.inst_coll, rand_token, 1)
      ON DUPLICATE KEY UPDATE
        n_cancelled_running_jobs = n_cancelled_running_jobs + 1;
    ELSE
      # running
      INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_running_jobs, running_cores_mcpu)
      VALUES (cur_user, NEW.inst_coll, rand_token, 1, NEW.cores_mcpu)
      ON DUPLICATE KEY UPDATE
        n_running_jobs = n_running_jobs + 1,
        running_cores_mcpu = running_cores_mcpu + NEW.cores_mcpu;
    END IF;
  ELSEIF NEW.state = 'Creating' THEN
    IF NOT (NEW.always_run OR cur_batch_cancelled) THEN
      # cancellable
      INSERT INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_creating_cancellable_jobs)
      VALUES (NEW.batch_id, NEW.inst_coll, rand_token, 1)
      ON DUPLICATE KEY UPDATE
        n_creating_cancellable_jobs = n_creating_cancellable_jobs + 1;
    END IF;

    # state = 'Creating' jobs cannot be cancelled at the job level
    IF NOT NEW.always_run AND cur_batch_cancelled THEN
      # cancelled
      INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_creating_jobs)
      VALUES (cur_user, NEW.inst_coll, rand_token, 1)
      ON DUPLICATE KEY UPDATE
        n_cancelled_creating_jobs = n_cancelled_creating_jobs + 1;
    ELSE
      # creating
      INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_creating_jobs)
      VALUES (cur_user, NEW.inst_coll, rand_token, 1)
      ON DUPLICATE KEY UPDATE
        n_creating_jobs = n_creating_jobs + 1;
    END IF;
  END IF;
END $$

-- DROP TRIGGER IF EXISTS jobs_after_update $$
-- CREATE TRIGGER jobs_after_update AFTER UPDATE ON jobs
-- FOR EACH ROW
-- BEGIN
--   DECLARE cur_user VARCHAR(100);
--   DECLARE cur_batch_cancelled BOOLEAN;
--   DECLARE cur_n_tokens INT;
--   DECLARE rand_token INT;
--
--   SELECT user INTO cur_user FROM batches WHERE id = NEW.batch_id;
--
--   SET cur_batch_cancelled = EXISTS (SELECT TRUE
--                                     FROM batches_cancelled
--                                     WHERE id = NEW.batch_id
--                                     LOCK IN SHARE MODE);
--
--   SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
--   SET rand_token = FLOOR(RAND() * cur_n_tokens);
--
--   IF OLD.state = 'Ready' THEN
--     IF NOT (OLD.always_run OR OLD.cancelled OR cur_batch_cancelled) THEN
--       # cancellable
--       INSERT IGNORE INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_ready_cancellable_jobs, ready_cancellable_cores_mcpu)
--       VALUES (OLD.batch_id, OLD.inst_coll, rand_token, -1, -OLD.cores_mcpu);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE batch_inst_coll_cancellable_resources
--         SET n_ready_cancellable_jobs = n_ready_cancellable_jobs - 1,
--           ready_cancellable_cores_mcpu = ready_cancellable_cores_mcpu - OLD.cores_mcpu
--         WHERE batch_id = OLD.batch_id AND inst_coll = OLD.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_ready_cancellable_jobs, ready_cancellable_cores_mcpu)
--           VALUES (OLD.batch_id, OLD.inst_coll, rand_token, -1, -OLD.cores_mcpu)
--           ON DUPLICATE KEY UPDATE
--             n_ready_cancellable_jobs = n_ready_cancellable_jobs - 1,
--             ready_cancellable_cores_mcpu = ready_cancellable_cores_mcpu - OLD.cores_mcpu;
--         END IF;
--       END IF;
--     END IF;
--
--     IF NOT OLD.always_run AND (OLD.cancelled OR cur_batch_cancelled) THEN
--       # cancelled
--       INSERT IGNORE INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_ready_jobs)
--       VALUES (cur_user, OLD.inst_coll, rand_token, -1);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE user_inst_coll_resources
--         SET n_cancelled_ready_jobs = n_cancelled_ready_jobs - 1
--         WHERE `user` = cur_user AND inst_coll = OLD.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_ready_jobs)
--           VALUES (cur_user, OLD.inst_coll, rand_token, -1)
--           ON DUPLICATE KEY UPDATE
--             n_cancelled_ready_jobs = n_cancelled_ready_jobs - 1;
--         END IF;
--       END IF;
--     ELSE
--       # runnable
--       INSERT IGNORE INTO user_inst_coll_resources (user, inst_coll, token, n_ready_jobs, ready_cores_mcpu)
--       VALUES (cur_user, OLD.inst_coll, rand_token, -1, -OLD.cores_mcpu);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE user_inst_coll_resources
--         SET n_ready_jobs = n_ready_jobs - 1,
--           ready_cores_mcpu = ready_cores_mcpu - OLD.cores_mcpu
--         WHERE user = cur_user AND inst_coll = OLD.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_ready_jobs, ready_cores_mcpu)
--           VALUES (cur_user, OLD.inst_coll, rand_token, -1, -OLD.cores_mcpu)
--           ON DUPLICATE KEY UPDATE
--             n_ready_jobs = n_ready_jobs - 1,
--             ready_cores_mcpu = ready_cores_mcpu - OLD.cores_mcpu;
--         END IF;
--       END IF;
--     END IF;
--   ELSEIF OLD.state = 'Running' THEN
--     IF NOT (OLD.always_run OR cur_batch_cancelled) THEN
--       # cancellable
--       INSERT IGNORE INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_running_cancellable_jobs, running_cancellable_cores_mcpu)
--       VALUES (OLD.batch_id, OLD.inst_coll, rand_token, -1, -OLD.cores_mcpu);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE batch_inst_coll_cancellable_resources
--         SET n_running_cancellable_jobs = n_running_cancellable_jobs - 1,
--           running_cancellable_cores_mcpu = running_cancellable_cores_mcpu - OLD.cores_mcpu
--         WHERE batch_id = OLD.batch_id AND inst_coll = OLD.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_running_cancellable_jobs, running_cancellable_cores_mcpu)
--           VALUES (OLD.batch_id, OLD.inst_coll, rand_token, -1, -OLD.cores_mcpu)
--           ON DUPLICATE KEY UPDATE
--             n_running_cancellable_jobs = n_running_cancellable_jobs - 1,
--             running_cancellable_cores_mcpu = running_cancellable_cores_mcpu - OLD.cores_mcpu;
--         END IF;
--       END IF;
--     END IF;
--
--     # state = 'Running' jobs cannot be cancelled at the job level
--     IF NOT OLD.always_run AND cur_batch_cancelled THEN
--       # cancelled
--       INSERT IGNORE INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_running_jobs)
--       VALUES (cur_user, OLD.inst_coll, rand_token, -1);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE user_inst_coll_resources
--         SET n_cancelled_running_jobs = n_cancelled_running_jobs - 1
--         WHERE `user` = cur_user AND inst_coll = OLD.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_running_jobs)
--           VALUES (cur_user, OLD.inst_coll, rand_token, -1)
--           ON DUPLICATE KEY UPDATE
--             n_cancelled_running_jobs = n_cancelled_running_jobs - 1;
--         END IF;
--       END IF;
--     ELSE
--       # running
--       INSERT IGNORE INTO user_inst_coll_resources (user, inst_coll, token, n_running_jobs, running_cores_mcpu)
--       VALUES (cur_user, OLD.inst_coll, rand_token, -1, -OLD.cores_mcpu);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE user_inst_coll_resources
--         SET n_running_jobs = n_running_jobs - 1,
--           running_cores_mcpu = running_cores_mcpu - OLD.cores_mcpu
--         WHERE `user` = cur_user AND inst_coll = OLD.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_running_jobs, running_cores_mcpu)
--           VALUES (cur_user, OLD.inst_coll, rand_token, -1, -OLD.cores_mcpu)
--           ON DUPLICATE KEY UPDATE
--             n_running_jobs = n_running_jobs - 1,
--             running_cores_mcpu = running_cores_mcpu - OLD.cores_mcpu;
--         END IF;
--       END IF;
--     END IF;
--   ELSEIF OLD.state = 'Creating' THEN
--     IF NOT (OLD.always_run OR cur_batch_cancelled) THEN
--       # cancellable
--       INSERT IGNORE INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_creating_cancellable_jobs)
--       VALUES (OLD.batch_id, OLD.inst_coll, rand_token, -1);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE batch_inst_coll_cancellable_resources
--         SET n_creating_cancellable_jobs = n_creating_cancellable_jobs - 1
--         WHERE batch_id = OLD.batch_id AND inst_coll = OLD.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_creating_cancellable_jobs)
--           VALUES (OLD.batch_id, OLD.inst_coll, rand_token, -1)
--           ON DUPLICATE KEY UPDATE
--             n_creating_cancellable_jobs = n_creating_cancellable_jobs - 1;
--         END IF;
--       END IF;
--     END IF;
--
--     # state = 'Creating' jobs cannot be cancelled at the job level
--     IF NOT OLD.always_run AND cur_batch_cancelled THEN
--       # cancelled
--       INSERT IGNORE INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_creating_jobs)
--       VALUES (cur_user, OLD.inst_coll, rand_token, -1);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE user_inst_coll_resources
--         SET n_cancelled_creating_jobs = n_cancelled_creating_jobs - 1
--         WHERE `user` = cur_user AND inst_coll = OLD.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_creating_jobs)
--           VALUES (cur_user, OLD.inst_coll, rand_token, -1)
--           ON DUPLICATE KEY UPDATE
--             n_cancelled_creating_jobs = n_cancelled_creating_jobs - 1;
--         END IF;
--       END IF;
--     ELSE
--       # creating
--       INSERT IGNORE INTO user_inst_coll_resources (user, inst_coll, token, n_creating_jobs)
--       VALUES (cur_user, OLD.inst_coll, rand_token, -1);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE user_inst_coll_resources
--         SET n_creating_jobs = n_creating_jobs - 1
--         WHERE `user` = cur_user AND inst_coll = OLD.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_creating_jobs)
--           VALUES (cur_user, OLD.inst_coll, rand_token, -1)
--           ON DUPLICATE KEY UPDATE
--             n_creating_jobs = n_creating_jobs - 1;
--         END IF;
--       END IF;
--     END IF;
--   END IF;
--
--   IF NEW.state = 'Ready' THEN
--     IF NOT (NEW.always_run OR NEW.cancelled OR cur_batch_cancelled) THEN
--       # cancellable
--       INSERT IGNORE INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_ready_cancellable_jobs, ready_cancellable_cores_mcpu)
--       VALUES (NEW.batch_id, NEW.inst_coll, rand_token, 1, NEW.cores_mcpu);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE batch_inst_coll_cancellable_resources
--         SET n_ready_cancellable_jobs = n_ready_cancellable_jobs + 1,
--           ready_cancellable_cores_mcpu = ready_cancellable_cores_mcpu + NEW.cores_mcpu
--         WHERE batch_id = NEW.batch_id AND inst_coll = NEW.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_ready_cancellable_jobs, ready_cancellable_cores_mcpu)
--           VALUES (NEW.batch_id, NEW.inst_coll, rand_token, 1, NEW.cores_mcpu)
--           ON DUPLICATE KEY UPDATE
--             n_ready_cancellable_jobs = n_ready_cancellable_jobs + 1,
--             ready_cancellable_cores_mcpu = ready_cancellable_cores_mcpu + NEW.cores_mcpu;
--         END IF;
--       END IF;
--     END IF;
--
--     IF NOT NEW.always_run AND (NEW.cancelled OR cur_batch_cancelled) THEN
--       # cancelled
--       INSERT IGNORE INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_ready_jobs)
--       VALUES (cur_user, NEW.inst_coll, rand_token, 1);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE user_inst_coll_resources
--         SET n_cancelled_ready_jobs = n_cancelled_ready_jobs + 1
--         WHERE `user` = cur_user AND inst_coll = NEW.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_ready_jobs)
--           VALUES (cur_user, NEW.inst_coll, rand_token, 1)
--           ON DUPLICATE KEY UPDATE
--             n_cancelled_ready_jobs = n_cancelled_ready_jobs + 1;
--         END IF;
--       END IF;
--     ELSE
--       # runnable
--       INSERT IGNORE INTO user_inst_coll_resources (user, inst_coll, token, n_ready_jobs, ready_cores_mcpu)
--       VALUES (cur_user, NEW.inst_coll, rand_token, 1, NEW.cores_mcpu);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE user_inst_coll_resources
--         SET n_ready_jobs = n_ready_jobs + 1,
--           ready_cores_mcpu = ready_cores_mcpu + NEW.cores_mcpu
--         WHERE `user` = cur_user AND inst_coll = NEW.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_ready_jobs, ready_cores_mcpu)
--           VALUES (cur_user, NEW.inst_coll, rand_token, 1, NEW.cores_mcpu)
--           ON DUPLICATE KEY UPDATE
--             n_ready_jobs = n_ready_jobs + 1,
--             ready_cores_mcpu = ready_cores_mcpu + NEW.cores_mcpu;
--         END IF;
--       END IF;
--     END IF;
--   ELSEIF NEW.state = 'Running' THEN
--     IF NOT (NEW.always_run OR cur_batch_cancelled) THEN
--       # cancellable
--       INSERT IGNORE INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_running_cancellable_jobs, running_cancellable_cores_mcpu)
--       VALUES (NEW.batch_id, NEW.inst_coll, rand_token, 1, NEW.cores_mcpu);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE batch_inst_coll_cancellable_resources
--         SET n_running_cancellable_jobs = n_running_cancellable_jobs + 1,
--           running_cancellable_cores_mcpu = running_cancellable_cores_mcpu + NEW.cores_mcpu
--         WHERE batch_id = NEW.batch_id AND inst_coll = NEW.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_running_cancellable_jobs, running_cancellable_cores_mcpu)
--           VALUES (NEW.batch_id, NEW.inst_coll, rand_token, 1, NEW.cores_mcpu)
--           ON DUPLICATE KEY UPDATE
--             n_running_cancellable_jobs = n_running_cancellable_jobs + 1,
--             running_cancellable_cores_mcpu = running_cancellable_cores_mcpu + NEW.cores_mcpu;
--         END IF;
--       END IF;
--     END IF;
--
--     # state = 'Running' jobs cannot be cancelled at the job level
--     IF NOT NEW.always_run AND cur_batch_cancelled THEN
--       # cancelled
--       INSERT IGNORE INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_running_jobs)
--       VALUES (cur_user, NEW.inst_coll, rand_token, 1);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE user_inst_coll_resources
--         SET n_cancelled_running_jobs = n_cancelled_running_jobs + 1
--         WHERE `user` = cur_user AND inst_coll = NEW.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_running_jobs)
--           VALUES (cur_user, NEW.inst_coll, rand_token, 1)
--           ON DUPLICATE KEY UPDATE
--             n_cancelled_running_jobs = n_cancelled_running_jobs + 1;
--         END IF;
--       END IF;
--     ELSE
--       # running
--       INSERT IGNORE INTO user_inst_coll_resources (user, inst_coll, token, n_running_jobs, running_cores_mcpu)
--       VALUES (cur_user, NEW.inst_coll, rand_token, 1, NEW.cores_mcpu);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE user_inst_coll_resources
--         SET n_running_jobs = n_running_jobs + 1,
--           running_cores_mcpu = running_cores_mcpu + NEW.cores_mcpu
--         WHERE `user` = cur_user AND inst_coll = NEW.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_running_jobs, running_cores_mcpu)
--           VALUES (cur_user, NEW.inst_coll, rand_token, 1, NEW.cores_mcpu)
--           ON DUPLICATE KEY UPDATE
--             n_running_jobs = n_running_jobs + 1,
--             running_cores_mcpu = running_cores_mcpu + NEW.cores_mcpu;
--         END IF;
--       END IF;
--     END IF;
--   ELSEIF NEW.state = 'Creating' THEN
--     IF NOT (NEW.always_run OR cur_batch_cancelled) THEN
--       # cancellable
--       INSERT IGNORE INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_creating_cancellable_jobs)
--       VALUES (NEW.batch_id, NEW.inst_coll, rand_token, 1);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE batch_inst_coll_cancellable_resources
--         SET n_creating_cancellable_jobs = n_creating_cancellable_jobs + 1
--         WHERE batch_id = NEW.batch_id AND inst_coll = NEW.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO batch_inst_coll_cancellable_resources (batch_id, inst_coll, token, n_creating_cancellable_jobs)
--           VALUES (NEW.batch_id, NEW.inst_coll, rand_token, 1)
--           ON DUPLICATE KEY UPDATE
--             n_creating_cancellable_jobs = n_creating_cancellable_jobs + 1;
--         END IF;
--       END IF;
--     END IF;
--
--     # state = 'Creating' jobs cannot be cancelled at the job level
--     IF NOT NEW.always_run AND cur_batch_cancelled THEN
--       # cancelled
--       INSERT IGNORE INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_creating_jobs)
--       VALUES (cur_user, NEW.inst_coll, rand_token, 1);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE user_inst_coll_resources
--         SET n_cancelled_creating_jobs = n_cancelled_creating_jobs + 1
--         WHERE `user` = cur_user AND inst_coll = NEW.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_cancelled_creating_jobs)
--           VALUES (cur_user, NEW.inst_coll, rand_token, 1)
--           ON DUPLICATE KEY UPDATE
--             n_cancelled_creating_jobs = n_cancelled_creating_jobs + 1;
--         END IF;
--       END IF;
--     ELSE
--       # creating
--       INSERT IGNORE INTO user_inst_coll_resources (user, inst_coll, token, n_creating_jobs)
--       VALUES (cur_user, NEW.inst_coll, rand_token, 1);
--
--       IF ROW_COUNT() != 1 THEN
--         UPDATE user_inst_coll_resources
--         SET n_creating_jobs = n_creating_jobs + 1
--         WHERE `user` = cur_user AND inst_coll = NEW.inst_coll AND token = rand_token;
--
--         IF ROW_COUNT() != 1 THEN
--           INSERT INTO user_inst_coll_resources (user, inst_coll, token, n_creating_jobs)
--           VALUES (cur_user, NEW.inst_coll, rand_token, 1)
--           ON DUPLICATE KEY UPDATE
--             n_creating_jobs = n_creating_jobs + 1;
--         END IF;
--       END IF;
--     END IF;
--   END IF;
-- END $$

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

  INSERT IGNORE INTO aggregated_billing_project_resources (billing_project, resource, token, `usage`)
  VALUES (cur_billing_project, NEW.resource, rand_token, NEW.quantity * msec_diff);

  IF ROW_COUNT() != 1 THEN
    UPDATE aggregated_billing_project_resources
    SET `usage` = `usage` + NEW.quantity * msec_diff
    WHERE billing_project = cur_billing_project AND resource = NEW.resource AND token = rand_token;

    IF ROW_COUNT() != 1 THEN
      INSERT INTO aggregated_billing_project_resources (billing_project, resource, token, `usage`)
      VALUES (cur_billing_project, NEW.resource, rand_token, NEW.quantity * msec_diff)
      ON DUPLICATE KEY UPDATE `usage` = `usage` + NEW.quantity * msec_diff;
    END IF;
  END IF;

  INSERT IGNORE INTO aggregated_batch_resources (batch_id, resource, token, `usage`)
  VALUES (NEW.batch_id, NEW.resource, rand_token, NEW.quantity * msec_diff);

  IF ROW_COUNT() != 1 THEN
    UPDATE aggregated_batch_resources
    SET `usage` = `usage` + NEW.quantity * msec_diff
    WHERE batch_id = NEW.batch_id AND resource = NEW.resource AND token = rand_token;

    IF ROW_COUNT() != 1 THEN
      INSERT INTO aggregated_batch_resources (batch_id, resource, token, `usage`)
      VALUES (NEW.batch_id, NEW.resource, rand_token, NEW.quantity * msec_diff)
      ON DUPLICATE KEY UPDATE `usage` = `usage` + NEW.quantity * msec_diff;
    END IF;
  END IF;

  INSERT IGNORE INTO aggregated_job_resources (batch_id, job_id, resource, `usage`)
  VALUES (NEW.batch_id, NEW.job_id, NEW.resource, NEW.quantity * msec_diff);

  IF ROW_COUNT() != 1 THEN
    UPDATE aggregated_job_resources
    SET `usage` = `usage` + NEW.quantity * msec_diff
    WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND resource = NEW.resource;

    IF ROW_COUNT() != 1 THEN
      INSERT INTO aggregated_job_resources (batch_id, job_id, resource, `usage`)
      VALUES (NEW.batch_id, NEW.job_id, NEW.resource, NEW.quantity * msec_diff)
      ON DUPLICATE KEY UPDATE `usage` = `usage` + NEW.quantity * msec_diff;
    END IF;
  END IF;
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

  SET delta_cores_mcpu = IFNULL(delta_cores_mcpu, 0);

  IF in_attempt_id IS NOT NULL THEN
    SELECT 1 FROM instances_free_cores_mcpu
    WHERE instances_free_cores_mcpu.name = in_instance_name
    FOR UPDATE;

    INSERT IGNORE INTO attempts (batch_id, job_id, attempt_id, instance_name)
    VALUES (in_batch_id, in_job_id, in_attempt_id, in_instance_name);

    IF ROW_COUNT() = 1 THEN
      SELECT state INTO cur_instance_state
      FROM instances
      WHERE name = in_instance_name
      LOCK IN SHARE MODE;

      UPDATE instances_free_cores_mcpu
      SET free_cores_mcpu = free_cores_mcpu - in_cores_mcpu
      WHERE instances_free_cores_mcpu.name = in_instance_name
        AND (cur_instance_state = 'pending' OR cur_instance_state = 'active');

--       UPDATE instances, instances_free_cores_mcpu
--       SET free_cores_mcpu = free_cores_mcpu - in_cores_mcpu
--       WHERE instances.name = in_instance_name
--         AND instances.name = instances_free_cores_mcpu.name
--         AND (instances.state = 'pending' OR instances.state = 'active');

      SET delta_cores_mcpu = -1 * in_cores_mcpu;
    END IF;
  END IF;
END $$

DELIMITER ;
