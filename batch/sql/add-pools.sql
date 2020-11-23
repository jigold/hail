ALTER TABLE globals DROP COLUMN `worker_cores`;
ALTER TABLE globals DROP COLUMN `worker_type`;
ALTER TABLE globals DROP COLUMN `worker_disk_size_gb`;
ALTER TABLE globals DROP COLUMN `worker_local_ssd_data_disk`;
ALTER TABLE globals DROP COLUMN `worker_pd_ssd_data_disk_size_gb`;
ALTER TABLE globals DROP COLUMN `standing_worker_cores`;
ALTER TABLE globals DROP COLUMN `max_instances`;
ALTER TABLE globals DROP COLUMN `pool_size`;

CREATE TABLE IF NOT EXISTS `pools` (
  `type` VARCHAR(255) NOT NULL,
  `cores` INT NOT NULL,
  `standing_worker_cores` BIGINT NOT NULL,
  `disk_size_gb` BIGINT NOT NULL,
  `local_ssd_data_disk` BOOLEAN NOT NULL DEFAULT 1,
  `pd_ssd_data_disk_size_gb` BIGINT NOT NULL DEFAULT 0,
  `max_instances` BIGINT NOT NULL,
  `pool_size` BIGINT NOT NULL,
  PRIMARY KEY (`type`)
) ENGINE = InnoDB;

INSERT INTO pools (`type`, `cores`, `standing_worker_cores`, `disk_size_gb`)

ALTER TABLE instances ADD COLUMN `pool` VARCHAR(255) NOT NULL DEFAULT 'standard';
ALTER TABLE jobs ADD COLUMN `pool` VARCHAR(255) NOT NULL DEFAULT 'standard';

# set jobs table

# delete ready_cores

# migrate user_resources

# migrate batch_staging_user_resources

# migrate batch_cancellable_resources

# add new trigger

# add close_batch

# add cancel_batch

