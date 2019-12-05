DROP PROCEDURE IF EXISTS activate_instance;
DROP PROCEDURE IF EXISTS deactivate_instance;
DROP PROCEDURE IF EXISTS mark_instance_deleted;
DROP PROCEDURE IF EXISTS close_batch;
DROP PROCEDURE IF EXISTS schedule_job;
DROP PROCEDURE IF EXISTS unschedule_job;
DROP PROCEDURE IF EXISTS mark_job_complete;

DROP FUNCTION IF EXISTS update_resource_usage;

DROP TABLE IF EXISTS `attempts`;
DROP TABLE IF EXISTS `batch_attributes`;
DROP TABLE IF EXISTS `job_attributes`;
DROP TABLE IF EXISTS `job_parents`;
DROP TABLE IF EXISTS `ready_cores`;
DROP TABLE IF EXISTS `gevents_mark`;
DROP TABLE IF EXISTS `jobs`;
DROP TABLE IF EXISTS `batches`;
DROP TABLE IF EXISTS `user_resources`;
DROP TABLE IF EXISTS `instances`;
DROP TABLE IF EXISTS `tokens`;
DROP TABLE IF EXISTS `billing_project_users`;
DROP TABLE IF EXISTS `billing_projects`;
