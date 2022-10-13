SET foreign_key_checks = 0;
ALTER TABLE `billing_project_users` ADD FOREIGN KEY (billing_project_id) REFERENCES `billing_projects` (billing_project_id) ON DELETE CASCADE, ALGORITHM=INPLACE;
ALTER TABLE `batches` ADD FOREIGN KEY (billing_project_id) REFERENCES `billing_projects` (billing_project_id) ON DELETE CASCADE, ALGORITHM=INPLACE;
SET foreign_key_checks = 1;

ALTER TABLE billing_projects MODIFY COLUMN `name_cs` VARCHAR(100) COLLATE utf8mb4_0900_as_cs NOT NULL, ALGORITHM=INPLACE, LOCK=NONE;
