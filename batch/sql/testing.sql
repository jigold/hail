
INSERT INTO billing_projects (name) VALUES ("hail");
INSERT INTO billing_project_users (billing_project, user) VALUES ("hail", "jigold");
UPDATE globals SET worker_cores = 16;

ALTER TABLE batches ADD COLUMN time_closed BIGINT;

