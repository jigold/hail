INSERT INTO `billing_projects` (`name`, `limit`)
VALUES ('test-zero-limit', 0);

INSERT INTO `billing_projects` (`name`, `limit`)
VALUES ('test-tiny-limit', 0.00001);

INSERT INTO `billing_projects` (`name`, `limit`)
VALUES ('test-agg-costs', NULL);

INSERT INTO `billing_project_users` (`billing_project`, `user`)
VALUES ('test-zero-limit', 'test');

INSERT INTO `billing_project_users` (`billing_project`, `user`)
VALUES ('test-tiny-limit', 'test');

INSERT INTO `billing_project_users` (`billing_project`, `user`)
VALUES ('test-agg-costs', 'test');
